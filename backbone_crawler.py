import asyncio
import random
import re
import os
import json
import pandas as pd
from datetime import datetime, timedelta
from google import genai
from google.genai import types
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

# --- CONFIGURABLE CONSTANTS ---
MAX_REVIEWS = 100            # Number of reviews to send to AI for analysis
MODEL_NAME = "gemini-2.5-flash-lite" # Latest 2026 stable budget flagship
CSV_FILE = "backbone_locations.csv"

# --- SYSTEM SETTINGS ---
# Pulls from GitHub Secrets or Local Env (Never hardcode this!)
GEMINI_API_KEY = os.environ.get("GOOGLE_API_KEY")

TARGET_URLS = [
    "https://park4night.com/en/search?lat=37.63658110718217&lng=-8.638597348689018&z=10",
    "https://park4night.com/en/search?lat=37.87856774592691&lng=-8.568677272965147&z=10"
]

if not GEMINI_API_KEY:
    print("❌ ERROR: GOOGLE_API_KEY not found in environment variables.")
    exit(1)

# Initialize the 2026 Unified Client
client = genai.Client(api_key=GEMINI_API_KEY)

class P4NScraper:
    def __init__(self):
        self.discovery_links = []
        self.processed_batch = []
        self.existing_df = self._load_existing()

    def _load_existing(self):
        """Loads CSV and ensures dates are proper Timestamp objects."""
        if os.path.exists(CSV_FILE):
            try:
                df = pd.read_csv(CSV_FILE)
                # Convert string dates to Timestamps to avoid sort errors
                df['last_scraped'] = pd.to_datetime(df['last_scraped'])
                return df
            except Exception as e:
                print(f"⚠️ Load Warning: Could not parse existing CSV: {e}")
        return pd.DataFrame()

    async def analyze_with_ai(self, raw_data):
        """Processes reviews with a 4s safety buffer to honor 15 RPM Free Tier."""
        prompt = f"Analyze property data and up to {MAX_REVIEWS} reviews. Return JSON only:\n{json.dumps(raw_data)}"
        
        config = types.GenerateContentConfig(
            response_mime_type="application/json",
            temperature=0.1,
            system_instruction=(
                f"You are a travel data assistant. Analyze {MAX_REVIEWS} reviews. "
                "1. Summarize into English pros/cons. "
                "2. Normalize costs to numeric EUR (parking_min, parking_max, service_price_clean). "
                "3. Detect language distribution (ISO: count)."
            )
        )

        try:
            # Mandatoy 4s sleep for 15 RPM Free Tier limit
            await asyncio.sleep(4) 
            response = await client.aio.models.generate_content(
                model=MODEL_NAME,
                contents=prompt,
                config=config
            )
            return json.loads(response.text)
        except Exception as e:
            print(f"🤖 AI Failure: {e}")
            return {}

    async def extract_atomic(self, page, url):
        """Scrapes one page and triggers AI analysis."""
        print(f"📄 Scraping: {url}")
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await asyncio.sleep(random.uniform(2, 4))

            # ID and Title
            p_id = await page.locator("body").get_attribute("data-place-id") or url.split("/")[-1]
            title = (await page.locator("h1").first.inner_text()).split('\n')[0].strip()
            
            # Review Scraping (Limited by MAX_REVIEWS constant)
            review_els = await page.locator(".place-feedback-article-content").all()
            reviews = [await r.inner_text() for r in review_els[:MAX_REVIEWS]]

            raw_payload = {
                "parking_cost": await self._get_dl(page, "Parking cost"),
                "service_price": await self._get_dl(page, "Price of services"),
                "reviews": reviews
            }

            ai_data = await self.analyze_with_ai(raw_payload)

            # Store as a dict (Dates stored as objects, NOT strings, for Pandas sorting)
            self.processed_batch.append({
                "p4n_id": p_id,
                "title": title,
                "parking_min_eur": ai_data.get("parking_min", 0),
                "parking_max_eur": ai_data.get("parking_max", 0),
                "service_price_eur": ai_data.get("service_price_clean", 0),
                "ai_pros": ai_data.get("pros", "N/A"),
                "ai_cons": ai_data.get("cons", "N/A"),
                "lang_dist": json.dumps(ai_data.get("lang_dist", {})),
                "url": url,
                "last_scraped": datetime.now() # Store as object for comparison
            })
        except Exception as e:
            print(f"⚠️ Extraction Error: {url} -> {e}")

    async def _get_dl(self, page, label):
        """Helper to find data in definition lists."""
        try:
            return (await page.locator(f"dt:has-text('{label}') + dd").first.inner_text()).strip()
        except:
            return "N/A"

    async def start(self):
        """Main orchestrator."""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            page = await context.new_page()
            await Stealth().apply_stealth_async(page)

            # 1. Discovery Phase
            print("🔍 Starting Discovery...")
            for url in TARGET_URLS:
                try:
                    await page.goto(url, wait_until="networkidle")
                    # Accept cookies if present
                    try: await page.click(".cc-btn-accept", timeout=3000)
                    except: pass
                    
                    links = await page.locator("a[href*='/place/']").all()
                    for link in links:
                        href = await link.get_attribute("href")
                        if href:
                            full_url = f"https://park4night.com{href}" if href.startswith("/") else href
                            self.discovery_links.append(full_url)
                except Exception as e:
                    print(f"⚠️ Discovery Skip: {e}")
            
            # 2. Filtering Phase (1-week rescrape rule)
            queue = []
            unique_links = list(set(self.discovery_links))
            for link in unique_links:
                match = re.search(r'/place/(\d+)', link)
                if not match: continue
                p_id = match.group(1)
                
                is_stale = True
                if not self.existing_df.empty and p_id in self.existing_df['p4n_id'].astype(str).values:
                    # Get the specific last_scraped date for this ID
                    last_date = self.existing_df[self.existing_df['p4n_id'].astype(str) == p_id]['last_scraped'].iloc[0]
                    if (datetime.now() - last_date) < timedelta(days=7):
                        is_stale = False
                
                if is_stale:
                    queue.append(link)

            print(f"⚡ Processing Queue: {len(queue)} items (Stale or New)")
            for link in queue:
                await self.extract_atomic(page, link)
            
            await browser.close()
            self._upsert_and_save()

    def _upsert_and_save(self):
        """Combines new data with old, handles types, and saves to CSV."""
        if not self.processed_batch:
            print("ℹ️ No new items processed.")
            return

        new_df = pd.DataFrame(self.processed_batch)
        
        # Combine dataframes
        final_df = pd.concat([new_df, self.existing_df], ignore_index=True)
        
        # CRITICAL: Fix for TypeError. Ensure all objects are Timestamps before sorting.
        final_df['last_scraped'] = pd.to_datetime(final_df['last_scraped'])
        
        # Sort by date (newest first) and remove duplicates of the same property ID
        final_df = final_df.sort_values('last_scraped', ascending=False).drop_duplicates('p4n_id')
        
        # Save back to CSV
        final_df.to_csv(CSV_FILE, index=False)
        print(f"🚀 Success! {len(final_df)} total records now in {CSV_FILE}.")

if __name__ == "__main__":
    asyncio.run(P4NScraper().start())
