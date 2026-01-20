import asyncio
import random
import re
import os
import json
import pandas as pd
import google.generativeai as genai
from datetime import datetime, timedelta
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

# --- SETTINGS ---
CSV_FILE = "backbone_locations.csv"
GEMINI_API_KEY = os.environ.get("GOOGLE_API_KEY", "AIzaSyD_A_bYXFkkOLzpXgPuvje39x4w7YPOfzs")
MODEL_NAME = "gemini-3-flash-preview" # Latest Jan 2026 Model

TARGET_URLS = [
    "https://park4night.com/en/search?lat=37.63658110718217&lng=-8.638597348689018&z=10",
    "https://park4night.com/en/search?lat=37.87856774592691&lng=-8.568677272965147&z=10"
]

# Initialize Gemini 3
genai.configure(api_key=GEMINI_API_KEY)
ai_model = genai.GenerativeModel(
    model_name=MODEL_NAME,
    generation_config={"response_mime_type": "application/json", "temperature": 0.1}
)

class P4NScraper:
    def __init__(self):
        self.discovery_links = []
        self.processed_batch = []
        self.existing_df = self._load_existing()

    def _load_existing(self):
        if os.path.exists(CSV_FILE):
            try:
                df = pd.read_csv(CSV_FILE)
                if 'last_scraped' in df.columns:
                    df['last_scraped'] = pd.to_datetime(df['last_scraped'])
                return df
            except: pass
        return pd.DataFrame()

    async def init_browser(self, p):
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={'width': 1280, 'height': 800}
        )
        page = await context.new_page()
        await Stealth().apply_stealth_async(page)
        return browser, page

    async def run_discovery(self, page):
        """Discovers all location links from search pages."""
        for url in TARGET_URLS:
            print(f"🔍 Discovery: {url}")
            try:
                await page.goto(url, wait_until="networkidle", timeout=60000)
                try: # Cookie Accept
                    await page.click(".cc-btn-accept", timeout=3000)
                except: pass
                await asyncio.sleep(4)
                links = await page.locator("#searchmap-list-results li a").all()
                for link in links:
                    href = await link.get_attribute("href")
                    if href and "/place/" in href:
                        self.discovery_links.append(f"https://park4night.com{href}")
            except Exception as e: print(f"⚠️ Discovery Error: {e}")

    async def analyze_with_ai(self, raw_data):
        """Combined AI pass: Normalizes prices, summarizes reviews, and detects languages."""
        prompt = f"""
        Analyze the following property raw data. 
        1. Normalize 'parking_cost' and 'service_price' to numeric values in EUR.
           - If a range is given (e.g. 13-28), provide the 'parking_cost_min' and 'parking_cost_max'.
           - If it's free, set to 0.
        2. Synthesize 'pros' and 'cons' from the reviews in English.
        3. Provide a 'lang_dist' dictionary (ISO code: count).

        RAW DATA:
        {json.dumps(raw_data)}
        
        Output JSON format strictly:
        {{
          "parking_min": float,
          "parking_max": float,
          "service_price_clean": float,
          "pros": "bullet list",
          "cons": "bullet list",
          "lang_dist": {{}}
        }}
        """
        try:
            await asyncio.sleep(4) # Throttling for Free Tier API
            response = await ai_model.generate_content_async(prompt)
            return json.loads(response.text)
        except Exception as e:
            print(f"🤖 AI Failure: {e}")
            return {}

    async def extract_atomic(self, page, url):
        """Visits location, scrapes raw data, and triggers AI analysis."""
        print(f"📄 Atomic Scrape: {url}")
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await asyncio.sleep(random.uniform(2, 4))

            # Raw Extraction
            p4n_id = await page.locator("body").get_attribute("data-place-id")
            title = (await page.locator("h1.place-header-name").inner_text()).split('\n')[0].strip()
            
            review_els = await page.locator(".place-feedback-article-content").all()
            reviews = [await r.inner_text() for r in review_els]

            async def get_dl(label):
                try: return (await page.locator(f"dl.place-info-details dt:has-text('{label}') + dd").inner_text()).strip()
                except: return "N/A"

            raw_payload = {
                "parking_cost": await get_dl("Parking cost"),
                "service_price": await get_dl("Price of services"),
                "reviews": reviews[:10] # Only send top 10 reviews to save tokens
            }

            # AI Normalization & Enrichment
            ai_data = await self.analyze_with_ai(raw_payload)

            self.processed_batch.append({
                "p4n_id": p4n_id,
                "title": title,
                "type": await page.locator(".place-header-access img").get_attribute("title"),
                "rating": (await page.locator(".rating-note").first.inner_text()).split('/')[0].strip() if await page.locator(".rating-note").count() > 0 else "0",
                "parking_min_eur": ai_data.get("parking_min", 0),
                "parking_max_eur": ai_data.get("parking_max", 0),
                "service_price_eur": ai_data.get("service_price_clean", 0),
                "num_places": await get_dl("Number of places"),
                "ai_pros": ai_data.get("pros", "N/A"),
                "ai_cons": ai_data.get("cons", "N/A"),
                "lang_dist": json.dumps(ai_data.get("lang_dist", {})),
                "url": url,
                "last_scraped": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
        except Exception as e: print(f"⚠️ Extraction Error: {url} -> {e}")

    async def start(self):
        async with async_playwright() as p:
            browser, page = await self.init_browser(p)
            await self.run_discovery(page)
            
            # Filter: 1-week Rescrape rule
            queue = []
            for link in list(set(self.discovery_links)):
                p_id = re.search(r'/place/(\d+)', link).group(1)
                if self.existing_df.empty or p_id not in self.existing_df['p4n_id'].astype(str).values:
                    queue.append(link)
                else:
                    row = self.existing_df[self.existing_df['p4n_id'].astype(str) == p_id]
                    if (datetime.now() - pd.to_datetime(row['last_scraped'].iloc[0])) > timedelta(days=7):
                        queue.append(link)

            print(f"⚡ Queue Size: {len(queue)} (New/Stale)")
            for link in queue:
                await self.extract_atomic(page, link)
            
            await browser.close()
            self._upsert_and_save()

    def _upsert_and_save(self):
        if not self.processed_batch: return
        new_df = pd.DataFrame(self.processed_batch)
        final_df = pd.concat([new_df, self.existing_df], ignore_index=True)
        final_df.sort_values('last_scraped', ascending=False).drop_duplicates('p4n_id').to_csv(CSV_FILE, index=False)
        print(f"🚀 Success! {len(final_df)} records in database.")

if __name__ == "__main__":
    asyncio.run(P4NScraper().start())
