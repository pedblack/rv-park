import asyncio
import random
import re
import os
import json
import argparse
import pandas as pd
from datetime import datetime, timedelta
from google import genai
from google.genai import types
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

# --- CONFIGURABLE CONSTANTS ---
MAX_REVIEWS = 100            # Standard review count for AI analysis
MODEL_NAME = "gemini-2.5-flash-lite" 
PROD_CSV = "backbone_locations.csv"
DEV_CSV = "backbone_locations_dev.csv"
LOG_FILE = "pipeline_execution.log"
AI_DELAY = 0.5               # Tier 1 Quota speed (300 RPM)

# --- SYSTEM SETTINGS ---
GEMINI_API_KEY = os.environ.get("GOOGLE_API_KEY")
P4N_USER = os.environ.get("P4N_USERNAME") 
P4N_PASS = os.environ.get("P4N_PASSWORD") 

TARGET_URLS = [
    "https://park4night.com/en/search?lat=37.6365&lng=-8.6385&z=10",
    "https://park4night.com/en/search?lat=37.8785&lng=-8.5686&z=10"
]

class PipelineLogger:
    @staticmethod
    def log_event(event_type, data):
        """Saves timestamped events to the log file with pretty-printing and datetime safety."""
        # json.dumps(default=str) ensures datetime objects don't crash the log
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "type": event_type,
            "content": data
        }
        
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            # indent=4 provides the requested pretty-printing
            f.write(json.dumps(log_entry, indent=4, default=str) + "\n" + "-"*50 + "\n")

    @staticmethod
    async def save_screenshot(page, name):
        """Captures a screenshot for auditing login or extraction failures."""
        path = f"debug_{name}_{datetime.now().strftime('%H%M%S')}.png"
        await page.screenshot(path=path)
        print(f"📸 DEBUG: Screenshot saved: {path}")

if not GEMINI_API_KEY:
    print("❌ ERROR: GOOGLE_API_KEY is missing.")
    exit(1)

client = genai.Client(api_key=GEMINI_API_KEY)

class P4NScraper:
    def __init__(self, is_dev=False):
        self.is_dev = is_dev
        self.csv_file = DEV_CSV if is_dev else PROD_CSV
        self.current_max_reviews = 1 if is_dev else MAX_REVIEWS 
        self.discovery_links = []
        self.processed_batch = []
        self.existing_df = self._load_existing()

    def _load_existing(self):
        if os.path.exists(self.csv_file):
            try:
                df = pd.read_csv(self.csv_file)
                df['last_scraped'] = pd.to_datetime(df['last_scraped'])
                return df
            except Exception: pass
        return pd.DataFrame()

    async def login(self, page):
        """Human-stealth login sequence using targeted button clicks."""
        if not P4N_USER or not P4N_PASS: return
        print(f"🔐 Attempting Login for {P4N_USER}...")
        try:
            # 1. Open the Account Dropdown
            account_btn = page.locator(".pageHeader-account-button")
            await account_btn.hover()
            await account_btn.click()
            await asyncio.sleep(2)
            
            # 2. Open the Modal via the Login link in dropdown
            login_trigger = page.locator(".pageHeader-account-dropdown >> text='Login'")
            await login_trigger.hover()
            await login_trigger.click(force=True)
            await page.wait_for_selector("#signinUserId", state="visible")

            # 3. Simulate human typing for credentials
            await page.type("#signinUserId", P4N_USER, delay=random.randint(70, 200))
            await asyncio.sleep(random.uniform(0.5, 1.2))
            await page.type("#signinPassword", P4N_PASS, delay=random.randint(70, 200))
            
            # 4. Target the specific Submit button in the modal footer
            submit_btn = page.locator(".modal-footer button[type='submit']:has-text('Login')")
            await submit_btn.hover()
            await asyncio.sleep(0.8)
            await submit_btn.click(force=True)
            
            # 5. Wait for Modal state change and stability
            try:
                await page.wait_for_selector("#signinModal", state="hidden", timeout=12000)
            except Exception: 
                await page.keyboard.press("Enter")
            
            await page.wait_for_load_state("networkidle")
            await asyncio.sleep(6) 

            # 6. Verification
            user_span = page.locator(".pageHeader-account-button span")
            actual_username = await user_span.inner_text()
            
            if P4N_USER.lower() in actual_username.lower():
                print(f"✅ Logged in as: {actual_username}")
                PipelineLogger.log_event("LOGIN_SUCCESS", {"user": actual_username})
            else:
                print(f"⚠️ Verification Failed. Found: '{actual_username}'")
                await PipelineLogger.save_screenshot(page, "login_verify_failed")
                PipelineLogger.log_event("LOGIN_FAILURE_AUDIT", {"header_text": actual_username})
        except Exception as e:
            print(f"❌ Login UI Error: {e}")
            await PipelineLogger.save_screenshot(page, "login_error")
            PipelineLogger.log_event("LOGIN_ERROR", {"error": str(e)})

    async def analyze_with_ai(self, raw_data):
        """Atomic AI call with safety for datetime serialization."""
        # Convert all content to JSON string for the prompt
        json_payload = json.dumps(raw_data, default=str)
        prompt = f"Analyze property data. Return JSON only:\n{json_payload}"
        
        PipelineLogger.log_event("AI_REQUEST_PROMPT", {
            "p4n_id": raw_data.get("p4n_id"),
            "prompt": prompt
        })

        config = types.GenerateContentConfig(response_mime_type="application/json", temperature=0.1)
        
        try:
            await asyncio.sleep(AI_DELAY) 
            response = await client.aio.models.generate_content(model=MODEL_NAME, contents=prompt, config=config)
            PipelineLogger.log_event("AI_RESPONSE_RAW", {"res": response.text})
            return json.loads(response.text)
        except Exception as e:
            PipelineLogger.log_event("AI_ERROR", {"err": str(e)})
            return {}

    async def extract_atomic(self, page, url):
        print(f"📄 Scraping: {url}")
        try:
            await page.goto(url, wait_until="domcontentloaded")
            p_id = await page.locator("body").get_attribute("data-place-id") or url.split("/")[-1]
            title = (await page.locator("h1").first.inner_text()).split('\n')[0].strip()
            
            # --- COORDINATE EXTRACTION ---
            lat, lng = 0.0, 0.0
            try:
                # Target the map search link containing latitude/longitude parameters
                coord_link = await page.locator("a[href*='lat='][href*='lng=']").first.get_attribute("href")
                if coord_link:
                    lat_match = re.search(r'lat=([-+]?\d*\.\d+|\d+)', coord_link)
                    lng_match = re.search(r'lng=([-+]?\d*\.\d+|\d+)', coord_link)
                    if lat_match and lng_match:
                        lat = float(lat_match.group(1))
                        lng = float(lng_match.group(1))
            except Exception as e:
                print(f"⚠️ Coord extraction failed for {p_id}: {e}")

            review_els = await page.locator(".place-feedback-article-content").all()
            raw_payload = {
                "p4n_id": p_id,
                "reviews": [await r.inner_text() for r in review_els[:self.current_max_reviews]]
            }
            ai_data = await self.analyze_with_ai(raw_payload)

            row = {
                "p4n_id": p_id, 
                "title": title, 
                "url": url,
                "latitude": lat,
                "longitude": lng,
                "parking_min_eur": ai_data.get("parking_min", 0),
                "ai_pros": ai_data.get("pros", "N/A"),
                "last_scraped": datetime.now()
            }
            
            PipelineLogger.log_event("STORAGE_ROW_PREPARED", row)
            self.processed_batch.append(row)
        except Exception as e: 
            print(f"⚠️ Error {url}: {e}")

    async def start(self):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            # High-reputation context to bypass detection
            context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            page = await context.new_page()
            await Stealth().apply_stealth_async(page)
            
            await page.goto("https://park4night.com/en", wait_until="networkidle")
            try: await page.click(".cc-btn-accept", timeout=3000)
            except Exception: pass
            await self.login(page)

            for url in TARGET_URLS:
                try:
                    await page.goto(url, wait_until="networkidle")
                    links = await page.locator("a[href*='/place/']").all()
                    for link in links:
                        href = await link.get_attribute("href")
                        if href:
                            self.discovery_links.append(f"https://park4night.com{href}" if href.startswith("/") else href)
                        if self.is_dev and len(self.discovery_links) >= 1: break
                except Exception: pass
                if self.is_dev and len(self.discovery_links) >= 1: break

            queue = []
            for link in list(set(self.discovery_links)):
                match = re.search(r'/place/(\d+)', link)
                if not match: continue
                p_id = match.group(1)
                
                if self.is_dev:
                    queue.append(link)
                    break
                
                is_stale = True
                if not self.existing_df.empty and p_id in self.existing_df['p4n_id'].astype(str).values:
                    last_date = self.existing_df[self.existing_df['p4n_id'].astype(str) == p_id]['last_scraped'].iloc[0]
                    if (datetime.now() - last_date) < timedelta(days=7): is_stale = False
                if is_stale: queue.append(link)

            print(f"⚡ Processing {len(queue)} items...")
            for link in queue:
                await self.extract_atomic(page, link)
            
            await browser.close()
            self._upsert_and_save()

    def _upsert_and_save(self):
        if not self.processed_batch: return
        new_df = pd.DataFrame(self.processed_batch)
        final_df = pd.concat([new_df, self.existing_df], ignore_index=True)
        final_df['last_scraped'] = pd.to_datetime(final_df['last_scraped'])
        final_df.sort_values('last_scraped', ascending=False).drop_duplicates('p4n_id').to_csv(self.csv_file, index=False)
        print(f"🚀 Success! Updated {self.csv_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--dev', action='store_true')
    args = parser.parse_args()
    asyncio.run(P4NScraper(is_dev=args.dev).start())
