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
MAX_REVIEWS = 100            
MODEL_NAME = "gemini-2.5-flash-lite" 
PROD_CSV = "backbone_locations.csv"
DEV_CSV = "backbone_locations_dev.csv"
LOG_FILE = "pipeline_execution.log"
AI_DELAY = 0.5               
STALENESS_DAYS = 30          # properties updated within 30 days are skipped

# --- PARTITION SETTINGS ---
URL_LIST_FILE = "url_list.txt"   
STATE_FILE = "queue_state.json"  

# --- SYSTEM SETTINGS ---
GEMINI_API_KEY = os.environ.get("GOOGLE_API_KEY")
P4N_USER = os.environ.get("P4N_USERNAME") 
P4N_PASS = os.environ.get("P4N_PASSWORD") 

class DailyQueueManager:
    @staticmethod
    def get_next_partition():
        if not os.path.exists(URL_LIST_FILE):
            print(f"❌ ERROR: {URL_LIST_FILE} not found.")
            return [], 0, 0
        with open(URL_LIST_FILE, 'r') as f:
            urls = [line.strip() for line in f if line.strip()]
        state = {"current_index": 0}
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, 'r') as f: state = json.load(f)
            except: pass
        idx = state.get("current_index", 0)
        if idx >= len(urls): idx = 0
        target_url = urls[idx]
        state["current_index"] = idx + 1
        with open(STATE_FILE, 'w') as f: json.dump(state, f)
        return [target_url], idx + 1, len(urls)

class PipelineLogger:
    @staticmethod
    def log_event(event_type, data):
        """Saves formatted JSON events with Unicode support."""
        processed_content = {}
        for k, v in data.items():
            if isinstance(v, str) and (v.strip().startswith('{') or v.strip().startswith('[')):
                try: processed_content[k] = json.loads(v)
                except: processed_content[k] = v
            else: processed_content[k] = v
        log_entry = {"timestamp": datetime.now().isoformat(), "type": event_type, "content": processed_content}
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            header = f"\n{'='*30} {event_type} {'='*30}\n"
            f.write(header + json.dumps(log_entry, indent=4, default=str, ensure_ascii=False) + "\n")

client = genai.Client(api_key=GEMINI_API_KEY)

class P4NScraper:
    def __init__(self, is_dev=False):
        self.is_dev = is_dev
        self.csv_file = DEV_CSV if is_dev else PROD_CSV
        self.current_max_reviews = 5 if is_dev else MAX_REVIEWS 
        self.discovery_links = []
        self.processed_batch = []
        self.existing_df = self._load_existing()

    def _load_existing(self):
        if os.path.exists(self.csv_file):
            try:
                df = pd.read_csv(self.csv_file)
                df['last_scraped'] = pd.to_datetime(df['last_scraped'], errors='coerce')
                return df
            except: pass
        return pd.DataFrame()

    async def login(self, page):
        if not P4N_USER or not P4N_PASS: return
        print(f"🔐 [LOGIN] Attempting for user: {P4N_USER}...")
        try:
            await page.click(".pageHeader-account-button")
            await asyncio.sleep(2)
            await page.click(".pageHeader-account-dropdown >> text='Login'", force=True)
            await page.wait_for_selector("#signinUserId", state="visible")
            await page.locator("#signinUserId").type(P4N_USER, delay=random.randint(150, 250))
            await page.locator("#signinPassword").type(P4N_PASS, delay=random.randint(150, 300))
            await page.click(".modal-footer button[type='submit']:has-text('Login')", force=True)
            await page.wait_for_load_state("networkidle")
            await asyncio.sleep(6)
            print("✅ [LOGIN] Success")
        except: 
            print("❌ [LOGIN] Failed")

    async def analyze_with_ai(self, raw_data):
        system_instruction = (
            "Analyze property data and return JSON ONLY. "
            "Schema: { 'parking_min': float, 'parking_max': float, 'electricity_eur': float, 'pros': 'string', 'cons': 'string' }. "
            "Summary strings must be succinct (3-5 words max)."
        )
        json_payload = json.dumps(raw_data, default=str, ensure_ascii=False)
        config = types.GenerateContentConfig(response_mime_type="application/json", temperature=0.1, system_instruction=system_instruction)
        try:
            await asyncio.sleep(AI_DELAY) 
            response = await client.aio.models.generate_content(model=MODEL_NAME, contents=f"ANALYZE:\n{json_payload}", config=config)
            return json.loads(response.text)
        except: return {}

    async def extract_atomic(self, page, url, current_num, total_num):
        # Console feedback for the current property being processed
        print(f"➡️  [{current_num}/{total_num}] Processing: {url}")
        try:
            await page.goto(url, wait_until="domcontentloaded")
            p_id = await page.locator("body").get_attribute("data-place-id") or url.split("/")[-1]
            title = (await page.locator("h1").first.inner_text()).split('\n')[0].strip()
            
            lat, lng = 0.0, 0.0
            coord_link = await page.locator("a[href*='lat='][href*='lng=']").first.get_attribute("href")
            if coord_link:
                m = re.search(r'lat=([-+]?\d*\.\d+|\d+)&lng=([-+]?\d*\.\d+|\d+)', coord_link)
                if m: lat, lng = float(m.group(1)), float(m.group(2))

            total_reviews, avg_rating = 0, 0.0
            try:
                raw_count = await page.locator(".place-feedback-average strong").inner_text()
                raw_rate = await page.locator(".place-feedback-average .text-gray").inner_text()
                total_reviews = int(re.search(r'(\d+)', raw_count).group(1))
                avg_rating = float(re.search(r'(\d+\.?\d*)', raw_rate).group(1))
            except: pass

            for _ in range(5):
                reviews = await page.locator(".place-feedback-article-content").all()
                if len(reviews) >= self.current_max_reviews: break
                more_btn = page.locator(".place-feedback-pagination button:has-text('More')")
                if await more_btn.is_visible():
                    await more_btn.click()
                    await asyncio.sleep(2)
                else: break

            raw_payload = {
                "p4n_id": p_id,
                "parking_cost": await self._get_dl(page, "Parking cost"),
                "services_cost": await self._get_dl(page, "Price of services"),
                "reviews": [await r.text_content() for r in (await page.locator(".place-feedback-article-content").all())[:self.current_max_reviews]]
            }
            
            ai_data = await self.analyze_with_ai(raw_payload)
            row = {
                "p4n_id": p_id, "title": title, "url": url,
                "latitude": lat, "longitude": lng,
                "total_reviews": total_reviews, "avg_rating": avg_rating,
                "parking_min_eur": ai_data.get("parking_min", 0),
                "parking_max_eur": ai_data.get("parking_max", 0),
                "electricity_eur": ai_data.get("electricity_eur", 0),
                "ai_pros": ai_data.get("pros", "N/A"),
                "ai_cons": ai_data.get("cons", "N/A"),
                "last_scraped": datetime.now()
            }
            PipelineLogger.log_event("STORAGE_ROW", row)
            self.processed_batch.append(row)
        except Exception as e: print(f"  ⚠️ Error scraping {url}: {e}")

    async def _get_dl(self, page, label):
        try: return (await page.locator(f"dt:has-text('{label}') + dd").first.inner_text()).strip()
        except: return "N/A"

    async def start(self):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(user_agent="Mozilla/5.0...")
            page = await context.new_page()
            await Stealth().apply_stealth_async(page)
            await page.goto("https://park4night.com/en", wait_until="networkidle")
            try: await page.click(".cc-btn-accept", timeout=3000)
            except: pass
            await self.login(page)

            target_urls, current_idx, total_idx = DailyQueueManager.get_next_partition()
            print(f"\n📅 [PARTITION] Day {current_idx} of {total_idx}")
            print(f"🔗 [URL] Searching: {target_urls[0]}\n")
            
            PipelineLogger.log_event("DAILY_CYCLE_START", {"partition": f"{current_idx}/{total_idx}", "url": target_urls[0]})

            for url in target_urls:
                await page.goto(url, wait_until="networkidle")
                links = await page.locator("a[href*='/place/']").all()
                for link in links:
                    href = await link.get_attribute("href")
                    if href:
                        self.discovery_links.append(f"https://park4night.com{href}" if href.startswith("/") else href)

            discovered = list(set(self.discovery_links))
            print(f"🔍 [DISCOVERY] Found {len(discovered)} total properties in this region.")

            # --- STALENESS FILTERING ---
            queue = []
            skipped_count = 0
            for link in discovered:
                m = re.search(r'/place/(\d+)', link)
                if not m: continue
                p_id = str(m.group(1))

                is_stale = True
                if not self.existing_df.empty and p_id in self.existing_df['p4n_id'].astype(str).values:
                    last_date = self.existing_df[self.existing_df['p4n_id'].astype(str) == p_id]['last_scraped'].iloc[0]
                    if pd.notnull(last_date) and (datetime.now() - last_date) < timedelta(days=STALENESS_DAYS):
                        is_stale = False
                
                if is_stale or self.is_dev:
                    queue.append(link)
                else:
                    skipped_count += 1

            print(f"⏭️  [TTL] Skipped {skipped_count} properties (already updated within {STALENESS_DAYS} days).")
            print(f"⚡ [QUEUE] Processing {len(queue)} stale properties...\n")

            for i, link in enumerate(queue, 1):
                await self.extract_atomic(page, link, i, len(queue))
            
            await browser.close()
            self._upsert_and_save()

    def _upsert_and_save(self):
        if not self.processed_batch: 
            print("\n🙌 [FINISH] No new data to save.")
            return
        new_df = pd.DataFrame(self.processed_batch)
        final_df = pd.concat([new_df, self.existing_df], ignore_index=True)
        final_df['last_scraped'] = pd.to_datetime(final_df['last_scraped'])
        final_df.sort_values('last_scraped', ascending=False).drop_duplicates('p4n_id').to_csv(self.csv_file, index=False)
        print(f"\n🚀 [FINISH] Database updated: {len(self.processed_batch)} new/refreshed records.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--dev', action='store_true')
    args = parser.parse_args()
    asyncio.run(P4NScraper(is_dev=args.dev).start())
