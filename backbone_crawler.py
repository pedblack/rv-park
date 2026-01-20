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
MODEL_NAME = "gemini-2.5-flash-lite" 
PROD_CSV = "backbone_locations.csv"
DEV_CSV = "backbone_locations_dev.csv"
LOG_FILE = "pipeline_execution.log"

# --- ADAPTED SETTINGS ---
AI_DELAY = 1.5               # Balanced for 300 RPM quota
STALENESS_DAYS = 30          # Skip if updated within 30 days
MIN_REVIEWS_THRESHOLD = 3    # Skip if property has < 3 reviews
DEV_LIMIT = 1                # Strict limit for --dev runs

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
        return [target_url], idx + 1, len(urls)

    @staticmethod
    def increment_state():
        if not os.path.exists(URL_LIST_FILE): return
        with open(URL_LIST_FILE, 'r') as f:
            urls = [line.strip() for line in f if line.strip()]
        state = {"current_index": 0}
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, 'r') as f: state = json.load(f)
            except: pass
        state["current_index"] = (state.get("current_index", 0) + 1) % len(urls)
        with open(STATE_FILE, 'w') as f: json.dump(state, f)

class PipelineLogger:
    @staticmethod
    def log_event(event_type, data):
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
    def __init__(self, is_dev=False, force=False):
        self.is_dev = is_dev
        self.force = force 
        self.csv_file = DEV_CSV if is_dev else PROD_CSV
        self.processed_batch = []
        self.existing_df = self._load_existing()
        self.stats = {
            "read": 0, 
            "discarded_fresh": 0, 
            "discarded_low_feedback": 0, 
            "gemini_calls": 0
        }

    def _load_existing(self):
        if os.path.exists(self.csv_file):
            try:
                df = pd.read_csv(self.csv_file)
                df['last_scraped'] = pd.to_datetime(df['last_scraped'], errors='coerce')
                return df
            except: pass
        return pd.DataFrame()

    async def login(self, page):
        if not P4N_USER or not P4N_PASS:
            print("⚠️ [LOGIN] Missing credentials.")
            return False

        print(f"🔐 [LOGIN] Attempting for user: {P4N_USER}...")
        try:
            await page.click(".pageHeader-account-button")
            await asyncio.sleep(1)
            await page.click(".pageHeader-account-dropdown >> text='Login'", force=True)
            await page.wait_for_selector("#signinUserId", state="visible", timeout=10000)
            
            # fill() is used for immediate and reliable typing
            await page.locator("#signinUserId").fill(P4N_USER)
            await page.locator("#signinPassword").fill(P4N_PASS)
            
            # Submit using the primary button selector from DOM
            submit_selector = "#signinModal .modal-footer button[type='submit']:has-text('Login')"
            await page.click(submit_selector, force=True)
            
            print("⏳ [LOGIN] Submitting... Waiting 10s for session update.")
            await page.wait_for_load_state("networkidle")
            await asyncio.sleep(10) # Extended wait for session persistence
            
            ts = int(datetime.now().timestamp())
            try:
                # Verifying if the username 'pblack' is present in the nav button
                account_span = page.locator(".pageHeader-account-button span")
                username_found = (await account_span.inner_text()).strip()
                if P4N_USER.lower() in username_found.lower():
                    print(f"✅ [LOGIN] Verified successfully: Logged in as '{username_found}'")
                    await page.screenshot(path=f"login_success_{ts}.png", full_page=True)
                    return True
                else:
                    print(f"❌ [LOGIN] Validation failed. Found header text: '{username_found}'")
            except Exception as e:
                print(f"❌ [LOGIN] Verification element not found: {e}")

            await page.screenshot(path=f"login_failure_{ts}.png")
            return False

        except Exception as e: 
            print(f"❌ [LOGIN] Error during process: {e}")
            await page.screenshot(path=f"login_exception_{int(datetime.now().timestamp())}.png")
            return False

    async def analyze_with_ai(self, raw_data):
        self.stats["gemini_calls"] += 1
        system_instruction = (
            "Analyze data and return JSON ONLY. Schema: { 'parking_min': float, 'parking_max': float, "
            "'electricity_eur': float, 'num_places': int, 'pros': [ {'topic': 'string', 'count': int} ], "
            "'cons': [ {'topic': 'string', 'count': int} ], 'languages': [ {'lang': 'string', 'count': int} ] }. "
            "1. Extract 'num_places' from 'places_count' field. 2. List items by recurrence frequency. "
            "3. Topics 3-5 words max."
        )
        json_payload = json.dumps(raw_data, default=str, ensure_ascii=False)
        config = types.GenerateContentConfig(response_mime_type="application/json", temperature=0.1, system_instruction=system_instruction)
        try:
            await asyncio.sleep(AI_DELAY) 
            response = await client.aio.models.generate_content(model=MODEL_NAME, contents=f"ANALYZE:\n{json_payload}", config=config)
            return json.loads(response.text)
        except: return {}

    async def extract_atomic(self, page, url, current_num, total_num):
        print(f"➡️  [{current_num}/{total_num}] Scraped Item: {url}")
        self.stats["read"] += 1
        try:
            await page.goto(url, wait_until="domcontentloaded")
            
            # --- MIN REVIEWS CHECK ---
            stats_container = page.locator(".place-feedback-average")
            raw_count_text = await stats_container.locator("strong").inner_text()
            count_match = re.search(r'(\d+)', raw_count_text)
            actual_feedback_count = int(count_match.group(1)) if count_match else 0
            
            if actual_feedback_count < MIN_REVIEWS_THRESHOLD:
                print(f"🗑️  [DISCARD] Insufficient feedback ({actual_feedback_count} reviews). Required: {MIN_REVIEWS_THRESHOLD}")
                self.stats["discarded_low_feedback"] += 1
                return

            p_id = await page.locator("body").get_attribute("data-place-id") or url.split("/")[-1]
            title = (await page.locator("h1").first.inner_text()).split('\n')[0].strip()
            
            # location_type from icon title
            location_type = "Unknown"
            try:
                location_type = await page.locator(".place-header-access img").get_attribute("title")
            except: pass

            # Coordinates
            lat, lng = 0.0, 0.0
            coord_link = await page.locator("a[href*='lat='][href*='lng=']").first.get_attribute("href")
            if coord_link:
                m = re.search(r'lat=([-+]?\d*\.\d+|\d+)&lng=([-+]?\d*\.\d+|\d+)', coord_link)
                if m: lat, lng = float(m.group(1)), float(m.group(2))

            raw_rate = await stats_container.locator(".text-gray").inner_text()
            avg_rating = float(re.search(r'(\d+\.?\d*)', raw_rate).group(1)) if re.search(r'(\d+\.?\d*)', raw_rate) else 0.0

            review_els = await page.locator(".place-feedback-article-content").all()
            reviews_text = [await r.text_content() for r in review_els]

            raw_payload = {
                "p4n_id": p_id,
                "location_type": location_type,
                "places_count": await self._get_dl(page, "Number of places"),
                "parking_cost": await self._get_dl(page, "Parking cost"),
                "services_cost": await self._get_dl(page, "Price of services"),
                "all_reviews": reviews_text 
            }
            
            ai_data = await self.analyze_with_ai(raw_payload)
            row = {
                "p4n_id": p_id, "title": title, "url": url, "latitude": lat, "longitude": lng,
                "location_type": location_type,
                "num_places": ai_data.get("num_places", 0),
                "total_reviews": actual_feedback_count, "avg_rating": avg_rating,
                "parking_min_eur": ai_data.get("parking_min", 0),
                "parking_max_eur": ai_data.get("parking_max", 0),
                "electricity_eur": ai_data.get("electricity_eur", 0),
                "ai_pros": "; ".join([f"{p['topic']} ({p['count']})" for p in ai_data.get('pros', [])]),
                "ai_cons": "; ".join([f"{c['topic']} ({c['count']})" for c in ai_data.get('cons', [])]),
                "top_languages": "; ".join([f"{l['lang']} ({l['count']})" for l in ai_data.get('languages', [])]),
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
            context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
            page = await context.new_page()
            await Stealth().apply_stealth_async(page)
            await page.goto("https://park4night.com/en", wait_until="networkidle")
            try: await page.click(".cc-btn-accept", timeout=3000)
            except: pass
            
            # --- LOGIN VERIFICATION ---
            logged_in = await self.login(page)
            if not logged_in and not self.is_dev:
                print("🛑 [CRITICAL] Login failed in production mode. Aborting run.")
                await browser.close()
                return

            target_urls, current_idx, total_idx = DailyQueueManager.get_next_partition()
            print(f"\n📅 [PARTITION] Day {current_idx} of {total_idx}")
            if target_urls: print(f"🔗 [SEARCH LINK] Fetching from: {target_urls[0]}\n")
            
            discovery_links = []
            for url in target_urls:
                await page.goto(url, wait_until="networkidle")
                links = await page.locator("a[href*='/place/']").all()
                for link in links:
                    href = await link.get_attribute("href")
                    if href: discovery_links.append(f"https://park4night.com{href}" if href.startswith("/") else href)

            discovered = list(set(discovery_links))
            queue = []
            for link in discovered:
                if self.is_dev and len(queue) >= DEV_LIMIT: break
                p_id = link.split("/")[-1]
                
                is_stale = True
                if not self.force and not self.existing_df.empty and p_id in self.existing_df['p4n_id'].astype(str).values:
                    last_date = self.existing_df[self.existing_df['p4n_id'].astype(str) == p_id]['last_scraped'].iloc[0]
                    if pd.notnull(last_date) and (datetime.now() - last_date) < timedelta(days=STALENESS_DAYS):
                        is_stale = False
                
                if is_stale or self.force: queue.append(link)
                else: 
                    print(f"🗑️  [DISCARD] Skipping {p_id}: Updated within 30 days.")
                    self.stats["discarded_fresh"] += 1

            if self.force: print(f"⚡ [FORCE] Overriding staleness. Processing all {len(queue)} items.")
            print(f"🔍 Found {len(discovered)} total items. TTL skips: {self.stats['discarded_fresh']}. Processing: {len(queue)}\n")
            
            for i, link in enumerate(queue, 1):
                await self.extract_atomic(page, link, i, len(queue))
            
            await browser.close()
            self._upsert_and_save()
            
            # --- FINAL SUMMARY LOGS ---
            print(f"\n🏁 [RUN SUMMARY]")
            print(f"🔹 Total identified: {len(discovered)}")
            print(f"🔹 Scrape attempts (read): {self.stats['read']}")
            print(f"🔹 Discarded (Fresh < 30d): {self.stats['discarded_fresh']}")
            print(f"🔹 Discarded (Low Feedback): {self.stats['discarded_low_feedback']}")
            print(f"🤖 Gemini API Calls: {self.stats['gemini_calls']}")
            print(f"🚀 Database Refreshed: {len(self.processed_batch)} records.")

            if not self.is_dev: DailyQueueManager.increment_state()

    def _upsert_and_save(self):
        if not self.processed_batch: return
        new_df = pd.DataFrame(self.processed_batch)
        final_df = pd.concat([new_df, self.existing_df], ignore_index=True)
        final_df['last_scraped'] = pd.to_datetime(final_df['last_scraped'])
        final_df.sort_values('last_scraped', ascending=False).drop_duplicates('p4n_id').to_csv(self.csv_file, index=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--dev', action='store_true')
    parser.add_argument('--force', action='store_true', help='Force recrawl even if fresh')
    args = parser.parse_args()
    asyncio.run(P4NScraper(is_dev=args.dev, force=args.force).start())
