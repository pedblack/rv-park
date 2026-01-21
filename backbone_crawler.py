import asyncio
import random
import re
import os
import json
import argparse
import time
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
CONCURRENCY_LIMIT = 3  

AI_DELAY = 1.0
STALENESS_DAYS = 30
MIN_REVIEWS_THRESHOLD = 5
DEV_LIMIT = 1 

URL_LIST_FILE = "url_list.txt"   
STATE_FILE = "queue_state.json"  

GEMINI_API_KEY = os.environ.get("GOOGLE_API_KEY")
P4N_USER = os.environ.get("P4N_USERNAME") 
P4N_PASS = os.environ.get("P4N_PASSWORD") 

def ts_print(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {msg}")

class PipelineLogger:
    _initialized = False

    @staticmethod
    def log_event(event_type, data):
        processed_content = {}
        for k, v in data.items():
            if isinstance(v, str) and (v.strip().startswith('{') or v.strip().startswith('[')):
                try: processed_content[k] = json.loads(v)
                except: processed_content[k] = v
            else: processed_content[k] = v
        
        log_entry = {"timestamp": datetime.now().isoformat(), "type": event_type, "content": processed_content}
        mode = "w" if not PipelineLogger._initialized else "a"
        if not PipelineLogger._initialized: PipelineLogger._initialized = True

        with open(LOG_FILE, mode, encoding="utf-8") as f:
            header = f"\n{'='*30} {event_type} {'='*30}\n"
            f.write(header + json.dumps(log_entry, indent=4, default=str, ensure_ascii=False) + "\n")

class DailyQueueManager:
    @staticmethod
    def get_next_partition():
        if not os.path.exists(URL_LIST_FILE):
            ts_print(f"❌ ERROR: {URL_LIST_FILE} not found.")
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

client = genai.Client(api_key=GEMINI_API_KEY)

class P4NScraper:
    def __init__(self, is_dev=False, force=False):
        self.is_dev = is_dev
        self.force = force 
        self.csv_file = DEV_CSV if is_dev else PROD_CSV
        self.processed_batch = []
        self.existing_df = self._load_existing()
        self.stats = {"read": 0, "discarded_fresh": 0, "discarded_low_feedback": 0, "gemini_calls": 0}
        self.semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

    def _load_existing(self):
        if os.path.exists(self.csv_file):
            try:
                df = pd.read_csv(self.csv_file)
                df['last_scraped'] = pd.to_datetime(df['last_scraped'], errors='coerce')
                return df
            except: pass
        return pd.DataFrame()

    async def analyze_with_ai(self, raw_data):
        self.stats["gemini_calls"] += 1
        PipelineLogger.log_event("SENT_TO_GEMINI", raw_data)
        
        # ENHANCED STRATEGIC PROMPT
        system_instruction = """Analyze the provided property data and reviews. You MUST identify recurring themes and count their occurrences across all reviews. Return JSON ONLY. Use snake_case.

        Schema:
        {
          "num_places": int,
          "parking_min": float,
          "parking_max": float,
          "electricity_eur": float,
          "top_languages": [ {"lang": "string", "count": int} ],
          "pros_cons": {
            "pros": [ {"topic": "string", "count": int} ],
            "cons": [ {"topic": "string", "count": int} ]
          }
        }

        Instructions:
        1. num_places: Extract from the 'places_count' field.
        2. Pricing: Extract min/max range. If only one price exists, set both. If included in parking, set electricity_eur to 0.0.
        3. Languages: Detect review languages and provide frequency counts.
        4. Themes: Extract pro/con themes (3-5 words max). You MUST provide a count for each theme.
        5. Priorities: For 'cons', highlight overcrowding, police/fines, or lack of services to help calculate the Frustration Index.
        6. Missing Data: Use null for missing numeric data. Do not hallucinate counts.

        Example Theme Format: {"topic": "quiet at night", "count": 5}"""
        
        json_payload = json.dumps(raw_data, default=str, ensure_ascii=False)
        # Added response_mime_type for strict JSON enforcement
        config = types.GenerateContentConfig(
            response_mime_type="application/json", 
            temperature=0.1, 
            system_instruction=system_instruction
        )
        try:
            await asyncio.sleep(AI_DELAY) 
            response = await client.aio.models.generate_content(model=MODEL_NAME, contents=f"ANALYZE:\n{json_payload}", config=config)
            clean_text = re.sub(r'```json\s*|\s*```', '', response.text).strip()
            ai_json = json.loads(clean_text)
            PipelineLogger.log_event("GEMINI_ANSWER", ai_json)
            return ai_json
        except Exception as e:
            PipelineLogger.log_event("GEMINI_ERROR", {"error": str(e)})
            return {}

    async def extract_atomic(self, context, url, current_num, total_num):
        async with self.semaphore:
            if self.is_dev and self.stats["read"] >= DEV_LIMIT:
                return

            ts_print(f"➡️  [{current_num}/{total_num}] Scraped Item: {url}")
            page = await context.new_page()
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_selector(".place-feedback-average", timeout=10000)

                stats_container = page.locator(".place-feedback-average")
                raw_count_text = await stats_container.locator("strong").text_content()
                actual_feedback_count = int(re.search(r'(\d+)', raw_count_text).group(1))
                
                if actual_feedback_count < MIN_REVIEWS_THRESHOLD:
                    ts_print(f"🗑️  [DISCARD] Low feedback ({actual_feedback_count} reviews) for: {url}")
                    self.stats["discarded_low_feedback"] += 1
                    return

                p_id = await page.locator("body").get_attribute("data-place-id") or url.split("/")[-1]
                title = (await page.locator("h1").first.text_content()).split('\n')[0].strip()

                lat, lng = 0.0, 0.0
                coord_link_el = page.locator("a[href*='lat='][href*='lng=']").first
                coord_link = await coord_link_el.get_attribute("href") if await coord_link_el.count() > 0 else None
                if coord_link:
                    m = re.search(r'lat=([-+]?\d*\.\d+|\d+)&lng=([-+]?\d*\.\d+|\d+)', coord_link)
                    if m: lat, lng = float(m.group(1)), float(m.group(2))

                # Fetch ALL reviews using text_content to capture hidden ones
                review_articles = await page.locator(".place-feedback-article").all()
                formatted_reviews, review_seasonality = [], {}

                for article in review_articles:
                    try:
                        date_text = await article.locator("span.caption.text-gray").text_content()
                        text_val = await article.locator(".place-feedback-article-content").text_content()
                        date_parts = date_text.strip().split('/')
                        if len(date_parts) == 3:
                            month_key = f"{date_parts[2]}-{date_parts[1]}"
                            review_seasonality[month_key] = review_seasonality.get(month_key, 0) + 1
                            date_val = f"{date_parts[2]}-{date_parts[1]}-{date_parts[0]}"
                        else: date_val = "Unknown"
                        formatted_reviews.append(f"[{date_val}]: {text_val.strip()}")
                    except: continue

                raw_payload = {
                    "places_count": await self._get_dl(page, "Number of places"),
                    "parking_cost": await self._get_dl(page, "Parking cost"),
                    "all_reviews": formatted_reviews 
                }
                
                ai_data = await self.analyze_with_ai(raw_payload)
                top_langs = ai_data.get("top_languages", [])
                pros_cons = ai_data.get("pros_cons") or {}
                
                row = {
                    "p4n_id": p_id, "title": title, "url": url, "latitude": lat, "longitude": lng,
                    "location_type": await self._get_type(page),
                    "num_places": ai_data.get("num_places"),
                    "total_reviews": actual_feedback_count, 
                    "avg_rating": float(re.search(r'(\d+\.?\d*)', await stats_container.locator(".text-gray").text_content()).group(1)),
                    "parking_min_eur": ai_data.get("parking_min"),
                    "parking_max_eur": ai_data.get("parking_max"),
                    "electricity_eur": ai_data.get("electricity_eur"),
                    "review_seasonality": json.dumps(review_seasonality),
                    "top_languages": "; ".join([f"{l.get('lang')} ({l.get('count')})" for l in top_langs if isinstance(l, dict)]),
                    "ai_pros": "; ".join([f"{p.get('topic')} ({p.get('count')})" for p in pros_cons.get('pros', []) if isinstance(p, dict)]),
                    "ai_cons": "; ".join([f"{c.get('topic')} ({c.get('count')})" for c in pros_cons.get('cons', []) if isinstance(c, dict)]),
                    "last_scraped": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                PipelineLogger.log_event("STORED_ROW", row)
                self.processed_batch.append(row)
                self.stats["read"] += 1
            except Exception as e: 
                ts_print(f"⚠️ Error: {e}")
            finally:
                await page.close()

    async def _get_type(self, page):
        try: return await page.locator(".place-header-access img").get_attribute("title")
        except: return "Unknown"

    async def _get_dl(self, page, label):
        try: return (await page.locator(f"dt:has-text('{label}') + dd").first.text_content()).strip()
        except: return "N/A"

    async def start(self):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()
            await Stealth().apply_stealth_async(page)
            
            target_urls, current_idx, total_idx = DailyQueueManager.get_next_partition()
            ts_print(f"📅 [PARTITION] Day {current_idx} of {total_idx}")
            
            discovery_links = []
            for url in target_urls:
                await page.goto(url, wait_until="domcontentloaded")
                try: await page.wait_for_selector("a[href*='/place/']", timeout=5000)
                except: pass
                links = await page.locator("a[href*='/place/']").all()
                for link in links:
                    href = await link.get_attribute("href")
                    if href: discovery_links.append(f"https://park4night.com{href}" if href.startswith("/") else href)

            discovered = list(set(discovery_links))
            
            if self.is_dev:
                ts_print(f"🛠️  [DEV MODE] Seeking {DEV_LIMIT} successful run(s)...")
                
            tasks = []
            for link in discovered:
                if self.is_dev and self.stats["read"] >= DEV_LIMIT:
                    break
                    
                p_id = link.split("/")[-1]
                is_stale = True
                if not self.force and not self.existing_df.empty and str(p_id) in self.existing_df['p4n_id'].astype(str).values:
                    last_date = self.existing_df[self.existing_df['p4n_id'].astype(str) == str(p_id)]['last_scraped'].iloc[0]
                    if pd.notnull(last_date) and (datetime.now() - pd.to_datetime(last_date)) < timedelta(days=STALENESS_DAYS):
                        is_stale = False
                
                if is_stale or self.force:
                    if self.is_dev:
                        await self.extract_atomic(context, link, self.stats["read"] + 1, "Seeking...")
                        if self.stats["read"] >= DEV_LIMIT: break
                    else:
                        tasks.append(self.extract_atomic(context, link, len(tasks) + 1, len(discovered)))
                else: 
                    ts_print(f"⏩  [SKIP] Listing fresh: {link}")
                    self.stats["discarded_fresh"] += 1

            if tasks:
                await asyncio.gather(*tasks)
                
            await browser.close()
            self._upsert_and_save()
            
            ts_print("="*40)
            ts_print("🏁 [RUN SUMMARY]")
            ts_print(f"✅ Items Successfully Processed: {self.stats['read']}")
            ts_print(f"⏩ Items Discarded (Fresh): {self.stats['discarded_fresh']}")
            ts_print(f"🗑️  Items Discarded (Low Feedback): {self.stats['discarded_low_feedback']}")
            ts_print(f"🤖 Total Gemini AI Calls: {self.stats['gemini_calls']}")
            ts_print("="*40)

            if not self.is_dev: DailyQueueManager.increment_state()

    def _upsert_and_save(self):
        if not self.processed_batch: return
        new_df = pd.DataFrame(self.processed_batch)
        final_df = pd.concat([new_df, self.existing_df], ignore_index=True)
        final_df.sort_values('last_scraped', ascending=False).drop_duplicates('p4n_id').to_csv(self.csv_file, index=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--dev', action='store_true')
    parser.add_argument('--force', action='store_true')
    args = parser.parse_args()
    asyncio.run(P4NScraper(is_dev=args.dev, force=args.force).start())
