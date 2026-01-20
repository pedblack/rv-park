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

# --- CONFIG ---
MAX_REVIEWS = 100
MODEL_NAME = "gemini-2.5-flash-lite"
PROD_CSV = "backbone_locations.csv"
DEV_CSV = "backbone_locations_dev.csv"
LOG_FILE = "pipeline_execution.log"
AI_DELAY = 0.5 

GEMINI_API_KEY = os.environ.get("GOOGLE_API_KEY")
P4N_USER = os.environ.get("P4N_USERNAME")
P4N_PASS = os.environ.get("P4N_PASSWORD")

class PipelineLogger:
    @staticmethod
    def log_event(event_type, data):
        """Saves timestamped events to a JSONL file."""
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "type": event_type,
            "content": data
        }
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(log_entry) + "\n")

    @staticmethod
    async def save_screenshot(page, name):
        path = f"debug_{name}_{datetime.now().strftime('%H%M%S')}.png"
        await page.screenshot(path=path)
        print(f"📸 Debug screenshot saved: {path}")

# Initialize Client
client = genai.Client(api_key=GEMINI_API_KEY)

class P4NScraper:
    def __init__(self, is_dev=False):
        self.is_dev = is_dev
        self.csv_file = DEV_CSV if is_dev else PROD_CSV
        self.current_max_reviews = 1 if is_dev else MAX_REVIEWS
        self.processed_batch = []
        self.existing_df = self._load_existing()

    def _load_existing(self):
        if os.path.exists(self.csv_file):
            try:
                df = pd.read_csv(self.csv_file)
                df['last_scraped'] = pd.to_datetime(df['last_scraped'])
                return df
            except: pass
        return pd.DataFrame()

    async def login(self, page):
        """Robust Modal Login with Verification."""
        if not P4N_USER or not P4N_PASS: return
        print(f"🔐 Attempting Login for {P4N_USER}...")
        try:
            await page.goto("https://park4night.com/en", wait_until="networkidle")
            try: await page.click(".cc-btn-accept", timeout=3000)
            except: pass

            # Open Dropdown and click Login
            await page.click(".pageHeader-account-button")
            await asyncio.sleep(1)
            await page.click("button[data-bs-target='#signinModal']", force=True)

            # Fill Modal
            await page.wait_for_selector("#signinUserId", state="visible")
            await page.fill("#signinUserId", P4N_USER)
            await page.fill("#signinPassword", P4N_PASS)
            await page.keyboard.press("Enter")
            await page.wait_for_load_state("networkidle")
            await asyncio.sleep(3)

            # Verify Success
            user_span = page.locator(".pageHeader-account-button span")
            actual_username = await user_span.inner_text()
            
            if P4N_USER.lower() in actual_username.lower():
                print(f"✅ Logged in as: {actual_username}")
                PipelineLogger.log_event("LOGIN_SUCCESS", {"user": actual_username})
            else:
                print(f"⚠️ Login Verification Failed. Found: '{actual_username}'")
                await PipelineLogger.save_screenshot(page, "login_verify_failed")
                PipelineLogger.log_event("LOGIN_WARNING", {"expected": P4N_USER, "found": actual_username})
        except Exception as e:
            print(f"❌ Login UI Error: {e}")
            await PipelineLogger.save_screenshot(page, "login_exception")
            PipelineLogger.log_event("LOGIN_ERROR", {"error": str(e)})

    async def analyze_with_ai(self, raw_data):
        """Atomic AI request with full prompt and response logging."""
        system_instr = "Normalize costs to EUR and summarize reviews to English pros/cons."
        prompt = f"Analyze property data. Return JSON only:\n{json.dumps(raw_data)}"
        
        # --- LOG FULL REQUEST ---
        PipelineLogger.log_event("AI_REQUEST", {
            "p4n_id": raw_data.get("p4n_id"),
            "model": MODEL_NAME,
            "system_instruction": system_instr,
            "full_prompt": prompt # This logs the exact text sent
        })

        config = types.GenerateContentConfig(
            response_mime_type="application/json",
            temperature=0.1,
            system_instruction=system_instr
        )

        try:
            await asyncio.sleep(AI_DELAY) 
            response = await client.aio.models.generate_content(
                model=MODEL_NAME, contents=prompt, config=config
            )
            
            # --- LOG FULL RESPONSE ---
            PipelineLogger.log_event("AI_RESPONSE", {
                "p4n_id": raw_data.get("p4n_id"),
                "raw_text": response.text
            })
            return json.loads(response.text)
        except Exception as e:
            PipelineLogger.log_event("AI_ERROR", {
                "p4n_id": raw_data.get("p4n_id"),
                "error": str(e)
            })
            return {}

    async def extract_atomic(self, page, url):
        print(f"📄 Scraping: {url}")
        try:
            await page.goto(url, wait_until="domcontentloaded")
            await asyncio.sleep(random.uniform(2, 4))
            
            p_id = await page.locator("body").get_attribute("data-place-id") or url.split("/")[-1]
            title = (await page.locator("h1").first.inner_text()).split('\n')[0].strip()
            review_els = await page.locator(".place-feedback-article-content").all()
            
            raw_payload = {
                "p4n_id": p_id,
                "parking_cost": await self._get_dl(page, "Parking cost"),
                "reviews": [await r.inner_text() for r in review_els[:self.current_max_reviews]]
            }

            ai_data = await self.analyze_with_ai(raw_payload)

            row = {
                "p4n_id": p_id,
                "title": title,
                "url": url,
                "parking_min_eur": ai_data.get("parking_min", 0),
                "parking_max_eur": ai_data.get("parking_max", 0),
                "ai_pros": ai_data.get("pros", "N/A"),
                "ai_cons": ai_data.get("cons", "N/A"),
                "last_scraped": datetime.now()
            }
            
            PipelineLogger.log_event("ROW_PREPARED", row)
            self.processed_batch.append(row)
        except Exception as e:
            print(f"⚠️ Extraction Error: {e}")

    async def _get_dl(self, page, label):
        try: return (await page.locator(f"dt:has-text('{label}') + dd").first.inner_text()).strip()
        except: return "N/A"

    async def start(self):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(user_agent="Mozilla/5.0...")
            page = await context.new_page()
            await Stealth().apply_stealth_async(page)

            await self.login(page)

            # --- DISCOVERY & QUEUE LOGIC ---
            # (Matches previous versions)
            self.discovery_links = [] # Placeholder
            
            # [Logic to populate self.discovery_links from TARGET_URLS]
            # [Logic to filter queue based on staleness]
            queue = [] # Placeholder

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
        print(f"🚀 Success! Total records: {len(final_df)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--dev', action='store_true')
    args = parser.parse_args()
    asyncio.run(P4NScraper(is_dev=args.dev).start())
