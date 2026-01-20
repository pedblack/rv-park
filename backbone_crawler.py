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
AI_DELAY = 0.5 

GEMINI_API_KEY = os.environ.get("GOOGLE_API_KEY")
P4N_USER = os.environ.get("P4N_USERNAME")
P4N_PASS = os.environ.get("P4N_PASSWORD")

class PipelineLogger:
    @staticmethod
    def log_event(event_type, data):
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "type": event_type,
            "content": data
        }
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(log_entry) + "\n")

client = genai.Client(api_key=GEMINI_API_KEY)

class P4NScraper:
    def __init__(self, is_dev=False):
        self.is_dev = is_dev
        self.csv_file = DEV_CSV if is_dev else PROD_CSV
        self.max_reviews = 1 if is_dev else 100
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
        """Automates login and verifies success by checking for username in header."""
        if not P4N_USER or not P4N_PASS:
            PipelineLogger.log_event("LOGIN_SKIP", "No credentials provided")
            return

        print("🔐 Opening Login Modal...")
        try:
            await page.goto("https://park4night.com/en", wait_until="networkidle")
            
            # Handle cookies
            try: await page.click(".cc-btn-accept", timeout=3000)
            except: pass

            # Trigger Modal
            await page.click(".pageHeader-account-button")
            await page.click("button[data-bs-target='#signinModal']")

            # Fill Credentials
            await page.wait_for_selector("#signinUserId", state="visible")
            await page.fill("#signinUserId", P4N_USER)
            await page.fill("#signinPassword", P4N_PASS)
            
            # Submit and wait for the page to react
            await page.keyboard.press("Enter")
            await page.wait_for_load_state("networkidle")
            
            # --- LOGIN VERIFICATION ---
            # We look for the span inside the account button that contains the username
            user_span = page.locator(".pageHeader-account-button span")
            actual_username = await user_span.inner_text()
            
            if P4N_USER.lower() in actual_username.lower():
                msg = f"Successfully logged in as {actual_username}"
                print(f"✅ {msg}")
                PipelineLogger.log_event("LOGIN_SUCCESS", {"user": actual_username})
            else:
                msg = f"Login failed or username mismatch. Found: '{actual_username}'"
                print(f"⚠️ {msg}")
                PipelineLogger.log_event("LOGIN_WARNING", {"expected": P4N_USER, "found": actual_username})

        except Exception as e:
            err_msg = f"Login UI Error: {str(e)}"
            print(f"❌ {err_msg}")
            PipelineLogger.log_event("LOGIN_ERROR", {"error": err_msg})

    async def analyze_with_ai(self, raw_data):
        """Tier 1 Atomic AI call with Request/Response logging."""
        PipelineLogger.log_event("AI_REQUEST", {"p4n_id": raw_data.get("p4n_id")})
        
        prompt = f"Analyze property data. Return JSON only:\n{json.dumps(raw_data)}"
        config = types.GenerateContentConfig(response_mime_type="application/json", temperature=0.1)

        try:
            await asyncio.sleep(AI_DELAY) 
            response = await client.aio.models.generate_content(model=MODEL_NAME, contents=prompt, config=config)
            PipelineLogger.log_event("AI_RESPONSE", {"p4n_id": raw_data.get("p4n_id"), "response": response.text})
            return json.loads(response.text)
        except Exception as e:
            PipelineLogger.log_event("AI_ERROR", {"p4n_id": raw_data.get("p4n_id"), "error": str(e)})
            return {}

    async def extract_atomic(self, page, url):
        print(f"📄 Scraping: {url}")
        try:
            await page.goto(url, wait_until="domcontentloaded")
            p_id = await page.locator("body").get_attribute("data-place-id") or url.split("/")[-1]
            title = (await page.locator("h1").first.inner_text()).split('\n')[0].strip()
            
            review_els = await page.locator(".place-feedback-article-content").all()
            raw_payload = {
                "p4n_id": p_id,
                "parking_cost": await self._get_dl(page, "Parking cost"),
                "reviews": [await r.inner_text() for r in review_els[:self.max_reviews]]
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

            # --- DISCOVERY & QUEUE (Simplified for briefness) ---
            # [Insert your Discovery/Queue logic here]
            # For this example, let's assume 'queue' is populated.
            queue = [] 

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
        print(f"🚀 Success! Logs in {LOG_FILE}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--dev', action='store_true')
    args = parser.parse_args()
    asyncio.run(P4NScraper(is_dev=args.dev).start())
