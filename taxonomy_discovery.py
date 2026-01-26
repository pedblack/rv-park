import asyncio
import json
import os
import re
import pandas as pd
from datetime import datetime
from google import genai
from google.genai import types
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

# --- CONFIG ---
DISCOVERY_MODEL = "gemini-2.5-flash"
URL_LIST_FILE = "url_list.txt"
TAXONOMY_FILE = "taxonomy.json"  # New source of truth
OUTPUT_FILE = "taxonomy_discovery_report.json"
BATCH_SIZE = 5 

GEMINI_API_KEY = os.environ.get("GOOGLE_API_KEY")
client = genai.Client(api_key=GEMINI_API_KEY)

def ts_print(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def load_current_taxonomy():
    """Reads the current taxonomy from the JSON file and formats it for the AI."""
    if os.path.exists(TAXONOMY_FILE):
        try:
            with open(TAXONOMY_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                
                # Extract the "topic" value from each object in the pros and cons lists
                pro_keys = [item["topic"] for item in data.get("pros", []) if isinstance(item, dict)]
                con_keys = [item["topic"] for item in data.get("cons", []) if isinstance(item, dict)]
                
                pro_keys_str = ", ".join(pro_keys)
                con_keys_str = ", ".join(con_keys)
                
                return f"PRO_KEYS: {pro_keys_str}\nCON_KEYS: {con_keys_str}"
        except Exception as e:
            return f"Error loading taxonomy: {e}"
            
    return "No current taxonomy found."

class TaxonomyDiscoverer:
    def __init__(self):
        self.suggested_keys = []

    async def scrape_url(self, context, url):
        ts_print(f"🌐 Scraping Property: {url}")
        page = await context.new_page()
        reviews = []
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_selector(".place-feedback-article", timeout=10000)
            
            elements = await page.locator(".place-feedback-article-content").all()
            for el in elements[:20]:
                text = await el.text_content()
                if text: reviews.append(text.strip())
        except Exception as e:
            ts_print(f"⚠️ Could not find reviews on {url}")
        finally:
            await page.close()
        return {"url": url, "reviews": reviews}

    async def analyze_batch(self, batch_data):
        valid_data = [d for d in batch_data if d['reviews']]
        if not valid_data:
            return {"new_suggestions": []}

        ts_print(f"🤖 Analyzing batch of {len(valid_data)} properties...")
        
        current_taxonomy = load_current_taxonomy()
        
        system_instruction = f"""You are a qualitative data analyst.
        I am providing reviews for camping locations. 
        Your goal is to find themes that DO NOT fit into my current taxonomy.

        ### MY CURRENT TAXONOMY ###
        {current_taxonomy}

        ### TASK ###
        1. Identify specific feedback points in the new data that are too unique or specific for my existing keys.
        2. For each "outlier", suggest a new 'snake_case' key.
        3. Extract the exact quote from the review that justifies this new key.

        ### OUTPUT JSON SCHEMA ###
        {{
            "new_suggestions": [
                {{"suggested_key": "string", "reasoning": "string", "example_quote": "string"}}
            ]
        }}"""

        config = types.GenerateContentConfig(
            response_mime_type="application/json",
            temperature=0.2,
            system_instruction=system_instruction,
        )

        response = await client.aio.models.generate_content(
            model=DISCOVERY_MODEL,
            contents=f"NEW DATA TO ANALYZE:\n{json.dumps(valid_data)}",
            config=config,
        )
        
        try:
            return json.loads(response.text)
        except:
            ts_print("❌ Failed to parse AI JSON")
            return {"new_suggestions": []}

    async def run(self):
        if not os.path.exists(URL_LIST_FILE):
            ts_print(f"❌ ERROR: {URL_LIST_FILE} not found.")
            return

        with open(URL_LIST_FILE, "r") as f:
            search_urls = [line.strip() for line in f if line.strip()][:3]

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            await Stealth().apply_stealth_async(context)
            
            discovery_links = []
            page = await context.new_page()
            
            for url in search_urls:
                ts_print(f"🔍 [SEARCH PAGE] Finding properties on: {url}")
                try:
                    await page.goto(url, wait_until="domcontentloaded")
                    try:
                        await page.wait_for_selector("a[href*='/place/']", timeout=5000)
                    except:
                        ts_print(f"⚠️ No place links found on {url}")
                        pass
                    
                    links = await page.locator("a[href*='/place/']").all()
                    for link in links:
                        href = await link.get_attribute("href")
                        if href:
                            discovery_links.append(
                                f"https://park4night.com{href}"
                                if href.startswith("/")
                                else href
                            )
                except Exception as e:
                    ts_print(f"⚠️ Search page error for {url}: {e}")

            discovered = list(set(discovery_links))
            await page.close()

            if not discovered:
                ts_print("❌ Still found 0 properties. Please check if search pages are active.")
                await browser.close()
                return

            ts_print(f"✅ Found {len(discovered)} properties. Limiting to first 50 for taxonomy audit.")
            target_sample = discovered[:50]

            for i in range(0, len(target_sample), BATCH_SIZE):
                batch_urls = target_sample[i:i + BATCH_SIZE]
                scrape_tasks = [self.scrape_url(context, u) for u in batch_urls]
                batch_results = await asyncio.gather(*scrape_tasks)
                
                analysis = await self.analyze_batch(batch_results)
                self.suggested_keys.extend(analysis.get("new_suggestions", []))

            await browser.close()

        with open(OUTPUT_FILE, "w") as f:
            json.dump({
                "discovery_timestamp": datetime.now().isoformat(),
                "total_suggestions": len(self.suggested_keys),
                "suggestions": self.suggested_keys
            }, f, indent=4)
        
        ts_print(f"✅ Discovery complete. Results in {OUTPUT_FILE}")

if __name__ == "__main__":
    asyncio.run(TaxonomyDiscoverer().run())
