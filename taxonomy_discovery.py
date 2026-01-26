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
PROMPT_FILE = "extraction_prompt.txt" # Externalized prompt file
OUTPUT_FILE = "taxonomy_discovery_report.json"
BATCH_SIZE = 5 # Increased for better pattern recognition

GEMINI_API_KEY = os.environ.get("GOOGLE_API_KEY")
client = genai.Client(api_key=GEMINI_API_KEY)

def ts_print(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def load_current_taxonomy():
    """Reads the current taxonomy from your production prompt file."""
    if os.path.exists(PROMPT_FILE):
        with open(PROMPT_FILE, "r", encoding="utf-8") as f:
            return f.read()
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
            # Wait for reviews to actually exist on the page
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
        # Filter out properties with zero reviews before sending to AI
        valid_data = [d for d in batch_data if d['reviews']]
        if not valid_data:
            return {"new_suggestions": []}

        ts_print(f"🤖 Analyzing batch of {len(valid_data)} properties...")
        
        current_taxonomy = load_current_taxonomy()
        
        system_instruction = f"""You are a qualitative data analyst.
        I am providing reviews for camping locations. 
        Your goal is to find themes that DO NOT fit into my current taxonomy.

        ### MY CURRENT TAXONOMY AND INSTRUCTIONS ###
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
            ts_print("❌ No url_list.txt found.")
            return

        with open(URL_LIST_FILE, "r") as f:
            search_urls = [line.strip() for line in f if line.strip()][:3]

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            await Stealth().apply_stealth_async(context)
            
            # Step 1: Discover Property URLs from Search Pages
            property_urls = []
            discovery_page = await context.new_page()
            for s_url in search_urls:
                ts_print(f"🔍 Finding properties on search page: {s_url}")
                try:
                    await discovery_page.goto(s_url, wait_until="domcontentloaded")
                    links = await discovery_page.locator("a[href*='/place/']").all()
                    for link in links:
                        href = await link.get_attribute("href")
                        if href:
                            full_url = f"https://park4night.com{href}" if href.startswith("/") else href
                            property_urls.append(full_url)
                except Exception as e:
                    ts_print(f"⚠️ Search page error: {e}")
            await discovery_page.close()

            property_urls = list(set(property_urls))[:15] 
            ts_print(f"✅ Found {len(property_urls)} properties. Starting review extraction...")

            # Step 2: Scrape and Analyze
            for i in range(0, len(property_urls), BATCH_SIZE):
                batch_urls = property_urls[i:i + BATCH_SIZE]
                scrape_tasks = [self.scrape_url(context, u) for u in batch_urls]
                batch_results = await asyncio.gather(*scrape_tasks)
                
                analysis = await self.analyze_batch(batch_results)
                self.suggested_keys.extend(analysis.get("new_suggestions", []))

            await browser.close()

        # Save final report
        with open(OUTPUT_FILE, "w") as f:
            json.dump({
                "discovery_timestamp": datetime.now().isoformat(),
                "suggestions": self.suggested_keys
            }, f, indent=4)
        
        ts_print(f"✅ Discovery complete. Results in {OUTPUT_FILE}")

if __name__ == "__main__":
    asyncio.run(TaxonomyDiscoverer().run())
