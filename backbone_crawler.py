import asyncio
import random
import re
import os
import pandas as pd
import google.generativeai as genai
import json
from datetime import datetime, timedelta
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

# Configuration
CSV_FILE = "backbone_locations.csv"
GEMINI_API_KEY = os.environ.get("GOOGLE_API_KEY", "AIzaSyD_A_bYXFkkOLzpXgPuvje39x4w7YPOfzs")

# Initialize Gemini with JSON mode for schema stability
genai.configure(api_key=GEMINI_API_KEY)
ai_model = genai.GenerativeModel(
    'gemini-1.5-flash',
    generation_config={"response_mime_type": "application/json"}
)

class P4NScraper:
    def __init__(self):
        self.discovery_links = []
        self.new_data = []
        self.existing_df = self.load_existing_data()

    def load_existing_data(self):
        if os.path.exists(CSV_FILE):
            try:
                df = pd.read_csv(CSV_FILE)
                df['last_scraped'] = pd.to_datetime(df['last_scraped'])
                return df
            except: pass
        return pd.DataFrame(columns=["p4n_id", "last_scraped"])

    async def init_browser(self, p):
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={'width': 1280, 'height': 800}
        )
        page = await context.new_page()
        await Stealth().apply_stealth_async(page)
        return browser, page

    async def summarize_and_detect_languages(self, reviews):
        """Single LLM call to get summary AND language distribution signal."""
        if not reviews:
            return "No reviews found.", "N/A", "{}"
        
        text_block = "\n".join(reviews)[:8000] # Increased context window for better language signal
        prompt = f"""
        Analyze these campsite reviews. Return a JSON object with these keys:
        - 'pros': Concise bullet list of positives in English.
        - 'cons': Concise bullet list of recurrent issues in English.
        - 'lang_dist': A dictionary where keys are the ISO language code (e.g. 'fr', 'pt', 'en', 'de') 
          and values are the count of reviews in that language.
        
        Reviews:
        {text_block}
        """
        try:
            # Respect Gemini Free Tier RPM (15 calls per minute)
            await asyncio.sleep(4) 
            response = await ai_model.generate_content_async(prompt)
            data = json.loads(response.text)
            
            # Convert dict to string for CSV storage
            lang_signal = json.dumps(data.get('lang_dist', {}))
            return data.get('pros', 'N/A'), data.get('cons', 'N/A'), lang_signal
        except Exception as e:
            print(f"AI Signal Error: {e}")
            return "Analysis failed.", "N/A", "{}"

    async def extract_atomic(self, page, url):
        """ATOMIC EXTRACTION: Detail metadata + Comments in ONE visit."""
        print(f"📄 Atomic Scrape: {url}")
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await asyncio.sleep(random.uniform(3, 5))

            # 1. Scrape all raw comments first
            comment_elements = await page.locator(".place-feedback-article-content").all()
            raw_reviews = [await c.inner_text() for c in comment_elements]
            
            # 2. Enrich via LLM (Summary + Language Signal)
            pros, cons, lang_dist = await self.summarize_and_detect_languages(raw_reviews)

            # 3. Scrape Metadata
            p4n_id = await page.locator("body").get_attribute("data-place-id")
            title = (await page.locator(".place-header-name").inner_text()).strip()
            
            async def get_dl(label):
                try: return (await page.locator(f"dl.place-info-details dt:has-text('{label}') + dd").inner_text()).strip()
                except: return "N/A"

            self.new_data.append({
                "p4n_id": p4n_id,
                "title": title,
                "type": await page.locator(".place-header-access img").get_attribute("title"),
                "rating": (await page.locator(".rating-note").first.inner_text()).split('/')[0].strip() if await page.locator(".rating-note").count() > 0 else "0",
                "photos_count": await page.locator("body").get_attribute("data-images-length") or "0",
                "service_price": await get_dl("Price of services"),
                "parking_cost": await get_dl("Parking cost"),
                "num_places": await get_dl("Number of places"),
                "ai_summary_pros": pros,
                "ai_summary_cons": cons,
                "comment_languages": lang_dist, # NEW SIGNAL
                "url": url,
                "last_scraped": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
        except Exception as e:
            print(f"⚠️ Atomic Error: {url} -> {e}")

    async def run(self):
        async with async_playwright() as p:
            browser, page = await self.init_browser(p)
            
            # Discovery Phase
            search_urls = [
                "https://park4night.com/en/search?lat=37.63658110718217&lng=-8.638597348689018&z=10",
                "https://park4night.com/en/search?lat=37.87856774592691&lng=-8.568677272965147&z=10"
            ]
            for url in search_urls:
                await page.goto(url, wait_until="networkidle")
                await asyncio.sleep(2)
                links = await page.locator("#searchmap-list-results li a").all()
                for link in links:
                    href = await link.get_attribute("href")
                    if href and "/place/" in href:
                        self.discovery_links.append(f"https://park4night.com{href}")

            # Delta Filter (1-week rule)
            queue = []
            for l in list(set(self.discovery_links)):
                p_id = re.search(r'/place/(\d+)', l).group(1)
                if p_id not in self.existing_df['p4n_id'].astype(str).values:
                    queue.append(l)
                else:
                    last_date = self.existing_df[self.existing_df['p4n_id'].astype(str) == p_id]['last_scraped'].iloc[0]
                    if (datetime.now() - last_date) > timedelta(days=7):
                        queue.append(l)

            print(f"✅ Queue: {len(queue)} items for atomic scrape.")
            for link in queue:
                await self.extract_atomic(page, link)
            
            await browser.close()
            self.save_data()

    def save_data(self):
        if not self.new_data: return
        new_df = pd.DataFrame(self.new_data)
        combined = pd.concat([new_df, self.existing_df], ignore_index=True)
        combined.sort_values('last_scraped', ascending=False).drop_duplicates('p4n_id').to_csv(CSV_FILE, index=False)

if __name__ == "__main__":
    asyncio.run(P4NScraper().run())
