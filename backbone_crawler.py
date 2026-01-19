import asyncio
import random
import re
import pandas as pd
from playwright.async_api import async_playwright
from playwright_stealth import Stealth
from tenacity import retry, stop_after_attempt, wait_exponential

# URLs to crawl for discovery
TARGET_URLS = [
    "https://park4night.com/en/search?lat=37.63658110718217&lng=-8.638597348689018&z=10",
    "https://park4night.com/en/search?lat=37.87856774592691&lng=-8.568677272965147&z=10"
]

class P4NScraper:
    def __init__(self):
        self.discovery_links = []
        self.final_results = []

    async def init_browser(self, p):
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={'width': 1280, 'height': 800}
        )
        page = await context.new_page()
        await Stealth().apply_stealth_async(page)
        return browser, page

    async def discover_urls(self, page, search_url):
        """Phase 1: Find all location links on the search map."""
        print(f"🔍 Searching: {search_url}")
        await page.goto(search_url, wait_until="networkidle")
        
        # Handle initial cookie gate
        try:
            await page.click(".cc-btn-accept", timeout=3000)
        except: pass

        await asyncio.sleep(4)
        links = await page.locator("#searchmap-list-results li a").all()
        for link in links:
            href = await link.get_attribute("href")
            if href and "/place/" in href:
                full_url = f"https://park4night.com{href}"
                if full_url not in self.discovery_links:
                    self.discovery_links.append(full_url)

    async def extract_details(self, page, url):
        """Phase 2: Deep extraction from the individual location page."""
        print(f"📄 Deep Scraping: {url}")
        try:
            await page.goto(url, wait_until="domcontentloaded")
            await asyncio.sleep(random.uniform(3, 6)) # Jitter to prevent detection

            # 1. Identity & Type (using your specific Icon Title request)
            p4n_id = await page.locator("body").get_attribute("data-place-id")
            loc_type = await page.locator(".place-header-access img").get_attribute("title")
            title = await page.locator(".place-header-name").inner_text()

            # 2. Ratings & Meta
            rating = await page.locator(".rating-note").first.inner_text() if await page.locator(".rating-note").count() > 0 else "0"
            feedback_text = await page.locator(".place-feedback-average").inner_text()
            ratings_count = re.search(r'\((\d+)\s+Feedback\)', feedback_text)
            
            # 3. Counts from specific attributes and containers
            photos_count = await page.locator("body").get_attribute("data-images-length")
            
            # Service count (First specs row)
            services_row = page.locator(".place-specs .row").nth(0)
            services_count = await services_row.locator("li").count()
            
            # Activity count (Second specs row)
            activities_row = page.locator(".place-specs .row").nth(1)
            activities_count = await activities_row.locator("li").count()

            # 4. Parsing the Detail List (Price, Cost, Places)
            async def get_dl_data(label):
                try:
                    # Finds the <dt> with text, then gets the immediately following <dd>
                    val = await page.locator(f"dl.place-info-details dt:has-text('{label}') + dd").inner_text()
                    return val.strip()
                except: return "N/A"

            service_price = await get_dl_data("Price of services")
            parking_cost = await get_dl_data("Parking cost")
            num_places = await get_dl_data("Number of places")
            creation_raw = await page.locator(".place-header-creation").inner_text()
            creation_date = re.search(r'(\d{2}\.\d{2}\.\d{4})', creation_raw).group(1) if creation_raw else "N/A"

            self.final_results.append({
                "p4n_id": p4n_id,
                "title": title.strip(),
                "type": loc_type,
                "rating": rating.replace('/5', ''),
                "ratings_count": ratings_count.group(1) if ratings_count else "0",
                "photos_count": photos_count,
                "services_count": services_count,
                "activities_count": activities_count,
                "service_price": service_price,
                "parking_cost": parking_cost,
                "num_places": num_places,
                "creation_date": creation_date,
                "url": url
            })
        except Exception as e:
            print(f"⚠️ Failed {url}: {e}")

    async def run(self):
        async with async_playwright() as p:
            browser, page = await self.init_browser(p)
            
            # Run Discovery
            for url in TARGET_URLS:
                await self.discover_urls(page, url)
            
            # Filter Discovery (Remove Advertising placeholders)
            # ADVERTISING results usually don't have the standard /place/ID structure
            unique_links = [l for l in self.discovery_links if re
