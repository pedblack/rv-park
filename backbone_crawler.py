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
        # Use new Stealth class API
        stealth = Stealth()
        await stealth.apply_stealth_async(page)
        return browser, page

    async def discover_urls(self, page, search_url):
        """Phase 1: Find all location links on the search map."""
        print(f"🔍 Searching: {search_url}")
        try:
            await page.goto(search_url, wait_until="networkidle", timeout=60000)
            
            # Handle initial cookie gate
            try:
                accept_btn = page.locator(".cc-btn-accept")
                if await accept_btn.is_visible(timeout=5000):
                    await accept_btn.click()
            except: pass

            await asyncio.sleep(4)
            links = await page.locator("#searchmap-list-results li a").all()
            for link in links:
                href = await link.get_attribute("href")
                if href and "/place/" in href:
                    full_url = f"https://park4night.com{href}"
                    if full_url not in self.discovery_links:
                        self.discovery_links.append(full_url)
        except Exception as e:
            print(f"⚠️ Discovery failed for {search_url}: {e}")

    async def extract_details(self, page, url):
        """Phase 2: Deep extraction from the individual location page."""
        print(f"📄 Deep Scraping: {url}")
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await asyncio.sleep(random.uniform(3, 5)) 

            # 1. Identity & Type (Natural Keys)
            p4n_id = await page.locator("body").get_attribute("data-place-id")
            title_el = page.locator(".place-header-name")
            title = await title_el.inner_text() if await title_el.count() > 0 else "N/A"
            
            # Type icon title
            type_img = page.locator(".place-header-access img")
            loc_type = await type_img.get_attribute("title") if await type_img.count() > 0 else "Unknown"

            # 2. Ratings & Feedback
            rating_el = page.locator(".rating-note").first
            rating = await rating_el.inner_text() if await rating_el.count() > 0 else "0"
            
            feedback_el = page.locator(".place-feedback-average")
            feedback_text = await feedback_el.inner_text() if await feedback_el.count() > 0 else "(0 Feedback)"
            ratings_count_match = re.search(r'\((\d+)\s+', feedback_text)
            ratings_count = ratings_count_match.group(1) if ratings_count_match else "0"
            
            # 3. Photo, Service, and Activity Counts
            photos_count = await page.locator("body").get_attribute("data-images-length") or "0"
            
            # Targeting rows by sequence as P4N structure is fixed
            specs_rows = page.locator(".place-specs .row")
            services_count = await specs_rows.nth(0).locator("li").count() if await specs_rows.count() > 0 else 0
            activities_count = await specs_rows.nth(1).locator("li").count() if await specs_rows.count() > 1 else 0

            # 4. Parsing the Detail List (Price, Cost, Places)
            async def get_dl_data(label):
                try:
                    val = await page.locator(f"dl.place-info-details dt:has-text('{label}') + dd").inner_text()
                    return val.strip()
                except: return "N/A"

            service_price = await get_dl_data("Price of services")
            parking_cost = await get_dl_data("Parking cost")
            num_places = await get_dl_data("Number of places")
            
            creation_el = page.locator(".place-header-creation")
            creation_raw = await creation_el.inner_text() if await creation_
