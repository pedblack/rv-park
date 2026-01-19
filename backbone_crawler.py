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
        stealth = Stealth()
        await stealth.apply_stealth_async(page)
        return browser, page

    async def discover_urls(self, page, search_url):
        """Phase 1: Discovery phase to find location links."""
        print(f"🔍 Searching Discovery URL: {search_url}")
        try:
            await page.goto(search_url, wait_until="networkidle", timeout=60000)
            
            # Handle cookie consent if visible
            try:
                accept_btn = page.locator(".cc-btn-accept")
                if await accept_btn.is_visible(timeout=5000):
                    await accept_btn.click()
            except: 
                pass

            await asyncio.sleep(4)
            links = await page.locator("#searchmap-list-results li a").all()
            for link in links:
                href = await link.get_attribute("href")
                if href and "/place/" in href:
                    full_url = f"https://park4night.com{href}"
                    if full_url not in self.discovery_links:
                        self.discovery_links.append(full_url)
        except Exception as e:
            print(f"⚠️ Discovery Error: {e}")

    async def extract_details(self, page, url):
        """Phase 2: Extract deep metadata from individual spot pages."""
        print(f"📄 Deep Scraping: {url}")
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await asyncio.sleep(random.uniform(3, 5)) 

            # 1. Basic Info & Type
            p4n_id = await page.locator("body").get_attribute("data-place-id")
            
            title_el = page.locator(".place-header-name")
            title = await title_el.inner_text() if await title_el.count() > 0 else "N/A"
            
            type_img = page.locator(".place-header-access img")
            loc_type = await type_img.get_attribute("title") if await type_img.count() > 0 else "Unknown"

            # 2. Ratings and Counter logic
            rating_el = page.locator(".rating-note").first
            rating_raw = await rating_el.inner_text() if await rating_el.count() > 0 else "0"
            rating_val = rating_raw.split('/')[0].strip()
            
            feedback_el = page.locator(".place-feedback-average")
            feedback_text = await feedback_el.inner_text() if await feedback_el.count() > 0 else "(0 Feedback)"
            ratings_count_match = re.search(r'\((\d+)\s+', feedback_text)
            ratings_count = ratings_count_match.group(1) if ratings_count_match else "0"
            
            # 3. Counters (Photos, Services, Activities)
            photos_count = await page.locator("body").get_attribute("data-images-length") or "0"
            
            specs_rows = page.locator(".place-specs .row")
            services_count = await specs_rows.nth(0).locator("li").count() if await specs_rows.count() > 0 else 0
            activities_count = await specs_rows.nth(1).locator("li").count() if await specs_rows.count() > 1 else 0

            # 4. Detail list extraction using text-based relative selectors
            async def get_metadata_val(label):
                try:
                    # Select the <dd> that follows a <dt> containing the label text
                    element = page.locator(f"dl.place-info-details dt:has-text('{label}') + dd")
                    if await element.count() > 0:
                        return (await element.inner_text()).strip()
                    return "N/A"
                except: 
                    return "N/A"

            service_price = await get_metadata_val("Price of services")
            parking_cost = await get_metadata_val("Parking cost")
            num_places = await get_metadata_val("Number of places")
            
            # Fix for Creation Date logic
            creation_el = page.locator(".place-header-creation")
            creation_raw = await creation_el.inner_text() if await creation_el.count() > 0 else ""
            creation_date_match = re.search(r'(\d{2}\.\d{2}\.\d{4})', creation_raw)
            creation_date = creation_date_match.group(1) if creation_date_match else "N/A"

            self.final_results.append({
                "p4n_id": p4n_id,
                "title": title.strip(),
                "type": loc_type,
                "rating": rating_val,
                "ratings_count": ratings_count,
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
            print(f"⚠️ Detail Extraction Error for {url}: {e}")

    async def run(self):
        async with async_playwright() as p:
            browser, page = await self.init_browser(p)
            
            # Discovery Phase
            for url in TARGET_URLS:
                await self.discover_urls(page, url)
            
            # Filtering unique links
            unique_links = [link for link in self.discovery_links if "/place/" in link]
            print(f"✅ Discovery complete. Found {len(unique_links)} unique spots to crawl.")

            # Detail Extraction Phase
            for link in unique_links:
                await self.extract_details(page, link)
            
            await browser.close()
            return pd.DataFrame(self.final_results)

async def main():
    scraper = P4NScraper()
    df = await scraper.run()
    
    if not df.empty:
        # Final Clean up
        df.drop_duplicates(subset=['p4n_id'], inplace=True)
        df = df[~df['title'].str.contains("ADVERTISING", case=False, na=False)]
        
        df.to_csv("backbone_locations.csv", index=False)
        print(f"🚀 Success! {len(df)} locations written to backbone_locations.csv.")
    else:
        print("❌ No data was collected.")

if __name__ == "__main__":
    asyncio.run(main())
