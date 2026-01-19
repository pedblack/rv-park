import asyncio
import random
import re
import os
import pandas as pd
from datetime import datetime, timedelta
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

# Configuration
CSV_FILE = "backbone_locations.csv"
TARGET_URLS = [
    "https://park4night.com/en/search?lat=37.63658110718217&lng=-8.638597348689018&z=10",
    "https://park4night.com/en/search?lat=37.87856774592691&lng=-8.568677272965147&z=10"
]

class P4NScraper:
    def __init__(self):
        self.discovery_links = []
        self.new_data = []
        self.existing_df = self.load_existing_data()

    def load_existing_data(self):
        """Loads existing database or creates a new one with correct headers."""
        if os.path.exists(CSV_FILE):
            try:
                df = pd.read_csv(CSV_FILE)
                # Ensure last_scraped is datetime
                if 'last_scraped' in df.columns:
                    df['last_scraped'] = pd.to_datetime(df['last_scraped'])
                return df
            except Exception as e:
                print(f"Error loading CSV: {e}")
        
        return pd.DataFrame(columns=[
            "p4n_id", "title", "type", "rating", "ratings_count", 
            "photos_count", "services_count", "activities_count", 
            "service_price", "parking_cost", "num_places", 
            "creation_date", "url", "last_scraped"
        ])

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
        """Phase 1: Find all location links on the map."""
        print(f"🔍 Discovery phase: {search_url}")
        try:
            await page.goto(search_url, wait_until="networkidle", timeout=60000)
            try:
                # Cookie acceptance
                accept_btn = page.locator(".cc-btn-accept")
                if await accept_btn.is_visible(timeout=3000):
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
            print(f"⚠️ Discovery Error: {e}")

    def should_scrape(self, url):
        """Determines if a URL is new or stale (> 7 days)."""
        p4n_id_match = re.search(r'/place/(\d+)', url)
        if not p4n_id_match:
            return False
        
        p4n_id = p4n_id_match.group(1)
        
        # If ID not in database, scrape it
        if p4n_id not in self.existing_df['p4n_id'].astype(str).values:
            return True
        
        # If ID exists, check the date
        row = self.existing_df[self.existing_df['p4n_id'].astype(str) == p4n_id]
        last_date = row['last_scraped'].iloc[0]
        
        # Scrape only if last scrape was more than 7 days ago
        return (datetime.now() - last_date) > timedelta(days=7)

    async def extract_details(self, page, url):
        """Phase 2: Deep extraction for stale or new URLs."""
        print(f"📄 Deep Scraping (New or Stale): {url}")
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await asyncio.sleep(random.uniform(3, 5)) 

            p4n_id = await page.locator("body").get_attribute("data-place-id")
            title = await page.locator(".place-header-name").inner_text()
            type_img = page.locator(".place-header-access img")
            loc_type = await type_img.get_attribute("title") if await type_img.count() > 0 else "Unknown"

            rating_el = page.locator(".rating-note").first
            rating = (await rating_el.inner_text()).split('/')[0].strip() if await rating_el.count() > 0 else "0"
            
            feedback_el = page.locator(".place-feedback-average")
            feedback_text = await feedback_el.inner_text() if await feedback_el.count() > 0 else "0"
            ratings_count = re.search(r'\((\d+)\s+', feedback_text).group(1) if "(" in feedback_text else "0"
            
            photos_count = await page.locator("body").get_attribute("data-images-length") or "0"
            specs_rows = page.locator(".place-specs .row")
            services_count = await specs_rows.nth(0).locator("li").count() if await specs_rows.count() > 0 else 0
            activities_count = await specs_rows.nth(1).locator("li").count() if await specs_rows.count() > 1 else 0

            async def get_val(label):
                try:
                    return (await page.locator(f"dl.place-info-details dt:has-text('{label}') + dd").inner_text()).strip()
                except: return "N/A"

            self.new_data.append({
                "p4n_id": p4n_id,
                "title": title.strip(),
                "type": loc_type,
                "rating": rating,
                "ratings_count": ratings_count,
                "photos_count": photos_count,
                "services_count": services_count,
                "activities_count": activities_count,
                "service_price": await get_val("Price of services"),
                "parking_cost": await get_val("Parking cost"),
                "num_places": await get_val("Number of places"),
                "creation_date": re.search(r'(\d{2}\.\d{2}\.\d{4})', await page.locator(".place-header-creation").inner_text()).group(1) if await page.locator(".place-header-creation").count() > 0 else "N/A",
                "url": url,
                "last_scraped": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
        except Exception as e:
            print(f"⚠️ Detail Error: {url} -> {e}")

    async def run(self):
        async with async_playwright() as p:
            browser, page = await self.init_browser(p)
            for url in TARGET_URLS:
                await self.discover_urls(page, url)
            
            # Filter links based on the 1-week rule
            links_to_scrape = [l for l in self.discovery_links if self.should_scrape(l)]
            print(f"✅ Found {len(self.discovery_links)} links. {len(links_to_scrape)} require (re)scraping.")

            for link in links_to_scrape:
                await self.extract_details(page, link)
            
            await browser.close()
            return self.merge_and_save()

    def merge_and_save(self):
        if not self.new_data:
            print("No new data to merge.")
            return self.existing_df

        new_df = pd.DataFrame(self.new_data)
        new_df['last_scraped'] = pd.to_datetime(new_df['last_scraped'])
        
        # Professional Upsert: Combine, Sort by Date, Drop older duplicates
        combined_df = pd.concat([new_df, self.existing_df], ignore_index=True)
        combined_df = combined_df.sort_values('last_scraped', ascending=False)
        combined_df = combined_df.drop_duplicates(subset=['p4n_id'], keep='first')
        
        combined_df.to_csv(CSV_FILE, index=False)
        return combined_df

async def main():
    scraper = P4NScraper()
    df = await scraper.run()
    print(f"📊 Database updated. Total records: {len(df)}")

if __name__ == "__main__":
    asyncio.run(main())
