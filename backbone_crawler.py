import asyncio
import random
import re
import pandas as pd
from playwright.async_api import async_playwright
from playwright_stealth import Stealth
from tenacity import retry, stop_after_attempt, wait_exponential

# Configuration based on your specific URL
LANDING_URL = "https://park4night.com/en/search?lat=37.63658110718217&lng=-8.638597348689018&z=10"
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
]

class P4NScraper:
    def __init__(self):
        self.results = []

    async def init_browser(self, p):
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport={'width': 1280, 'height': 800}
        )
        page = await context.new_page()
        stealth = Stealth()
        await stealth.apply_stealth_async(page)
        return browser, page

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=4, max=10))
    async def scrape_locations(self):
        async with async_playwright() as p:
            browser, page = await self.init_browser(p)
            try:
                print(f"Opening landing page...")
                await page.goto(LANDING_URL, wait_until="networkidle")

                # 1. Handle Cookie Consent (using your specific class)
                try:
                    accept_btn = page.locator(".cc-btn-accept")
                    if await accept_btn.is_visible():
                        await accept_btn.click()
                        print("Cookies accepted.")
                except Exception:
                    print("Cookie banner not found or already closed.")

                # Small delay to let the list populate after consent
                await asyncio.sleep(random.uniform(2, 4))

                # 2. Extract results from the specific UL ID
                # We use the 'li a' pattern identified in your HTML dump
                result_links = await page.locator("#searchmap-list-results li a").all()
                print(f"Found {len(result_links)} potential locations.")

                for link in result_links:
                    href = await link.get_attribute("href")
                    raw_text = await link.inner_text()

                    # Extract p4n_id from URL (e.g., /en/place/30443 -> 30443)
                    p4n_id_match = re.search(r'/place/(\d+)', href)
                    p4n_id = p4n_id_match.group(1) if p4n_id_match else "N/A"

                    # Clean title: Removes the (7630-592) postal code prefix if desired
                    clean_title = re.sub(r'\(.*?\)', '', raw_text).strip()

                    self.results.append({
                        "p4n_id": p4n_id,
                        "title": clean_title,
                        "url": f"https://park4night.com{href}"
                    })

                return self.results

            finally:
                await browser.close()

async def main():
    scraper = P4NScraper()
    data = await scraper.scrape_locations()
    if data:
        df = pd.DataFrame(data)
        df.to_csv("backbone_locations.csv", index=False)
        print(f"Success! {len(df)} locations saved to backbone_locations.csv")
    else:
        print("No data collected.")

if __name__ == "__main__":
    asyncio.run(main())
