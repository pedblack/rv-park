import argparse
import asyncio
import random
import re
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin

import pandas as pd
import tenacity
from playwright.async_api import Page, async_playwright
from playwright_stealth import stealth_async

BASE_URL = "https://park4night.com"
PORTUGAL_URL = "https://park4night.com/en/search?lat=37.63658110718217&lng=-8.638597348689018&z=10"


class Park4NightCrawler:

  def __init__(self, limit: Optional[int] = None, headless: bool = True):
    self.limit = limit
    self.headless = headless
    self.places_data: List[Dict] = []
    self.visited_place_ids: Set[str] = set()

  @tenacity.retry(
      stop=tenacity.stop_after_attempt(3),
      wait=tenacity.wait_fixed(2),
      reraise=True,
  )
  async def navigate(self, page: Page, url: str):
    await page.goto(url, wait_until="domcontentloaded", timeout=60000)

  @tenacity.retry(
      stop=tenacity.stop_after_attempt(3),
      wait=tenacity.wait_fixed(2),
      reraise=True,
  )
  async def accept_cookies(self, page: Page):
    try:
      await page.locator("button#didomi-notice-agree-button").click(
          timeout=5000
      )
      print("Cookies accepted.")
    except Exception:
      print("Cookie banner not found or could not be clicked.")

  @tenacity.retry(
      stop=tenacity.stop_after_attempt(3),
      wait=tenacity.wait_fixed(2),
      reraise=True,
  )
  async def get_place_list_urls(self, page: Page) -> List[str]:
    print(f"Visiting {PORTUGAL_URL} to find place list URLs...")
    await self.navigate(page, PORTUGAL_URL)
    await self.accept_cookies(page)

    subregion_links = await page.locator(
        'a[href*="/list-places/subregion/"]'
    ).all()
    island_links = await page.locator(
        'a[href*="/list-places/region/189-"],'
        ' a[href*="/list-places/region/190-"]'
    ).all()

    urls = set()
    for link in subregion_links + island_links:
      href = await link.get_attribute("href")
      if href:
        urls.add(urljoin(BASE_URL, href))

    print(f"Found {len(urls)} place list URLs.")
    return list(urls)

  @tenacity.retry(
      stop=tenacity.stop_after_attempt(3),
      wait=tenacity.wait_fixed(2),
      reraise=True,
  )
  async def get_place_urls_from_list_page(
      self, page: Page, list_url: str
  ) -> List[str]:
    place_urls_on_page = []
    print(f"Scraping place list: {list_url}")
    await self.navigate(page, list_url)

    max_page = 1
    pagination_links = await page.locator(
        "ul.pagination a.page-link[href*='page=']"
    ).all()
    for link in pagination_links:
      href = await link.get_attribute("href")
      if href:
        match = re.search(r"page=(\d+)", href)
        if match:
          max_page = max(max_page, int(match.group(1)))

    print(f"Found {max_page} pages for {list_url}")

    for i in range(1, max_page + 1):
      if i > 1:
        page_url = f"{list_url}?page={i}"
        print(f"Navigating to page {i}: {page_url}")
        try:
          await self.navigate(page, page_url)
        except Exception as e:
          print(f"Could not navigate to page {i} of {list_url}: {e}")
          continue
      elif i == 1:
        await self.navigate(page, list_url)

      links = await page.locator(
          'article.card a.stretched-link[href*="/place/"]'
      ).all()
      for link in links:
        href = await link.get_attribute("href")
        if href:
          place_urls_on_page.append(urljoin(BASE_URL, href))
      await asyncio.sleep(random.uniform(0.5, 1.5))

    return list(set(place_urls_on_page))

  @tenacity.retry(
      stop=tenacity.stop_after_attempt(3),
      wait=tenacity.wait_fixed(5),
      reraise=True,
  )
  async def extract_place_data(
      self, page: Page, place_url: str
  ) -> Optional[Dict]:
    print(f"Extracting data from: {place_url}")
    try:
      await self.navigate(page, place_url)
    except Exception as e:
      print(f"Failed to navigate to {place_url}: {e}")
      return None
    await asyncio.sleep(random.uniform(2, 5))

    try:
      p4n_id_match = re.search(r"/place/(\d+)", place_url)
      p4n_id = p4n_id_match.group(1) if p4n_id_match else None

      if not p4n_id:
        print(f"Could not get p4n_id from URL: {place_url}")
        return None

      if p4n_id in self.visited_place_ids:
        return None
      self.visited_place_ids.add(p4n_id)

      title = await page.locator("h1").inner_text(timeout=10000)

      try:
        category = await page.locator(
            "h2.subtitle a:nth-of-type(3)"
        ).inner_text(timeout=5000)
      except Exception:
        category = None

      coords_text = await coords_link.inner_text(timeout=5000)
      coords_match = re.search(
          r"GPS\s*:\s*(-?[0-9.]+),\s*(-?[0-9.]+)", coords_text
      )
      lat, lon = None, None
      if coords_match:
        lat, lon = float(coords_match.group(1)), float(coords_match.group(2))
      else:  # Fallback for different separators or formats if any
        coords_match_fallback = re.search(
            r"(-?[0-9.]+),\s*(-?[0-9.]+)", coords_text
        )
        if coords_match_fallback:
          lat, lon = float(coords_match_fallback.group(1)), float(
              coords_match_fallback.group(2)
          )

      rating_val = None
      try:
        rating_badge = await page.locator("div.badge-rating").first
        rating_text = await rating_badge.inner_text(timeout=1000)
        rating_match = re.search(r"([0-9.]+)/5", rating_text)
        if rating_match:
          rating_val = float(rating_match.group(1))
      except Exception:
        rating_val = None

      data = {
          "p4n_id": p4n_id,
          "title": title.strip(),
          "category": category.strip() if category else None,
          "latitude": lat,
          "longitude": lon,
          "rating": rating_val,
      }
      return data
    except Exception as e:
      print(f"Error extracting data from {place_url}: {e}")
      return None

  async def run(self):
    async with async_playwright() as p:
      browser = await p.chromium.launch(headless=self.headless)
      context = await browser.new_context(**p.devices["iPhone 13"])
      page = await context.new_page()
      await stealth_async(page)

      try:
        place_list_urls = await self.get_place_list_urls(page)
      except Exception as e:
        print(f"Failed to get place list URLs: {e}")
        place_list_urls = []

      all_place_urls = set()
      for list_url in place_list_urls:
        try:
          urls = await self.get_place_urls_from_list_page(page, list_url)
          for url in urls:
            all_place_urls.add(url)
        except Exception as e:
          print(f"Could not process place list {list_url}: {e}")

      print(f"Found {len(all_place_urls)} unique place URLs to scrape.")

      place_urls_list = sorted(
          list(all_place_urls)
      )  # for deterministic order for limit
      for place_url in place_urls_list:
        if self.limit is not None and len(self.places_data) >= self.limit:
          print(f"Reached limit of {self.limit} places.")
          break
        try:
          data = await self.extract_place_data(page, place_url)
          if data:
            self.places_data.append(data)
            print(f"Scraped {data['p4n_id']}: {data['title']}")
        except Exception as e:
          print(f"Failed to scrape {place_url}: {e}")

      await browser.close()


def main():
  parser = argparse.ArgumentParser(
      description="Park4Night scraper for Portugal."
  )
  parser.add_argument(
      "--limit", type=int, help="Limit number of places to scrape"
  )
  parser.add_argument(
      "--csv_output",
      type=str,
      default="backbone_locations.csv",
      help="Output CSV file path",
  )
  parser.add_argument(
      "--no-headless",
      action="store_false",
      dest="headless",
      help="Run browser in non-headless mode",
  )
  args = parser.parse_args()

  crawler = Park4NightCrawler(limit=args.limit, headless=args.headless)
  asyncio.run(crawler.run())

  if crawler.places_data:
    df = pd.DataFrame(crawler.places_data)
    df["coordinates"] = df.apply(
        lambda row: f"({row['latitude']}, {row['longitude']})"
        if pd.notna(row["latitude"]) and pd.notna(row["longitude"])
        else None,
        axis=1,
    )
    df_out = pd.DataFrame()
    df_out["p4n_id"] = df["p4n_id"]
    df_out["title"] = df["title"]
    df_out["category"] = df["category"]
    df_out["coordinates"] = df["coordinates"]
    df_out["rating"] = df["rating"]

    df_out.to_csv(args.csv_output, index=False)
    print(f"Saved {len(crawler.places_data)} places to {args.csv_output}")
  else:
    print("No data scraped.")


if __name__ == "__main__":
  main()


# import os
# from firecrawl import FirecrawlApp

# def main():
#     # 1. Initialize the App
#     # Replace 'fc-YOUR_API_KEY' with your actual key or set it in your env variables
#     api_key = os.getenv("FIRECRAWL_API_KEY", "fc-YOUR_API_KEY")
#     app = FirecrawlApp(api_key=api_key)

#     # 2. Define the target URL
#     target_url = "https://www.idealista.pt/imovel/33454228/" # Example listing

#     print(f"--- Starting scrape for: {target_url} ---")

#     try:
#         # 3. Perform the scrape (Fixed Syntax)
#         scrape_result = app.scrape_url(
#             target_url, 
#             params={
#                 "formats": ["json"],
#                 "jsonOptions": {
#                     "schema": {
#                         "type": "object",
#                         "properties": {
#                             "title": {"type": "string"},
#                             "price": {"type": "string"},
#                             "location": {"type": "string"},
#                             "description": {"type": "string"},
#                             "features": {"type": "array", "items": {"type": "string"}},
#                             "energy_certificate": {"type": "string"}
#                         },
#                         "required": ["price", "location"]
#                     }
#                 },
#                 "waitFor": 3000  # Gives Idealista time to load/bypass initial check
#             }
#         )

#         # 4. Display the results
#         if scrape_result:
#             print("Successfully scraped data:")
#             print(scrape_result)
#         else:
#             print("No data returned.")

#     except Exception as e:
#         print(f"An error occurred: {e}")

# if __name__ == "__main__":
#     main()
