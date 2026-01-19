import os
import csv
from firecrawl import Firecrawl

def run_land_engine():
    # 1. Initialize the 2026 SDK
    app = Firecrawl(api_key=os.getenv("FIRECRAWL_API_KEY"))
    csv_file = "land_deals.csv"
    headers = ["url", "price", "area_sqm", "location", "has_water"]

    if not os.path.exists(csv_file):
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()

    # 2. Go directly to the SOURCE (OLX Alentejo Land Listings)
    # This bypasses stale search engine indexes
    list_url = "https://www.olx.pt/imoveis/terrenos-quintas/alentejo/"
    
    print(f"🔎 DEBUG: Accessing live listings at: {list_url}")
    
    try:
        # We scrape the list page specifically to get 'links'
        list_page = app.scrape(list_url, formats=["links"])
        
        # Access links found on the page
        all_links = list_page.get('links', []) if isinstance(list_page, dict) else getattr(list_page, 'links', [])
        
        # Filter for actual listing links
        listing_links = [l for l in all_links if "/d/anuncio/" in l]
        
        print(f"🔎 DEBUG: Found {len(listing_links)} live property links.")

        if not listing_links:
            print("❌ No links found on the page. OLX might be using high-level bot protection.")
            return

        # --- MAX 1 POC ---
        target_url = listing_links[0]
        print(f"✨ Target found: {target_url}. Scraping details...")

        # 3. Scrape the single target with our AI Schema
        scrape_result = app.scrape(target_url, formats=["json"], jsonOptions={
            "schema": {
                "type": "object",
                "properties": {
                    "price": {"type": "integer"},
                    "area_sqm": {"type": "integer"},
                    "location": {"type": "string"},
                    "has_water": {"type": "boolean"}
                },
                "required": ["price"]
            }
        })
        
        data = scrape_result.get('json', {}) if isinstance(scrape_result, dict) else getattr(scrape_result, 'json', {})
        
        if data:
            row = {
                "url": target_url,
                "price": data.get("price"),
                "area_sqm": data.get("area_sqm"),
                "location": data.get("location"),
                "has_water": data.get("has_water")
            }

            # 4. Save to CSV
            with open(csv_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writerow(row)
            
            print(f"✅ SUCCESS! Saved {row['price']}€ listing in {row['location']} to CSV.")
        else:
            print("⚠️ Scrape succeeded but AI returned no data.")

    except Exception as e:
        print(f"❌ SDK Error: {e}")

if __name__ == "__main__":
