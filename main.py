import os
import csv
from firecrawl import Firecrawl

def run_land_engine():
    # 1. Initialize the latest SDK
    # Note: FirecrawlApp was renamed to Firecrawl in the newest versions
    app = Firecrawl(api_key=os.getenv("FIRECRAWL_API_KEY"))
    csv_file = "land_deals.csv"
    headers = ["url", "price", "area_sqm", "location", "has_water"]

    # Initialize CSV with headers if it doesn't exist
    if not os.path.exists(csv_file):
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()

    # 2. Search Strategy
    # We use a natural language query which is more resilient for marketplaces
    search_query = "terrenos rusticos baratos Alentejo Portugal OLX"
    print(f"🔎 Searching for: {search_query}")
    
    try:
        # FIXED: Removed 'params' dictionary. limit is now a direct argument.
        search_result = app.search(search_query, limit=3)
        
        # Access results safely (handles both dict and object returns)
        listings = search_result.get('data', []) if isinstance(search_result, dict) else getattr(search_result, 'data', [])
        
        if listings:
            # --- PROOF OF CONCEPT: MAX 1 ---
            # We pick the first valid-looking listing to save our 500 credits
            target_url = None
            for item in listings:
                candidate_url = item.get('url', '')
                if "olx.pt" in candidate_url and "/d/anuncio/" in candidate_url:
                    target_url = candidate_url
                    break
            
            if not target_url:
                target_url = listings[0].get('url') # Fallback to first result

            print(f"✨ Target found: {target_url}")

            # FIXED: Scrape call updated for the latest SDK
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
            
            # Extract data
            data = scrape_result.get('json', {}) if isinstance(scrape_result, dict) else getattr(scrape_result, 'json', {})
            
            row = {
                "url": target_url,
                "price": data.get("price"),
                "area_sqm": data.get("area_sqm"),
                "location": data.get("location"),
                "has_water": data.get("has_water")
            }

            # 3. Save to CSV
            with open(csv_file, 'a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writerow(row)
            
            print(f"✅ Success! Saved {row['price']}€ listing in {row['location']} to CSV.")
            
        else:
            print("❌ Search returned no results. Try a broader query.")

    except Exception as e:
        print(f"❌ SDK Error: {e}")

if __name__ == "__main__":
    run_land_engine()
