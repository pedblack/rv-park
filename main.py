import os
import json
from firecrawl import FirecrawlApp

def run_land_scraper():
    # 1. Initialize Firecrawl 
    # It pulls the key from the GitHub Secrets we set up
    api_key = os.getenv("FIRECRAWL_API_KEY")
    if not api_key:
        print("❌ Error: FIRECRAWL_API_KEY not found in environment variables.")
        return

    app = FirecrawlApp(api_key=api_key)

    # 2. Targeted Land Listing (Test URL)
    # You can change this URL to any land listing from OLX or Imovirtual
    test_url = "https://www.olx.pt/d/anuncio/terreno-rustico-com-7-250m2-em-messines-IDHvX6p.html"
    
    print(f"🕵️ Analyzing Listing: {test_url}")

    # 3. AI Extraction Logic
    # We define the 'schema' so the AI knows exactly what to look for
    try:
        scrape_result = app.scrape_url(test_url, params={
            "formats": ["json"],
            "jsonOptions": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "price": {"type": "integer", "description": "The numeric price in Euro"},
                        "area_sqm": {"type": "integer", "description": "The land size in square meters"},
                        "has_water": {"type": "boolean", "description": "True if mentions a well (poço), borehole (furo), or water connection"},
                        "location": {"type": "string", "description": "The municipality or village name"},
                        "is_private_seller": {"type": "boolean", "description": "True if the seller is a private individual, not an agency"}
                    },
                    "required": ["price", "area_sqm"]
                }
            }
        })

        # 4. Process and Print Results
        data = scrape_result.get('json', {})
        
        print("\n--- ✅ EXTRACTION SUCCESSFUL ---")
        print(f"💰 PRICE:    {data.get('price', 'N/A')}€")
        print(f"📏 AREA:     {data.get('area_sqm', 'N/A')} m²")
        print(f"📍 LOCATION: {data.get('location', 'Unknown')}")
        print(f"💧 WATER:    {'Detected' if data.get('has_water') else 'None mentioned'}")
        print(f"👤 PRIVATE:  {'Yes' if data.get('is_private_seller') else 'No (Agency)'}")
        print("--------------------------------\n")

    except Exception as e:
        print(f"❌ An error occurred during scraping: {e}")

if __name__ == "__
