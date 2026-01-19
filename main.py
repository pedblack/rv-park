import os
from firecrawl import Firecrawl

def run_land_engine():
    # 1. Initialize with the new class name
    app = Firecrawl(api_key=os.getenv("FIRECRAWL_API_KEY"))

    search_url = "https://www.olx.pt/imoveis/terrenos-quintas/q-terreno/?search%5Bfilter_float_price%3Ato%5D=80000"
    
    print(f"🔎 Searching for new deals at: {search_url}")
    
    try:
        # 2. Use .map() instead of .map_url()
        map_result = app.map(search_url)
        
        # The new SDK returns an object or dict; we check for 'links'
        all_links = map_result.get('links', []) if isinstance(map_result, dict) else getattr(map_result, 'links', [])
        
        listing_links = [l for l in all_links if "/d/anuncio/" in l]
        print(f"Found {len(listing_links)} potential links. Analyzing the top 3...")

        for url in listing_links[:3]:
            print(f"\n✨ Analyzing: {url}")
            
            # 3. Use .scrape() instead of .scrape_url()
            scrape_result = app.scrape(url, params={
                "formats": ["json"],
                "jsonOptions": {
                    "schema": {
                        "type": "object",
                        "properties": {
                            "price": {"type": "integer"},
                            "area_sqm": {"type": "integer"},
                            "location": {"type": "string"},
                            "has_water": {"type": "boolean"},
                            "is_private": {"type": "boolean"}
                        },
                        "required": ["price", "area_sqm"]
                    }
                }
            })
            
            # Extract data from the result
            data = scrape_result.get('json', {}) if isinstance(scrape_result, dict) else getattr(scrape_result, 'json', {})
            price = data.get('price', 0)
            area = data.get('area_sqm', 0)

            if price > 0 and area >= 5000 and price < 60000:
                print(f"🔥 DEAL FOUND! {price}€ for {area}m2 in {data.get('location')}")
            else:
                print(f"   (Skipping: Price {price}€ or Area {area}m2 does not meet criteria)")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    run_land_engine()
