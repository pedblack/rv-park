import os
from firecrawl import FirecrawlApp

def run_land_engine():
    # Initialize Firecrawl
    app = FirecrawlApp(api_key=os.getenv("FIRECRAWL_API_KEY"))

    # 1. SEARCH: Find land listings in Portugal (filtered by price in the URL)
    # This URL targets land under 80k on OLX
    search_url = "https://www.olx.pt/imoveis/terrenos-quintas/q-terreno/?search%5Bfilter_float_price%3Ato%5D=80000"
    
    print(f"🔎 Searching for new deals at: {search_url}")
    
    # map_url finds listing links
    map_result = app.map_url(search_url)
    all_links = map_result.get('links', [])
    listing_links = [l for l in all_links if "/d/anuncio/" in l]
    
    print(f"Found {len(listing_links)} potential links. Analyzing the top 3...")

    # 2. PROCESS: Analyze the first 3 links
    for url in listing_links[:3]:
        print(f"\n✨ Analyzing: {url}")
        
        try:
            scrape_result = app.scrape_url(url, params={
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
            
            data = scrape_result.get('json', {})
            price = data.get('price', 0)
            area = data.get('area_sqm', 0)

            # 3. THE FIRE FILTER: Check if it's actually a good deal
            # Criteria: Under 60k Euro AND larger than 5000 m2
            if price < 60000 and area >= 5000:
                print(f"🔥 DEAL FOUND! {price}€ for {area}m2 in {data.get('location')}")
                print(f"   Water: {data.get('has_water')} | Private: {data.get('is_private')}")
            else:
                print(f"   (Skipping: Price {price}€ or Area {area}m2 does not meet criteria)")
            
        except Exception as e:
            print(f"⚠️ Could not parse listing: {e}")

    print("\n🏁 Engine run complete.")

if __name__ == "__main__":
    run_land_engine()
