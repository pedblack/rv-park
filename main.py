import os
from firecrawl import Firecrawl

def run_land_engine():
    # 1. Initialize the latest SDK
    app = Firecrawl(api_key=os.getenv("FIRECRAWL_API_KEY"))

    # 2. Search query 
    # Searching is often more successful on marketplaces like OLX
    query = "site:olx.pt terreno rustico alentejo preco 50000..80000"
    
    print(f"🔎 Querying Firecrawl for: {query}")
    
    try:
        # We ask for 3 links but we will only ever use the first one
        search_result = app.search(query, params={"limit": 3})
        
        # Search results are in the 'data' list
        listings = search_result.get('data', [])
        
        if listings:
            # --- THE MAX 1 STRATEGY ---
            # We take the absolute first result to save credits
            target_item = listings[0]
            url = target_item.get('url')
            
            print(f"✨ Proof of Concept: Analyzing ONLY the #1 result: {url}")
            
            # Scrape and extract the structured data
            scrape_result = app.scrape(url, params={
                "formats": ["json"],
                "jsonOptions": {
                    "schema": {
                        "type": "object",
                        "properties": {
                            "price": {"type": "integer"},
                            "area_sqm": {"type": "integer"},
                            "location": {"type": "string"}
                        },
                        "required": ["price"]
                    }
                }
            })
            
            # Extract data from the result
            data = scrape_result.get('json', {})
            price = data.get('price', 0)
            area = data.get('area_sqm', 0)

            print("\n--- 🎯 POC ANALYSIS RESULT ---")
            if price > 0:
                print(f"💰 Found: {price}€ | 📏 Area: {area}m2 | 📍 Loc: {data.get('location')}")
                if price < 70000 and area > 5000:
                    print("🔥 STATUS: Potential FIRE Deal detected!")
                else:
                    print("   STATUS: Listing found but does not meet FIRE criteria.")
            else:
                print("⚠️ Scrape succeeded but returned no numeric data.")
            print("------------------------------")
            
        else:
            print("❌ No links found. Try changing the query in the code.")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    run_land_engine()
