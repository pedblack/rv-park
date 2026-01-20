import pandas as pd
import folium
from folium.plugins import MarkerCluster, HeatMap
import re
import os

CSV_FILE = "backbone_locations.csv"
OUTPUT_MAP = "portugal_land_map.html"

def generate_map():
    if not os.path.exists(CSV_FILE):
        print("No data found to visualize.")
        return

    # Load data
    df = pd.read_csv(CSV_FILE)
    
    # 1. Coordinate Extraction Logic
    def get_coords(url):
        try:
            # Matches both ?lat=1.2&lng=3.4 and /place/123 (if coords were in URL)
            lat = re.search(r'lat=([-.\d]+)', url)
            lng = re.search(r'lng=([-.\d]+)', url)
            if lat and lng:
                return float(lat.group(1)), float(lng.group(1))
            return None
        except:
            return None

    df['coords'] = df['url'].apply(get_coords)
    df = df.dropna(subset=['coords'])

    if df.empty:
        print("No coordinates found in URLs. Map cannot be generated.")
        return

    # 2. Initialize Map (Centered on Portugal)
    m = folium.Map(location=[39.3999, -8.2245], zoom_start=7, tiles="cartodbpositron")

    # 3. Add HeatMap (Visual density of spots)
    heat_data = [[c[0], c[1]] for c in df['coords']]
    HeatMap(heat_data, name="Spot Density", show=False).add_to(m)

    # 4. Add Individual Markers with AI Context
    marker_cluster = MarkerCluster(name="Land Details").add_to(m)

    for _, row in df.iterrows():
        # Resilience: Use .get() or check if columns exist to prevent crashes
        p_min = row.get('parking_min_eur', 0)
        p_max = row.get('parking_max_eur', 0)
        title = row.get('title', 'Unknown Location')
        rating = row.get('rating', 'N/A')
        
        # Color Logic
        color = "green" if p_min <= 5 else "orange" if p_min < 20 else "red"
        
        # Build Popup HTML
        popup_html = f"""
        <div style='width:250px; font-family: sans-serif; line-height: 1.4;'>
            <h4 style='margin-bottom:5px;'>{title}</h4>
            <b>Rating:</b> ⭐{rating}<br>
            <b>Price:</b> {p_min}€ - {p_max}€<br>
            <hr style='margin:10px 0;'>
            <details>
                <summary style='cursor:pointer; color:#2c3e50; font-weight:bold;'>Show AI Insights</summary>
                <div style='margin-top:10px; background:#f9f9f9; padding:5px; border-radius:4px;'>
                    <b style='color:green;'>Pros:</b><br><small>{row.get('ai_pros', 'N/A')}</small><br>
                    <b style='color:red; margin-top:5px; display:inline-block;'>Cons:</b><br><small>{row.get('ai_cons', 'N/A')}</small>
                </div>
            </details>
            <br>
            <a href="{row['url']}" target="_blank" style='display:inline-block; background:#3498db; color:white; padding:5px 10px; text-decoration:none; border-radius:3px; font-size:12px;'>Open in Park4Night</a>
        </div>
        """
        
        folium.Marker(
            location=row['coords'],
            popup=folium.Popup(popup_html, max_width=300),
            icon=folium.Icon(color=color, icon="campground", prefix="fa")
        ).add_to(marker_cluster)

    folium.LayerControl().add_to(m)
    
    # Save Map
    m.save(OUTPUT_MAP)
    print(f"✅ Map successfully generated: {OUTPUT_MAP} ({len(df)} spots)")

if __name__ == "__main__":
    generate_map()
