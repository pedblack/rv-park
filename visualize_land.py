import pandas as pd
import folium
from folium.plugins import MarkerCluster
import os

# Select source based on environment
CSV_FILE = os.environ.get("CSV_FILE", "backbone_locations.csv")

def generate_map():
    if not os.path.exists(CSV_FILE):
        print(f"❌ {CSV_FILE} not found. Skip map generation.")
        return

    print(f"📊 Loading data from: {CSV_FILE}")
    df = pd.read_csv(CSV_FILE)
    
    # Filter rows that have valid coordinates
    df_clean = df[(df['latitude'] != 0) & (df['longitude'] != 0)].dropna(subset=['latitude', 'longitude'])

    if df_clean.empty:
        print("⚠️ No valid coordinates found. Generating default map center.")
        m = folium.Map(location=[39.5, -8.0], zoom_start=6)
        m.save("portugal_land_map.html")
        return

    # Center on Portugal
    m = folium.Map(location=[39.5, -8.0], zoom_start=7, tiles="cartodbpositron")
    marker_cluster = MarkerCluster().add_to(m)

    for _, row in df_clean.iterrows():
        # Helper to clean up semicolon-separated lists into HTML bullets
        def format_list(text):
            if pd.isna(text) or text == "N/A" or not str(text).strip(): return "<li>None</li>"
            items = str(text).split(";")
            return "".join([f"<li>{item.strip()}</li>" for item in items])

        # Enhanced modern popup without the scrape date
        popup_html = f"""
        <div style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; width: 260px; line-height: 1.4; color: #2c3e50;">
            <h4 style="margin: 0 0 5px 0; color: #1a1a1a; border-bottom: 2px solid #27d9a1; padding-bottom: 3px;">
                {row['title']}
            </h4>
            
            <div style="font-size: 0.85em; color: #7f8c8d; margin-bottom: 10px;">
                <b>Type:</b> {row.get('location_type', 'Unknown')} | <b>Places:</b> {row.get('num_places', 0)}
            </div>

            <div style="background: #f8f9fa; padding: 8px; border-radius: 6px; margin-bottom: 10px; border: 1px solid #eee;">
                <table style="width: 100%; font-size: 0.9em;">
                    <tr><td>💰 <b>Min:</b></td><td style="text-align: right;">{row['parking_min_eur']}€</td></tr>
                    <tr><td>💸 <b>Max:</b></td><td style="text-align: right;">{row['parking_max_eur']}€</td></tr>
                    <tr><td>⚡ <b>Elec:</b></td><td style="text-align: right;">{row['electricity_eur']}€</td></tr>
                </table>
            </div>

            <div style="font-size: 0.85em; margin-bottom: 8px;">
                <b style="color: #27ae60;">Top Pros:</b>
                <ul style="margin: 3px 0; padding-left: 18px;">{format_list(row['ai_pros'])}</ul>
            </div>

            <div style="font-size: 0.85em; margin-bottom: 8px;">
                <b style="color: #e74c3c;">Top Cons:</b>
                <ul style="margin: 3px 0; padding-left: 18px;">{format_list(row['ai_cons'])}</ul>
            </div>

            <div style="font-size: 0.8em; color: #34495e; border-top: 1px solid #eee; padding-top: 8px;">
                🌍 <b>Languages:</b> {row.get('top_languages', 'N/A')}<br>
                ⭐ {row['avg_rating']}/5 ({row['total_reviews']} reviews)
            </div>

            <a href="{row['url']}" target="_blank" style="display: block; margin-top: 10px; text-align: center; background: #27d9a1; color: white; padding: 8px; border-radius: 4px; text-decoration: none; font-weight: bold; font-size: 0.9em;">
                Open in Park4Night
            </a>
        </div>
        """
        
        # Color coding based on Min Price
        p_min = row['parking_min_eur']
        icon_color = 'green' if p_min < 15 else 'orange' if p_min <= 25 else 'red'

        folium.Marker(
            location=[row['latitude'], row['longitude']],
            popup=folium.Popup(popup_html, max_width=300),
            icon=folium.Icon(color=icon_color, icon='home', prefix='fa'),
            tooltip=f"{row['title']} ({row['location_type']})"
        ).add_to(marker_cluster)

    m.save("portugal_land_map.html")
    print(f"🚀 Map successfully generated: portugal_land_map.html")

if __name__ == "__main__":
    generate_map()
