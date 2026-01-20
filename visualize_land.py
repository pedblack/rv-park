import pandas as pd
import folium
from folium.plugins import MarkerCluster
import os
import json

CSV_FILE = os.environ.get("CSV_FILE", "backbone_locations.csv")

def generate_map():
    if not os.path.exists(CSV_FILE):
        print(f"❌ {CSV_FILE} not found.")
        return

    df = pd.read_csv(CSV_FILE)
    df_clean = df[(df['latitude'] != 0) & (df['longitude'] != 0)].dropna(subset=['latitude', 'longitude'])

    if df_clean.empty:
        print("⚠️ No valid data found.")
        return

    # Center on Alentejo
    m = folium.Map(location=[38.5, -7.9], zoom_start=9, tiles="cartodbpositron")
    
    marker_cluster = MarkerCluster(name="Properties", control=False).add_to(m)

    prop_types = sorted(df_clean['location_type'].unique().tolist())

    for _, row in df_clean.iterrows():
        def format_list(text):
            if pd.isna(text) or text == "N/A" or not str(text).strip(): return "<li>None</li>"
            return "".join([f"<li>{item.strip()}</li>" for item in str(text).split(";")])

        popup_html = f"""
        <div style="font-family: Arial, sans-serif; width: 300px; line-height: 1.6; font-size: 14px; color: #333;">
            <h3 style="margin: 0 0 10px 0; font-size: 18px; color: #2c3e50; border-bottom: 3px solid #27d9a1;">{row['title']}</h3>
            <div style="margin-bottom: 12px; font-weight: bold; font-size: 15px; color: #7f8c8d;">
                📍 {row['location_type']} | 🚗 {row['num_places']} spots
            </div>
            <div style="background: #f1f3f5; padding: 10px; border-radius: 8px; margin-bottom: 12px; border: 1px solid #dee2e6;">
                <table style="width: 100%; font-size: 15px;">
                    <tr><td>💰 <b>Min:</b></td><td style="text-align: right;">{row['parking_min_eur']}€</td></tr>
                    <tr><td>⚡ <b>Elec:</b></td><td style="text-align: right;">{row['electricity_eur']}€</td></tr>
                </table>
            </div>
            <div style="font-size: 14px;">
                <b style="color: #27ae60;">Pros:</b> <ul style="margin:5px 0; padding-left:20px;">{format_list(row['ai_pros'])}</ul>
                <b style="color: #e74c3c;">Cons:</b> <ul style="margin:5px 0; padding-left:20px;">{format_list(row['ai_cons'])}</ul>
            </div>
            <div style="padding-top: 10px; border-top: 1px solid #eee; font-size: 13px;">
                ⭐ <b>{row['avg_rating']}</b> / 5 ({row['total_reviews']} reviews)
            </div>
            <a href="{row['url']}" target="_blank" style="display: block; margin-top: 15px; text-align: center; background: #27d9a1; color: white; padding: 12px; border-radius: 6px; text-decoration: none; font-weight: bold;">View on Park4Night</a>
        </div>
        """

        # Icon logic
        icon_color = 'green' if row['avg_rating'] >= 4 else 'orange' if row['avg_rating'] >= 3 else 'red'

        marker = folium.Marker(
            location=[row['latitude'], row['longitude']],
            popup=folium.Popup(popup_html, max_width=350),
            icon=folium.Icon(color=icon_color, icon='home', prefix='fa'),
            tooltip=f"{row['title']} ({row['avg_rating']}⭐)"
        )
        
        # Data binding for JS
        marker.options['data_rating'] = float(row['avg_rating'])
        marker.options['data_reviews'] = int(row['total_reviews'])
        marker.options['data_places'] = int(row.get('num_places', 0))
        marker.options['data_type'] = str(row['location_type'])
        marker.add_to(marker_cluster)

    # --- HTML UI ELEMENTS (Legend & Filter Panel) ---
    filter_html = f"""
    <style>
        .map-overlay {{ font-family: sans-serif; background: white; border-radius: 12px; box-shadow: 0 4px 20px rgba(0,0,0,0.15); border: 1px solid #eee; }}
        #filter-panel {{ position: fixed; top: 20px; right: 20px; z-index: 9999; padding: 20px; width: 220px; }}
        #legend-panel {{ position: fixed; bottom: 30px; left: 20px; z-index: 9999; padding: 15px; width: 160px; font-size: 13px; }}
        .legend-item {{ display: flex; align-items: center; margin-bottom: 5px; }}
        .dot {{ height: 12px; width: 12px; border-radius: 50%; display: inline-block; margin-right: 8px; }}
    </style>

    <div id="legend-panel" class="map-overlay">
        <h4 style="margin: 0 0 10px 0; font-size: 14px;">Rating Legend</h4>
        <div class="legend-item"><span class="dot" style="background: #28a745;"></span> Excellent (4+)</div>
        <div class="legend-item"><span class="dot" style="background: #fd7e14;"></span> Good (3-4)</div>
        <div class="legend-item"><span class="dot" style="background: #dc3545;"></span> Poor (<3)</div>
    </div>

    <div id="filter-panel" class="map-overlay">
        <h3 style="margin-top:0; font-size:18px; color: #2c3e50;">Filter Map</h3>
        
        <div style="margin-bottom:15px;">
            <label style="font-size:13px; font-weight:bold;">Min Rating: <span id="val-rating" style="color:#27d9a1">0</span></label>
            <input type="range" id="filter-rating" min="0" max="5" step="0.1" value="0" oninput="document.getElementById('val-rating').innerText = this.value" style="width:100%;">
        </div>
        
        <div style="margin-bottom:15px;">
            <label style="font-size:13px; font-weight:bold;">Min Reviews: <span id="val-reviews" style="color:#27d9a1">0</span></label>
            <input type="range" id="filter-reviews" min="0" max="500" step="5" value="0" oninput="document.getElementById('val-reviews').innerText = this.value" style="width:100%;">
        </div>

        <div style="margin-bottom:15px;">
            <label style="font-size:13px; font-weight:bold;">Min Places: <span id="val-places" style="color:#27d9a1">0</span></label>
            <input type="range" id="filter-places" min="0" max="200" step="10" value="0" oninput="document.getElementById('val-places').innerText = this.value" style="width:100%;">
        </div>
        
        <div style="margin-bottom:20px;">
            <label style="font-size:13px; font-weight:bold;">Property Type:</label>
            <select id="filter-type" style="width:100%; padding:5px; border-radius:4px; border:1px solid #ccc;">
                <option value="All">All Types</option>
                {" ".join([f'<option value="{t}">{t}</option>' for t in prop_types])}
            </select>
        </div>
        
        <button onclick="applyFilters()" style="width:100%; background:#2c3e50; border:none; 
                color:white; padding:12px; border-radius:6px; cursor:pointer; font-weight:bold;">Apply Filters</button>
    </div>

    <script>
    var allMarkersBackup = null;

    function applyFilters() {{
        const minRate = parseFloat(document.getElementById('filter-rating').value);
        const minRev = parseInt(document.getElementById('filter-reviews').value);
        const minPlc = parseInt(document.getElementById('filter-places').value);
        const type = document.getElementById('filter-type').value;

        var mapObj = null;
        var clusterGroup = null;

        for (let key in window) {{
            if (window[key] instanceof L.Map) mapObj = window[key];
            if (window[key] instanceof L.MarkerClusterGroup) clusterGroup = window[key];
        }}

        if (!mapObj || !clusterGroup) return;

        if (!allMarkersBackup) {{
            allMarkersBackup = clusterGroup.getLayers();
        }}

        clusterGroup.clearLayers();

        const filtered = allMarkersBackup.filter(m => {{
            const r = m.options.data_rating || 0;
            const rev = m.options.data_reviews || 0;
            const plc = m.options.data_places || 0;
            const t = m.options.data_type || "";
            return r >= minRate && rev >= minRev && plc >= minPlc && (type === "All" || t === type);
        }});

        clusterGroup.addLayers(filtered);
    }}
    </script>
    """
    m.get_root().html.add_child(folium.Element(filter_html))

    m.save("portugal_land_map.html")
    print("🚀 Map generated with Places Filter and Legend.")

if __name__ == "__main__":
    generate_map()
