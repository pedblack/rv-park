import pandas as pd
import folium
from folium.plugins import MarkerCluster
import os
import json
import numpy as np

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
        def clean_int(val):
            try:
                if pd.isna(val) or val == "": return 0
                return int(float(val))
            except: return 0

        def format_list(text):
            if pd.isna(text) or text == "N/A" or not str(text).strip(): return "<li>None</li>"
            return "".join([f"<li>{item.strip()}</li>" for item in str(text).split(";")])

        num_places = clean_int(row.get('num_places', 0))
        intensity = float(row.get('intensity_index', 0)) if not pd.isna(row.get('intensity_index')) else 0
        bar_color = "#28a745" if intensity < 4 else "#fd7e14" if intensity < 8 else "#dc3545"

        popup_html = f"""
        <div style="font-family: Arial, sans-serif; width: 320px; line-height: 1.6; font-size: 14px; color: #333;">
            <h3 style="margin: 0 0 10px 0; font-size: 18px; color: #2c3e50; border-bottom: 3px solid #27d9a1;">{row['title']}</h3>
            <div style="margin-bottom: 12px; font-weight: bold; font-size: 15px; color: #7f8c8d;">📍 {row['location_type']} | 🚗 {num_places} spots</div>
            <div style="margin-bottom: 15px;">
                <div style="display:flex; justify-content: space-between; font-size: 12px; margin-bottom: 3px;"><b>Occupancy Intensity:</b> <span>{intensity}/10</span></div>
                <div style="width: 100%; background: #eee; border-radius: 10px; height: 8px;"><div style="width: {intensity*10}%; background: {bar_color}; height: 8px; border-radius: 10px;"></div></div>
            </div>
            <div style="background: #f1f3f5; padding: 10px; border-radius: 8px; margin-bottom: 12px; border: 1px solid #dee2e6;">
                <table style="width: 100%; font-size: 14px;">
                    <tr><td>💰 <b>Min:</b></td><td style="text-align: right;">{row.get('parking_min_eur', 0)}€</td></tr>
                    <tr><td>⚡ <b>Elec:</b></td><td style="text-align: right;">{row.get('electricity_eur', 0)}€</td></tr>
                    <tr><td>🕒 <b>Arrival:</b></td><td style="text-align: right; color: #e67e22;"><b>{row.get('arrival_window', 'anytime')}</b></td></tr>
                </table>
            </div>
            <div style="font-size: 13px;">
                <b style="color: #27ae60;">Pros:</b> <ul style="margin:2px 0; padding-left:18px;">{format_list(row.get('ai_pros'))}</ul>
                <b style="color: #e74c3c;">Cons:</b> <ul style="margin:2px 0; padding-left:18px;">{format_list(row.get('ai_cons'))}</ul>
            </div>
            <a href="{row['url']}" target="_blank" style="display: block; text-align: center; background: #27d9a1; color: white; padding: 10px; border-radius: 6px; text-decoration: none; font-weight: bold; margin-top:10px;">View on Park4Night</a>
        </div>
        """

        icon_color = 'green' if row['avg_rating'] >= 4 else 'orange' if row['avg_rating'] >= 3 else 'red'

        marker = folium.Marker(
            location=[row['latitude'], row['longitude']],
            popup=folium.Popup(popup_html, max_width=350),
            icon=folium.Icon(color=icon_color, icon='home', prefix='fa'),
            tooltip=f"{row['title']} ({row['avg_rating']}⭐)"
        )
        
        # KEY DATA BINDING FOR JAVASCRIPT
        marker.options['data_rating'] = float(row['avg_rating'])
        marker.options['data_places'] = num_places
        marker.options['data_type'] = str(row['location_type'])
        marker.add_to(marker_cluster)

    filter_html = f"""
    <style>
        .map-overlay {{ font-family: sans-serif; background: white; border-radius: 12px; box-shadow: 0 4px 20px rgba(0,0,0,0.15); border: 1px solid #eee; }}
        #filter-panel {{ position: fixed; top: 20px; right: 20px; z-index: 9999; padding: 20px; width: 220px; }}
        input[type=range] {{ width: 100%; }}
    </style>

    <div id="filter-panel" class="map-overlay">
        <h3 style="margin-top:0; font-size:18px; color: #2c3e50;">Filter Map</h3>
        
        <div style="margin-bottom:15px;">
            <label style="font-size:12px; font-weight:bold;">Min Rating: <span id="val-rating" style="color:#27d9a1">0</span></label>
            <input type="range" id="filter-rating" min="0" max="5" step="0.1" value="0" oninput="document.getElementById('val-rating').innerText = this.value">
        </div>
        
        <div style="margin-bottom:15px;">
            <label style="font-size:12px; font-weight:bold;">Min Places: <span id="val-places" style="color:#27d9a1">0</span></label>
            <input type="range" id="filter-places" min="0" max="100" step="5" value="0" oninput="document.getElementById('val-places').innerText = this.value">
        </div>
        
        <div style="margin-bottom:20px;">
            <label style="font-size:12px; font-weight:bold;">Type:</label>
            <select id="filter-type" style="width:100%; padding:5px; border:1px solid #ccc;">
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
        const minPlc = parseInt(document.getElementById('filter-places').value);
        const type = document.getElementById('filter-type').value;

        var clusterGroup = null;
        // Correctly find the MarkerClusterGroup in the global Leaflet context
        for (let key in window) {{
            if (window[key] instanceof L.MarkerClusterGroup) {{
                clusterGroup = window[key];
                break;
            }}
        }}

        if (!clusterGroup) {{
            console.error("MarkerClusterGroup not found");
            return;
        }}

        // Capture all markers on the first click to prevent data loss
        if (!allMarkersBackup) {{
            allMarkersBackup = clusterGroup.getLayers();
        }}

        // Clear the visible cluster group
        clusterGroup.clearLayers();

        // Filter from the persistent backup
        const filtered = allMarkersBackup.filter(m => {{
            const r = m.options.data_rating || 0;
            const plc = m.options.data_places || 0;
            const t = m.options.data_type || "";
            return r >= minRate && plc >= minPlc && (type === "All" || t === type);
        }});

        // Re-add filtered layers to the cluster group
        clusterGroup.addLayers(filtered);
    }}
    </script>
    """
    m.get_root().html.add_child(folium.Element(filter_html))
    m.save("index.html")
    print("🚀 Map successfully generated: index.html with fixed JS filtering.")

if __name__ == "__main__":
    generate_map()
