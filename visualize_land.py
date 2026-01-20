import pandas as pd
import folium
import os
import json
import numpy as np

CSV_FILE = os.environ.get("CSV_FILE", "backbone_locations.csv")

def generate_map():
    if not os.path.exists(CSV_FILE):
        print(f"❌ {CSV_FILE} not found.")
        return

    df = pd.read_csv(CSV_FILE)
    # Filter out invalid coordinates
    df_clean = df[(df['latitude'] != 0) & (df['longitude'] != 0)].dropna(subset=['latitude', 'longitude'])

    if df_clean.empty:
        print("⚠️ No valid data found.")
        return

    # Center on Alentejo
    m = folium.Map(location=[38.5, -7.9], zoom_start=9, tiles="cartodbpositron")
    
    # FeatureGroup ensures all points are shown all the time (no bundling)
    # We assign a specific name to the options to help JS find it reliably
    marker_layer = folium.FeatureGroup(name="MainPropertyLayer", overlay=True, control=True).add_to(m)

    prop_types = sorted(df_clean['location_type'].unique().tolist())

    for _, row in df_clean.iterrows():
        def clean_int(val):
            try:
                if pd.isna(val) or val == "": return 0
                return int(float(val))
            except: return 0

        num_places = clean_int(row.get('num_places', 0))
        intensity = float(row.get('intensity_index', 0)) if not pd.isna(row.get('intensity_index')) else 0
        bar_color = "#28a745" if intensity < 4 else "#fd7e14" if intensity < 8 else "#dc3545"
        
        popup_html = f"""<div style="font-family: Arial; width: 300px;">
            <h3 style="margin:0 0 10px 0; border-bottom: 2px solid #27d9a1;">{row['title']}</h3>
            <b>Intensity:</b> {intensity}/10 <br>
            <b>Places:</b> {num_places} | <b>Rating:</b> {row['avg_rating']}⭐
            <div style="width: 100%; background: #eee; height: 8px; margin: 5px 0; border-radius: 4px;">
                <div style="width: {intensity*10}%; background: {bar_color}; height: 8px; border-radius: 4px;"></div>
            </div>
            <br><a href="{row['url']}" target="_blank">View on Park4Night</a>
        </div>"""

        marker = folium.Marker(
            location=[row['latitude'], row['longitude']],
            popup=folium.Popup(popup_html, max_width=350),
            icon=folium.Icon(color='green' if row['avg_rating'] >= 4 else 'orange', icon='home', prefix='fa')
        )
        
        # KEY: Using a standardized 'extra_data' object inside options for JS accessibility
        marker.options['extra_data'] = {{
            'rating': float(row['avg_rating']),
            'places': num_places,
            'type': str(row['location_type'])
        }}
        marker.add_to(marker_layer)

    # --- UI & JAVASCRIPT ---
    # Using doubled curly braces {{ }} to prevent Python f-string errors
    filter_html = f"""
    <style>
        .map-overlay {{ font-family: sans-serif; background: white; border-radius: 12px; padding: 20px; box-shadow: 0 4px 20px rgba(0,0,0,0.2); position: fixed; z-index: 9999; }}
        #filter-panel {{ top: 20px; right: 20px; width: 220px; }}
        .btn-group {{ display: flex; gap: 10px; margin-top: 15px; }}
        button {{ flex: 1; padding: 10px; border-radius: 6px; cursor: pointer; font-weight: bold; border: none; }}
        .btn-apply {{ background: #2c3e50; color: white; }}
        .btn-reset {{ background: #95a5a6; color: white; }}
    </style>

    <div id="filter-panel" class="map-overlay">
        <h3 style="margin:0 0 10px 0;">Filters</h3>
        <p style="font-size: 14px;">Matching: <b id="match-count">{len(df_clean)}</b> sites</p>
        
        <div style="margin-bottom:10px;">
            <label style="font-size:12px;">Min Rating: <span id="txt-rating">0</span></label>
            <input type="range" id="range-rating" min="0" max="5" step="0.1" value="0" style="width:100%" oninput="document.getElementById('txt-rating').innerText=this.value">
        </div>
        
        <div style="margin-bottom:10px;">
            <label style="font-size:12px;">Min Places: <span id="txt-places">0</span></label>
            <input type="range" id="range-places" min="0" max="100" step="5" value="0" style="width:100%" oninput="document.getElementById('txt-places').innerText=this.value">
        </div>
        
        <label style="font-size:12px;">Property Type:</label>
        <select id="sel-type" style="width:100%; margin-top:5px;">
            <option value="All">All Types</option>
            {" ".join([f'<option value="{t}">{t}</option>' for t in prop_types])}
        </select>
        
        <div class="btn-group">
            <button onclick="applyFilters()" class="btn-apply">Apply</button>
            <button onclick="resetFilters()" class="btn-reset">Reset</button>
        </div>
    </div>

    <script>
    var markerStore = null;

    function applyFilters() {{
        console.log("Apply button clicked...");
        const minR = parseFloat(document.getElementById('range-rating').value);
        const minP = parseInt(document.getElementById('range-places').value);
        const type = document.getElementById('sel-type').value;

        let layerGroup = null;
        
        // Find the specific layer group by checking options
        for (let key in window) {{
            if (window[key] instanceof L.LayerGroup) {{
                let layers = window[key].getLayers();
                if (layers.length > 0 && layers[0].options && layers[0].options.extra_data) {{
                    layerGroup = window[key];
                    console.log("Target layer found with " + layers.length + " markers.");
                    break;
                }}
            }}
        }}

        if (!layerGroup) {{
            console.error("CRITICAL: Marker layer group not identified. Filters cannot proceed.");
            return;
        }}

        if (!markerStore) {{
            markerStore = layerGroup.getLayers();
            console.log("Initial marker backup created.");
        }}

        layerGroup.clearLayers();

        const filtered = markerStore.filter(m => {{
            const data = m.options.extra_data;
            if (!data) return false;
            return data.rating >= minR && data.places >= minP && (type === "All" || data.type === type);
        }});

        console.log("Filtered count: " + filtered.length);
        filtered.forEach(m => layerGroup.addLayer(m));
        document.getElementById('match-count').innerText = filtered.length;
    }}

    function resetFilters() {{
        document.getElementById('range-rating').value = 0;
        document.getElementById('txt-rating').innerText = 0;
        document.getElementById('range-places').value = 0;
        document.getElementById('txt-places').innerText = 0;
        document.getElementById('sel-type').value = "All";
        applyFilters();
    }}
    </script>
    """
    m.get_root().html.add_child(folium.Element(filter_html))
    m.save("index.html")
    print("🚀 Map generated: index.html with enhanced JS layer detection.")

if __name__ == "__main__":
    generate_map()
