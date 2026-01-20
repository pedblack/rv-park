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

    # Load and clean data
    df = pd.read_csv(CSV_FILE)
    df_clean = df[(df['latitude'] != 0) & (df['longitude'] != 0)].dropna(subset=['latitude', 'longitude'])

    if df_clean.empty:
        print("⚠️ No valid data found.")
        return

    # Initialize Map
    m = folium.Map(location=[38.5, -7.9], zoom_start=9, tiles="cartodbpositron")
    
    # FeatureGroup for markers - this is what the JS will search for
    marker_layer = folium.FeatureGroup(name="MainPropertyLayer")
    marker_layer.add_to(m)

    prop_types = sorted(df_clean['location_type'].unique().tolist())

    for _, row in df_clean.iterrows():
        def clean_int(val):
            try:
                if pd.isna(val) or val == "": return 0
                return int(float(val))
            except: return 0

        num_places = clean_int(row.get('num_places', 0))
        
        popup_html = f"""<div style="font-family: Arial; width: 300px;">
            <h3>{row['title']}</h3>
            <b>Places:</b> {num_places} | <b>Rating:</b> {row['avg_rating']}⭐
            <br><a href="{row['url']}" target="_blank">View on Park4Night</a>
        </div>"""

        marker = folium.Marker(
            location=[row['latitude'], row['longitude']],
            popup=folium.Popup(popup_html, max_width=350),
            icon=folium.Icon(color='green' if row['avg_rating'] >= 4 else 'orange', icon='home', prefix='fa')
        )
        
        # Binding metadata for the JS engine - Standardized to extra_data
        marker.options['extra_data'] = {
            'rating': float(row['avg_rating']),
            'places': num_places,
            'type': str(row['location_type'])
        }
        marker.add_to(marker_layer)

    # --- THE FILTER JAVASCRIPT ---
    filter_html = f"""
    <style>
        .map-overlay {{ font-family: sans-serif; background: white; border-radius: 12px; padding: 15px; box-shadow: 0 4px 20px rgba(0,0,0,0.2); position: fixed; z-index: 9999; }}
        #filter-panel {{ top: 20px; right: 20px; width: 220px; }}
        #debug-log {{ top: 20px; left: 60px; font-size: 10px; background: rgba(255,255,255,0.8); padding: 5px; border-radius: 4px; border: 1px solid #ccc; }}
        .btn-apply {{ background: #2c3e50; color: white; width: 100%; padding: 10px; border-radius: 6px; cursor: pointer; font-weight: bold; border: none; margin-top: 10px; }}
        .btn-reset {{ background: #95a5a6; color: white; width: 100%; padding: 10px; border-radius: 6px; cursor: pointer; font-weight: bold; border: none; margin-top: 5px; }}
    </style>

    <div id="debug-log" class="map-overlay">Status: Ready</div>

    <div id="filter-panel" class="map-overlay">
        <h3 style="margin:0;">Filters</h3>
        <p style="font-size: 12px;">Sites matching: <b id="match-count">{len(df_clean)}</b></p>
        
        <label style="font-size:11px;">Min Rating: <span id="txt-rating">0</span></label>
        <input type="range" id="range-rating" min="0" max="5" step="0.1" value="0" style="width:100%" oninput="document.getElementById('txt-rating').innerText=this.value">
        
        <label style="font-size:11px;">Min Places: <span id="txt-places">0</span></label>
        <input type="range" id="range-places" min="0" max="100" step="5" value="0" style="width:100%" oninput="document.getElementById('txt-places').innerText=this.value">
        
        <select id="sel-type" style="width:100%; margin-top:10px;">
            <option value="All">All Types</option>
            {" ".join([f'<option value="{t}">{t}</option>' for t in prop_types])}
        </select>
        
        <button onclick="applyFilters()" class="btn-apply">Apply Filters</button>
        <button onclick="resetFilters()" class="btn-reset">Reset</button>
    </div>

    <script>
    var markerStore = null;
    var targetLayer = null;

    function log(msg) {{
        document.getElementById('debug-log').innerText = "Status: " + msg;
        console.log(msg);
    }}

    function findLayer() {{
        // Scans all global objects to find the Folium FeatureGroup containing our markers
        for (let key in window) {{
            try {{
                if (window[key] instanceof L.LayerGroup || window[key] instanceof L.FeatureGroup) {{
                    let layers = window[key].getLayers();
                    // Robust check: ensure the layer group contains markers with our metadata key
                    if (layers.length > 0 && layers[0].options && layers[0].options.extra_data) {{
                        return window[key];
                    }}
                }}
            } catch(e) {{ continue; }}
        }
        return null;
    }}

    function applyFilters() {{
        log("Filtering...");
        const minR = parseFloat(document.getElementById('range-rating').value);
        const minP = parseInt(document.getElementById('range-places').value);
        const type = document.getElementById('sel-type').value;

        if (!targetLayer) targetLayer = findLayer();
        if (!targetLayer) {{ 
            log("Err: Layer not found"); 
            return; 
        }}

        // Initialize the master list of markers on first run
        if (!markerStore) {{
            markerStore = targetLayer.getLayers();
            log("Backup created: " + markerStore.length);
        }}

        targetLayer.clearLayers();

        const filtered = markerStore.filter(m => {{
            const d = m.options.extra_data;
            if (!d) return false;
            return d.rating >= minR && 
                   d.places >= minP && 
                   (type === "All" || d.type === type);
        }});

        filtered.forEach(m => targetLayer.addLayer(m));
        document.getElementById('match-count').innerText = filtered.length;
        log("Match: " + filtered.length);
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
    print(f"🚀 Map successfully generated for {len(df_clean)} locations.")

if __name__ == "__main__":
    generate_map()
