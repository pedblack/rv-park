import pandas as pd
import folium
import os
import json
import numpy as np

CSV_FILE = os.environ.get("CSV_FILE", "backbone_locations.csv")
STRATEGIC_FILE = "strategic_analysis.json"

def generate_map():
    if not os.path.exists(CSV_FILE):
        print(f"❌ {CSV_FILE} not found.")
        return

    # 1. Load Strategic Intelligence
    score_map = {}
    recommendation = None
    if os.path.exists(STRATEGIC_FILE):
        try:
            with open(STRATEGIC_FILE, 'r') as f:
                strategy = json.load(f)
                recommendation = strategy.get("strategic_recommendation")
                # Map p4n_ids to their cluster's opportunity score
                for cluster in strategy.get("clusters", []):
                    for p_id in cluster.get("top_performing_p4n_ids", []):
                        score_map[str(p_id)] = cluster.get("opportunity_score", 0)
        except Exception as e:
            print(f"⚠️ Could not load strategy JSON: {e}")

    # Load and clean data
    df = pd.read_csv(CSV_FILE)
    df_clean = df[(df['latitude'] != 0) & (df['longitude'] != 0)].dropna(subset=['latitude', 'longitude'])

    if df_clean.empty:
        print("⚠️ No valid data found.")
        return

    # Initialize Map (Centered on Alentejo/Algarve)
    m = folium.Map(location=[38.0, -8.5], zoom_start=8, tiles="cartodbpositron")
    
    marker_layer = folium.FeatureGroup(name="MainPropertyLayer")
    marker_layer.add_to(m)
    layer_variable_name = marker_layer.get_name()

    prop_types = sorted(df_clean['location_type'].unique().tolist())

    for _, row in df_clean.iterrows():
        def clean_int(val):
            try:
                if pd.isna(val) or val == "": return 0
                return int(float(val))
            except: return 0

        def format_cost(val):
            if pd.isna(val) or val == "": return "N/A"
            try:
                num = float(val)
                return "Free" if num == 0 else f"{num}€"
            except: return "N/A"

        num_places = clean_int(row.get('num_places', 0))
        p_min = format_cost(row.get('parking_min_eur'))
        p_max = format_cost(row.get('parking_max_eur'))
        elec = format_cost(row.get('electricity_eur'))
        
        parking_display = f"{p_min} - {p_max}" if p_min != p_max else p_min

        # Parse Seasonality
        seasonality_text = "No data"
        stability_ratio = 0.0
        try:
            if pd.notna(row.get('review_seasonality')):
                s_dict = json.loads(row['review_seasonality'])
                seasonality_text = ", ".join([f"{k}: {v}" for k, v in sorted(s_dict.items())[-2:]])
                # Simple check for winter stability (Nov-Feb)
                winter_count = sum(v for k, v in s_dict.items() if any(m in k for m in ["-11", "-12", "-01", "-02"]))
                stability_ratio = 1.0 if winter_count > 0 else 0.0
        except: pass
        
        # --- FIRE Intelligence Scoring ---
        opp_score = score_map.get(str(row['p4n_id']), 0)
        
        # Color Logic: CadetBlue = Gold Zone (90+), Green = Strong, Orange = Neutral
        if opp_score >= 85:
            marker_color = 'cadetblue'
            icon_type = 'star'
        elif row['avg_rating'] >= 4.2 and stability_ratio > 0:
            marker_color = 'green'
            icon_type = 'thumbs-up'
        else:
            marker_color = 'orange'
            icon_type = 'home'

        popup_html = f"""<div style="font-family: Arial; width: 320px; font-size: 13px;">
            <div style="float: right; background: {'#f1c40f' if opp_score >= 85 else '#eee'}; padding: 4px; border-radius: 4px; font-weight: bold;">
                Score: {opp_score if opp_score > 0 else 'N/A'}
            </div>
            <h3 style="margin-bottom: 5px; margin-top: 0;">{row['title']}</h3>
            <div style="color: #666; font-style: italic; margin-bottom: 10px;">{row['location_type']}</div>
            
            <b>FIRE Stats:</b> {num_places} places | <b>Rating:</b> {row['avg_rating']}⭐ ({row['total_reviews']} revs)<br>
            <b>Costs:</b> {parking_display} | <b>Elec:</b> {elec}<br>
            <b>Demographics:</b> {row.get('top_languages', 'N/A')}<br>
            <b>Winter Stability:</b> {'✅ STABLE' if stability_ratio > 0 else '❌ SEASONAL'}<br>
            <span style="font-size: 10px; color: #888;">Recent: {seasonality_text}</span>
            
            <div style="margin-top: 10px; border-top: 1px solid #eee; padding-top: 10px;">
                <b style="color: green;">Growth Moats (Pros):</b><br>
                <span style="font-size: 11px;">{row.get('ai_pros', 'None listed')}</span>
            </div>
            <div style="margin-top: 5px;">
                <b style="color: #d35400;">Yield Risks (Cons):</b><br>
                <span style="font-size: 11px;">{row.get('ai_cons', 'None listed')}</span>
            </div>

            <br><a href="{row['url']}" target="_blank" style="display: block; text-align: center; background: #2c3e50; color: white; padding: 8px; border-radius: 4px; text-decoration: none; font-weight: bold;">View Data Source</a>
        </div>"""

        marker = folium.Marker(
            location=[row['latitude'], row['longitude']],
            popup=folium.Popup(popup_html, max_width=350),
            icon=folium.Icon(color=marker_color, icon=icon_type, prefix='fa')
        )
        
        marker.options['extraData'] = {
            'rating': float(row['avg_rating']),
            'places': num_places,
            'type': str(row['location_type']),
            'score': opp_score
        }
        marker.add_to(marker_layer)

    # --- STRATEGIC INTELLIGENCE OVERLAY ---
    strat_box = f"""
    <div id="strat-panel" class="map-overlay" style="bottom: 20px; left: 20px; width: 280px; border-left: 5px solid #f1c40f;">
        <h4 style="margin:0; color: #2c3e50;">🔥 FIRE Investment Memo</h4>
        <hr style="margin: 10px 0;">
        <div style="font-size: 12px;">
            <b>Target Region:</b> {recommendation['target_region'] if recommendation else 'Running Analysis...'}<br>
            <b>Max Opportunity:</b> <span style="color: #27ae60; font-weight: bold;">{recommendation['opportunity_score'] if recommendation else 'N/A'} pts</span><br>
            <p style="margin-top: 8px; font-style: italic;">"{recommendation['market_gap'] if recommendation else 'Awaiting data...'}"</p>
        </div>
        <div style="font-size: 10px; color: #7f8c8d; margin-top: 5px;">
            <b>Mandate:</b> Target high-WTP clusters (DE/NL) in 'Service Deserts'.
        </div>
    </div>
    """

    filter_html = f"""
    <style>
        .map-overlay {{ font-family: sans-serif; background: white; border-radius: 12px; padding: 15px; box-shadow: 0 4px 20px rgba(0,0,0,0.2); position: fixed; z-index: 9999; }}
        #filter-panel {{ top: 20px; right: 20px; width: 220px; }}
        #debug-log {{ top: 20px; left: 60px; font-size: 10px; background: rgba(255,255,255,0.8); padding: 5px; border-radius: 4px; border: 1px solid #ccc; }}
        .btn-apply {{ background: #2c3e50; color: white; width: 100%; padding: 10px; border-radius: 6px; cursor: pointer; font-weight: bold; border: none; margin-top: 10px; }}
        .btn-reset {{ background: #95a5a6; color: white; width: 100%; padding: 10px; border-radius: 6px; cursor: pointer; font-weight: bold; border: none; margin-top: 5px; }}
    </style>

    <div id="debug-log" class="map-overlay">Status: Ready</div>
    {strat_box}

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

    function applyFilters() {{
        const minR = parseFloat(document.getElementById('range-rating').value);
        const minP = parseInt(document.getElementById('range-places').value);
        const type = document.getElementById('sel-type').value;

        var targetLayer = window['{layer_variable_name}'];
        if (!markerStore) markerStore = targetLayer.getLayers();

        targetLayer.clearLayers();

        const filtered = markerStore.filter(m => {{
            const d = m.options.extraData;
            return d.rating >= minR && d.places >= minP && (type === "All" || d.type === type);
        }});

        filtered.forEach(m => targetLayer.addLayer(m));
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
    print(f"🚀 Map generated. Intelligence Panel active.")

if __name__ == "__main__":
    generate_map()
