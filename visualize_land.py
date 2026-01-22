import json
import os

import folium
import numpy as np
import pandas as pd

# Use environment variables to support both prod and dev modes
CSV_FILE = os.environ.get("CSV_FILE", "backbone_locations.csv")
STRATEGIC_FILE = "strategic_analysis.json"


def generate_map(output_file="index.html"):
    if not os.path.exists(CSV_FILE):
        print(f"❌ {CSV_FILE} not found.")
        return

    # 1. Load Strategic Intelligence (Universal Score Map)
    score_map = {}
    recommendation = None
    if os.path.exists(STRATEGIC_FILE):
        try:
            with open(STRATEGIC_FILE, "r") as f:
                strategy = json.load(f)
                recommendation = strategy.get("strategic_recommendation")
                score_map = strategy.get("full_score_map", {})
        except Exception as e:
            print(f"⚠️ Could not load strategy JSON: {e}")

    # 2. Load and clean data
    df = pd.read_csv(CSV_FILE)

    # Defensive cleaning
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce").fillna(0)
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce").fillna(0)
    df["avg_rating"] = pd.to_numeric(df["avg_rating"], errors="coerce").fillna(0)
    df["num_places"] = (
        pd.to_numeric(df["num_places"], errors="coerce").fillna(0).astype(int)
    )
    df["total_reviews"] = (
        pd.to_numeric(df["total_reviews"], errors="coerce").fillna(0).astype(int)
    )

    df_clean = df[(df["latitude"] != 0) & (df["longitude"] != 0)].dropna(
        subset=["latitude", "longitude"]
    )

    if df_clean.empty:
        print("⚠️ No valid data found.")
        return

    # Dynamic limits for sliders
    max_p_limit = int(df_clean["num_places"].max()) if not df_clean.empty else 100

    # 3. Initialize Map
    m = folium.Map(location=[38.0, -8.5], zoom_start=8, tiles="cartodbpositron")

    # Inject External Dependencies (noUiSlider & Chart.js)
    m.get_root().header.add_child(
        folium.Element('<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>')
    )
    m.get_root().header.add_child(
        folium.Element(
            '<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/nouislider/dist/nouislider.min.css">'
        )
    )
    m.get_root().header.add_child(
        folium.Element(
            '<script src="https://cdn.jsdelivr.net/npm/nouislider/dist/nouislider.min.js"></script>'
        )
    )

    marker_layer = folium.FeatureGroup(name="MainPropertyLayer")
    marker_layer.add_to(m)
    layer_name = marker_layer.get_name()  # Capture internal ID for JS

    prop_types = sorted(df_clean["location_type"].unique().tolist())

    for _, row in df_clean.iterrows():

        def clean_int(val):
            try:
                if pd.isna(val) or val == "":
                    return 0
                return int(float(val))
            except:
                return 0

        def format_cost(val):
            if pd.isna(val) or val == "":
                return "N/A"
            try:
                num = float(val)
                return "Free" if num == 0 else f"{num}€"
            except:
                return "N/A"

        num_places = clean_int(row.get("num_places", 0))
        p_min = format_cost(row.get("parking_min_eur"))
        p_max = format_cost(row.get("parking_max_eur"))
        elec = format_cost(row.get("electricity_eur"))
        parking_display = f"{p_min} - {p_max}" if p_min != p_max else p_min

        seasonality_text = "No data"
        stability_ratio = 0.0
        try:
            if pd.notna(row.get("review_seasonality")):
                s_dict = json.loads(row["review_seasonality"])
                sorted_keys = sorted(s_dict.keys())
                seasonality_text = ", ".join(
                    [f"{k}: {s_dict[k]}" for k in sorted_keys[-2:]]
                )
                winter_count = sum(
                    v
                    for k, v in s_dict.items()
                    if any(m in k for m in ["-11", "-12", "-01", "-02"])
                )
                stability_ratio = 1.0 if winter_count > 0 else 0.0
        except:
            pass

        opp_score = score_map.get(str(row["p4n_id"]), 0)

        if opp_score >= 85:
            marker_color, icon_type = "cadetblue", "star"
        elif opp_score >= 60:
            marker_color, icon_type = "green", "thumbs-up"
        else:
            marker_color, icon_type = "orange", "home"

        popup_html = f"""<div style="font-family: Arial; width: 320px; font-size: 13px;">
            <div style="float: right; background: {'#f1c40f' if opp_score >= 85 else '#eee'}; padding: 4px; border-radius: 4px; font-weight: bold;">Score: {opp_score}</div>
            <h3 style="margin: 0;">{row['title']}</h3>
            <div style="color: #666; font-style: italic; margin-bottom: 10px;">{row['location_type']}</div>
            <b>FIRE Stats:</b> {num_places} places | <b>Rating:</b> {row['avg_rating']}⭐ ({row['total_reviews']} revs)<br>
            <b>Costs:</b> {parking_display} | <b>Elec:</b> {elec}<br>
            <b>Winter Stability:</b> {'✅ STABLE' if stability_ratio > 0 else '❌ SEASONAL'}<br>
            <div style="margin-top: 10px; border-top: 1px solid #eee; padding-top: 10px;">
                <b style="color: green;">Growth Moats:</b><br><span style="font-size: 11px;">{row.get('ai_pros', 'None listed')}</span>
            </div>
            <div style="margin-top: 5px;">
                <b style="color: #d35400;">Yield Risks:</b><br><span style="font-size: 11px;">{row.get('ai_cons', 'None listed')}</span>
            </div>
            <br><a href="{row['url']}" target="_blank" style="display: block; text-align: center; background: #2c3e50; color: white; padding: 8px; border-radius: 4px; text-decoration: none; font-weight: bold;">View Data Source</a>
        </div>"""

        marker = folium.Marker(
            location=[row["latitude"], row["longitude"]],
            popup=folium.Popup(popup_html, max_width=350),
            icon=folium.Icon(color=marker_color, icon=icon_type, prefix="fa"),
        )

        # We assign as a standard dictionary
        marker.options["extraData"] = {
            "rating": float(row["avg_rating"]),
            "places": int(num_places),
            "reviews": int(row["total_reviews"]),
            "type": str(row["location_type"]),
            "seasonality": (
                row["review_seasonality"]
                if pd.notna(row["review_seasonality"])
                else "{}"
            ),
            "pros": row["ai_pros"] if pd.notna(row["ai_pros"]) else "",
            "cons": row["ai_cons"] if pd.notna(row["ai_cons"]) else "",
        }
        marker.add_to(marker_layer)

    strat_box = f"""
    <div id="strat-panel" class="map-overlay" style="bottom: 20px; left: 20px; width: 280px; border-left: 5px solid #f1c40f; position: fixed; z-index: 9999; background: white; padding: 15px; border-radius: 12px; box-shadow: 0 4px 20px rgba(0,0,0,0.2); font-family: sans-serif;">
        <h4 style="margin:0; color: #2c3e50;">🔥 FIRE Investment Memo</h4>
        <hr style="margin: 10px 0;">
        <div style="font-size: 12px;">
            <b>Target Region:</b> {recommendation['target_region'] if recommendation else 'Awaiting Analysis...'}<br>
            <b>Max Opportunity:</b> <span style="color: #27ae60; font-weight: bold;">{recommendation['opportunity_score'] if recommendation else 'N/A'} pts</span><br>
            <p style="margin-top: 8px; font-style: italic;">"{recommendation['market_gap'] if recommendation else 'Recalculating...'}"</p>
        </div>
    </div>
    """

    ui_html = f"""
    <style>
        .map-overlay {{ font-family: sans-serif; background: white; border-radius: 12px; padding: 15px; box-shadow: 0 4px 20px rgba(0,0,0,0.2); position: fixed; z-index: 9999; overflow-y: auto; }}
        #filter-panel {{ top: 20px; right: 20px; width: 250px; max-height: 90vh; }}
        #stats-panel {{ top: 20px; left: 20px; width: 320px; max-height: 70vh; }}
        .stat-section {{ margin-top: 15px; border-top: 1px solid #eee; padding-top: 10px; font-size: 11px; }}
        .tag-item {{ display: flex; justify-content: space-between; margin-bottom: 2px; }}
        .slider-wrap {{ margin: 10px 10px 25px 10px; }}
        .noUi-connect {{ background: #2c3e50; }}
        .noUi-handle {{ width: 18px !important; height: 18px !important; right: -9px !important; top: -5px !important; border-radius: 50%; cursor: pointer; }}
        .noUi-handle:after, .noUi-handle:before {{ display: none; }}
        select[multiple] {{ width: 100%; height: 120px; border-radius: 6px; border: 1px solid #ccc; }}
    </style>

    {strat_box}

    <div id="stats-panel" class="map-overlay">
        <h4 style="margin:0;">📊 Market Intelligence</h4>
        <p style="font-size: 11px; color: #666; margin-bottom: 4px;">Aggregating <span id="agg-count">0</span> sites</p>
        <p style="font-size: 11px; color: #2c3e50; margin: 0;"><b>Total Places:</b> <span id="total-places-count">0</span></p>
        <p style="font-size: 11px; color: #2c3e50; margin: 0;"><b>Avg Rating:</b> <span id="avg-rating-count">0</span> ⭐</p>
        
        <div class="stat-section">
            <b>Review Seasonality</b>
            <canvas id="seasonChart" height="150"></canvas>
        </div>
        <div class="stat-section">
            <b style="color: green;">Top Pros</b>
            <div id="top-pros"></div>
        </div>
        <div class="stat-section">
            <b style="color: #d35400;">Top Cons</b>
            <div id="top-cons"></div>
        </div>
    </div>

    <div id="filter-panel" class="map-overlay">
        <h3 style="margin:0 0 10px 0;">Filters</h3>
        <div class="stat-section" style="border:none; padding:0;">
            <b>Rating: <span id="lbl-rating">0 - 5</span> ⭐</b>
            <div class="slider-wrap"><div id="slider-rating"></div></div>
        </div>
        <div class="stat-section">
            <b>Places: <span id="lbl-places">0 - {max_p_limit}</span></b>
            <div class="slider-wrap"><div id="slider-places"></div></div>
        </div>
        <div class="stat-section">
            <b>Property Types</b>
            <select id="sel-type" multiple>
                <option value="All" selected>All Types</option>
                {" ".join([f'<option value="{t}">{t}</option>' for t in prop_types])}
            </select>
        </div>
        <button onclick="applyFilters()" style="width:100%; padding:10px; background:#2c3e50; color:white; border:none; border-radius:6px; cursor:pointer; font-weight:bold; margin-top:10px;">Apply Filters</button>
        <button onclick="resetFilters()" style="width:100%; margin-top:5px; border:none; background:none; color:#666; cursor:pointer; font-size:11px;">Reset All</button>
    </div>

    <script>
    var markerStore = null;
    var chartInstance = null;
    var sRating, sPlaces;

    function parseThemeString(str) {{
        const results = {{}};
        if (!str) return results;
        str.split(';').forEach(item => {{
            const match = item.match(/(.+)\\s\\((\\d+)\\)/);
            if (match) results[match[1].trim()] = parseInt(match[2]);
        }});
        return results;
    }}

    function updateDashboard(activeMarkers) {{
        let globalSeason = {{}}, globalPros = {{}}, globalCons = {{}};
        let totalPlaces = 0, totalRating = 0;

        activeMarkers.forEach(m => {{
            const d = m.options.extraData;
            totalPlaces += d.places;
            totalRating += d.rating;

            try {{
                const s = JSON.parse(d.seasonality);
                for (let date in s) {{
                    const month = date.split('-')[1];
                    globalSeason[month] = (globalSeason[month] || 0) + s[date];
                }}
            }} catch(e) {{}}
            
            const p = parseThemeString(d.pros);
            for (let k in p) globalPros[k] = (globalPros[k] || 0) + p[k];
            const c = parseThemeString(d.cons);
            for (let k in c) globalCons[k] = (globalCons[k] || 0) + c[k];
        }});

        document.getElementById('agg-count').innerText = activeMarkers.length;
        document.getElementById('total-places-count').innerText = totalPlaces.toLocaleString();
        document.getElementById('avg-rating-count').innerText = activeMarkers.length ? (totalRating / activeMarkers.length).toFixed(2) : 0;

        const labels = ["01","02","03","04","05","06","07","08","09","10","11","12"];
        const ctx = document.getElementById('seasonChart').getContext('2d');
        if (chartInstance) chartInstance.destroy();
        chartInstance = new Chart(ctx, {{
            type: 'bar',
            data: {{ labels, datasets: [{{ label: 'Reviews', data: labels.map(l => globalSeason[l] || 0), backgroundColor: '#3498db' }}] }},
            options: {{ plugins: {{ legend: {{ display: false }} }}, scales: {{ y: {{ beginAtZero: true }} }} }}
        }});

        const renderList = (data, id) => {{
            const sorted = Object.entries(data).sort((a,b) => b[1]-a[1]).slice(0, 8);
            document.getElementById(id).innerHTML = sorted.map(i => `<div class="tag-item"><span>${{i[0]}}</span><b>${{i[1]}}</b></div>`).join('');
        }};
        renderList(globalPros, 'top-pros');
        renderList(globalCons, 'top-cons');
    }}

    function applyFilters() {{
        const [minR, maxR] = sRating.noUiSlider.get().map(parseFloat);
        const [minP, maxP] = sPlaces.noUiSlider.get().map(Number);
        const selTypes = Array.from(document.getElementById('sel-type').selectedOptions).map(o => o.value);

        const targetLayer = {layer_name};
        if (!markerStore) {{
            markerStore = targetLayer.getLayers();
        }}

        targetLayer.clearLayers();
        const filtered = markerStore.filter(m => {{
            const d = m.options.extraData;
            const typeMatch = selTypes.includes("All") || selTypes.includes(d.type) || selTypes.length === 0;
            return d.rating >= minR && d.rating <= maxR && d.places >= minP && d.places <= maxP && typeMatch;
        }});

        filtered.forEach(m => targetLayer.addLayer(m));
        updateDashboard(filtered);
    }}

    function resetFilters() {{
        sRating.noUiSlider.set([0, 5]);
        sPlaces.noUiSlider.set([0, {max_p_limit}]);
        const sel = document.getElementById('sel-type');
        for (let i=0; i<sel.options.length; i++) sel.options[i].selected = (sel.options[i].value === "All");
        applyFilters();
    }}
    
    window.onload = () => {{
        sRating = document.getElementById('slider-rating');
        noUiSlider.create(sRating, {{ start: [0, 5], connect: true, step: 0.1, range: {{'min': 0, 'max': 5}} }});
        sRating.noUiSlider.on('update', v => document.getElementById('lbl-rating').innerText = parseFloat(v[0]).toFixed(1) + ' - ' + parseFloat(v[1]).toFixed(1));

        sPlaces = document.getElementById('slider-places');
        noUiSlider.create(sPlaces, {{ start: [0, {max_p_limit}], connect: true, step: 1, range: {{'min': 0, 'max': {max_p_limit}}} }});
        sPlaces.noUiSlider.on('update', v => document.getElementById('lbl-places').innerText = parseInt(v[0]) + ' - ' + parseInt(v[1]));

        setTimeout(() => {{
            const layer = {layer_name};
            if (layer) {{
                updateDashboard(layer.getLayers());
            }}
        }}, 1000);
    }};
    </script>
    """
    m.get_root().html.add_child(folium.Element(ui_html))
    m.save(output_file)


if __name__ == "__main__":
    generate_map()
