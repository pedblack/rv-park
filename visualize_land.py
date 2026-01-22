import pandas as pd
import folium
import os
import json

# --- DYNAMIC CONFIGURATION ---
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
                score_map = strategy.get("full_score_map", {})
        except Exception as e:
            print(f"⚠️ Could not load strategy JSON: {e}")

    # 2. Load and Clean Data
    df = pd.read_csv(CSV_FILE)
    
    # DEFENSIVE FIX: Ensure numeric columns are clean to prevent int(NaN) crashes
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce').fillna(0)
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce').fillna(0)
    df['avg_rating'] = pd.to_numeric(df['avg_rating'], errors='coerce').fillna(0)
    df['num_places'] = pd.to_numeric(df['num_places'], errors='coerce').fillna(0).astype(int)

    df_clean = df[(df['latitude'] != 0) & (df['longitude'] != 0)].dropna(subset=['latitude', 'longitude'])

    # 3. Initialize Map
    m = folium.Map(location=[38.0, -8.5], zoom_start=8, tiles="cartodbpositron")
    
    # REQUIREMENT: Add Chart.js for the histogram
    m.get_root().header.add_child(folium.Element('<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>'))

    marker_layer = folium.FeatureGroup(name="MainPropertyLayer")
    marker_layer.add_to(m)
    layer_var = marker_layer.get_name()

    for _, row in df_clean.iterrows():
        opp_score = score_map.get(str(row['p4n_id']), 0)
        
        marker = folium.Marker(
            location=[row['latitude'], row['longitude']],
            popup=folium.Popup(f"<b>{row['title']}</b>", max_width=350),
            icon=folium.Icon(color='green' if opp_score >= 60 else 'orange', icon='home', prefix='fa')
        )
        
        # DATA EMBEDDING: Store cleaned numeric data for JS to aggregate
        marker.options['extraData'] = {
            'rating': float(row['avg_rating']),
            'places': int(row['num_places']),
            'type': str(row['location_type']),
            'seasonality': row['review_seasonality'] if pd.notna(row['review_seasonality']) else "{}",
            'pros': row['ai_pros'] if pd.notna(row['ai_pros']) else "",
            'cons': row['ai_cons'] if pd.notna(row['ai_cons']) else ""
        }
        marker.add_to(marker_layer)

    # 4. UI AND AGGREGATION LOGIC
    dashboard_html = f"""
    <style>
        .map-overlay {{ font-family: sans-serif; background: white; border-radius: 12px; padding: 15px; box-shadow: 0 4px 20px rgba(0,0,0,0.2); position: fixed; z-index: 9999; overflow-y: auto; }}
        #filter-panel {{ top: 20px; right: 20px; width: 220px; }}
        #stats-panel {{ top: 20px; left: 20px; width: 320px; max-height: 85vh; }}
        .stat-section {{ margin-top: 15px; border-top: 1px solid #eee; padding-top: 10px; font-size: 11px; }}
        .tag-item {{ display: flex; justify-content: space-between; margin-bottom: 2px; }}
        .tag-item b {{ color: #2c3e50; }}
    </style>

    <div id="stats-panel" class="map-overlay">
        <h4 style="margin:0;">📊 Market Intelligence</h4>
        <p style="font-size: 11px; color: #666;">Aggregating <span id="agg-count">{len(df_clean)}</span> visible sites</p>
        
        <div class="stat-section">
            <b>Review Seasonality (Total)</b>
            <canvas id="seasonChart" height="150"></canvas>
        </div>

        <div class="stat-section">
            <b style="color: green;">Top 10 Pros</b>
            <div id="top-pros" style="margin-top:5px;"></div>
        </div>

        <div class="stat-section">
            <b style="color: #d35400;">Top 10 Cons</b>
            <div id="top-cons" style="margin-top:5px;"></div>
        </div>
    </div>

    <script>
    var markerStore = null;
    var chartInstance = null;

    function parseThemeString(str) {{
        const results = {{}};
        if (!str) return results;
        str.split(';').forEach(item => {{
            const match = item.match(/(.+)\\s\\((\\d+)\\)/);
            if (match) {{
                results[match[1].trim()] = parseInt(match[2]);
            }}
        }});
        return results;
    }}

    function updateDashboard(activeMarkers) {{
        let globalSeason = {{}};
        let globalPros = {{}};
        let globalCons = {{}};

        activeMarkers.forEach(m => {{
            const d = m.options.extraData;
            
            try {{
                const s = JSON.parse(d.seasonality);
                for (let date in s) {{
                    const month = date.split('-')[1];
                    globalSeason[month] = (globalSeason[month] || 0) + s[date];
                }}
            }} catch(e) {{}}

            const p = parseThemeString(d.pros);
            for (let k in p) {{ globalPros[k] = (globalPros[k] || 0) + p[k]; }}
            const c = parseThemeString(d.cons);
            for (let k in c) {{ globalCons[k] = (globalCons[k] || 0) + c[k]; }}
        }});

        const labels = ["01","02","03","04","05","06","07","08","09","10","11","12"];
        const ctx = document.getElementById('seasonChart').getContext('2d');
        if (chartInstance) chartInstance.destroy();
        chartInstance = new Chart(ctx, {{
            type: 'bar',
            data: {{
                labels: labels,
                datasets: [{{ label: 'Reviews', data: labels.map(l => globalSeason[l] || 0), backgroundColor: '#3498db' }}]
            }},
            options: {{ plugins: {{ legend: {{ display: false }} }}, scales: {{ y: {{ beginAtZero: true }} }} }}
        }});

        const renderTop10 = (data, divId) => {{
            const sorted = Object.entries(data).sort((a,b) => b[1]-a[1]).slice(0, 10);
            document.getElementById(divId).innerHTML = sorted.map(i => 
                `<div class="tag-item"><span>${{i[0]}}</span><b>${{i[1]}}</b></div>`).join('');
        }};
        renderTop10(globalPros, 'top-pros');
        renderTop10(globalCons, 'top-cons');
        document.getElementById('agg-count').innerText = activeMarkers.length;
    }}

    function applyFilters() {{
        const minR = parseFloat(document.getElementById('range-rating').value);
        const minP = parseInt(document.getElementById('range-places').value);
        const type = document.getElementById('sel-type').value;

        var targetLayer = window['{layer_var}'];
        if (!markerStore) markerStore = targetLayer.getLayers();

        targetLayer.clearLayers();
        const filtered = markerStore.filter(m => {{
            const d = m.options.extraData;
            return d.rating >= minR && d.places >= minP && (type === "All" || d.type === type);
        }});

        filtered.forEach(m => targetLayer.addLayer(m));
        
        updateDashboard(filtered);
    }}
    
    window.onload = () => {{
        setTimeout(() => {{
            var layer = window['{layer_var}'];
            updateDashboard(layer.getLayers());
        }}, 1000);
    }};
    </script>
    """
    m.get_root().html.add_child(folium.Element(dashboard_html))
    m.save("index.html")
    print("✅ index.html generated successfully.")

if __name__ == "__main__":
    generate_map()
