import pandas as pd
import json
import numpy as np
import os
from datetime import datetime, timedelta
from sklearn.cluster import DBSCAN

# --- DYNAMIC CONFIGURATION ---
INPUT_CSV = os.environ.get("INPUT_CSV", "backbone_locations.csv")
OUTPUT_JSON = "strategic_analysis.json"
STALENESS_DAYS = 30
EARTH_RADIUS_KM = 6371.0
CLUSTER_RADIUS_KM = 15.0  # Proper km distance

FRUSTRATION_KEYWORDS = ["full", "crowded", "dirty", "no space", "loud", "police", "fines", "busy"]
HIGH_WTP_LANGUAGES = ["German", "Dutch", "English"]

def load_and_filter_data():
    if not os.path.exists(INPUT_CSV):
        return None
    df = pd.read_csv(INPUT_CSV)
    df['last_scraped'] = pd.to_datetime(df['last_scraped'])
    freshness_threshold = datetime.now() - timedelta(days=STALENESS_DAYS)
    return df[df['last_scraped'] >= freshness_threshold].copy()

def calculate_seasonality_stability(seasonality_json):
    try:
        data = json.loads(seasonality_json)
        if not data: return 0.0
        winter = sum(v for k, v in data.items() if any(m in k for m in ["-11", "-12", "-01", "-02"]))
        summer = sum(v for k, v in data.items() if any(m in k for m in ["-06", "-07", "-08", "-09"]))
        return min(1.0, winter / summer) if summer > 0 else (1.0 if winter > 0 else 0.0)
    except: return 0.0

def run_analysis():
    df = load_and_filter_data()
    if df is None or df.empty: return

    df['stability_score'] = df['review_seasonality'].apply(calculate_seasonality_stability)
    df['frustration_score'] = df['ai_cons'].apply(lambda x: min(1.0, sum(1 for w in FRUSTRATION_KEYWORDS if w in str(x).lower()) / 3.0))

    # DBSCAN using Haversine (Radians)
    coords = np.radians(df[['latitude', 'longitude']].values)
    epsilon = CLUSTER_RADIUS_KM / EARTH_RADIUS_KM
    db = DBSCAN(eps=epsilon, min_samples=1, metric='haversine').fit(coords)
    df['cluster_id'] = db.labels_

    full_score_map = {}
    cluster_intelligence = []

    for cid in df['cluster_id'].unique():
        c_df = df[df['cluster_id'] == cid]
        
        demand = min(30, (c_df['total_reviews'].sum() / 200) * 30)
        gap = min(30, (c_df['frustration_score'].mean() * 30) + ((5 - c_df['avg_rating'].mean()) * 6))
        wtp = min(20, (sum(1 for lang in HIGH_WTP_LANGUAGES if any(lang in str(x) for x in c_df['top_languages'])) / len(c_df)) * 20)
        season = c_df['stability_score'].mean() * 20
        total_score = round(demand + gap + wtp + season, 2)

        for p_id in c_df['p4n_id']:
            full_score_map[str(p_id)] = total_score

        cluster_intelligence.append({
            "cluster_name": f"Region {cid} ({c_df.iloc[0]['title'][:15]})",
            "opportunity_score": total_score,
            "top_performing_p4n_ids": c_df.nlargest(3, 'total_reviews')['p4n_id'].tolist()
        })

    clusters = sorted(cluster_intelligence, key=lambda x: x['opportunity_score'], reverse=True)
    
    result = {
        "strategic_recommendation": {
            "target_region": clusters[0]['cluster_name'],
            "opportunity_score": clusters[0]['opportunity_score'],
            "market_gap": "High frustration index identified in cluster."
        },
        "full_score_map": full_score_map,
        "clusters": clusters[:5]
    }

    with open(OUTPUT_JSON, 'w') as f:
        # FIX: Swapped positional argument 'f' with keyword argument 'indent'
        json.dump(result, f, indent=4)
    
    print(f"✅ Strategic Analysis complete. Top Opportunity: {clusters[0]['opportunity_score']} in {clusters[0]['cluster_name']}")

if __name__ == "__main__":
    run_analysis()
