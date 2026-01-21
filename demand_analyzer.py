import pandas as pd
import json
import numpy as np
import os
from datetime import datetime, timedelta
from sklearn.cluster import DBSCAN

# --- CONFIGURABLE CONSTANTS ---
INPUT_CSV = "backbone_locations.csv"
OUTPUT_JSON = "strategic_analysis.json"
STALENESS_DAYS = 30
CLUSTER_RADIUS_KM = 15  # Approx 0.15 degrees
MIN_REVIEWS_FOR_STABILITY = 10

# Keywords for "Frustration Index"
FRUSTRATION_KEYWORDS = ["full", "crowded", "dirty", "no space", "loud", "police", "fines", "busy"]
HIGH_WTP_LANGUAGES = ["German", "Dutch", "English"]

def load_and_filter_data():
    if not os.path.exists(INPUT_CSV):
        print(f"❌ {INPUT_CSV} not found.")
        return None
    
    df = pd.read_csv(INPUT_CSV)
    df['last_scraped'] = pd.to_datetime(df['last_scraped'])
    
    # Filter for Freshness (FIRE Requirement: Yield-on-Time depends on accurate data)
    freshness_threshold = datetime.now() - timedelta(days=STALENESS_DAYS)
    df = df[df['last_scraped'] >= freshness_threshold].copy()
    return df

def calculate_seasonality_stability(seasonality_json):
    """
    Calculates the 'Winter Floor'. 
    Ratio of Winter (Nov-Feb) reviews vs Summer (Jun-Sep).
    Protects the €60k/year lifestyle income requirement.
    """
    try:
        data = json.loads(seasonality_json)
        if not data: return 0.0
        
        winter_months = ["-11", "-12", "-01", "-02"]
        summer_months = ["-06", "-07", "-08", "-09"]
        
        winter_count = sum(v for k, v in data.items() if any(m in k for m in winter_months))
        summer_count = sum(v for k, v in data.items() if any(m in k for m in summer_months))
        
        if summer_count == 0: return 1.0 if winter_count > 0 else 0.0
        return min(1.0, winter_count / summer_count)
    except:
        return 0.0

def get_frustration_index(cons_text):
    """Detects service failures in competitors to find 'Service Deserts'."""
    if pd.isna(cons_text): return 0.0
    matches = sum(1 for word in FRUSTRATION_KEYWORDS if word in cons_text.lower())
    return min(1.0, matches / 3.0)

def run_analysis():
    df = load_and_filter_data()
    if df is None or df.empty: return

    # 1. Seasonality & Frustration Mapping
    df['stability_score'] = df['review_seasonality'].apply(calculate_seasonality_stability)
    df['frustration_score'] = df['ai_cons'].apply(get_frustration_index)

    # 2. Geographic Clustering (DBSCAN)
    # Using 0.15 degrees as a proxy for 15km
    coords = df[['latitude', 'longitude']].values
    db = DBSCAN(eps=0.15, min_samples=1).fit(coords)
    df['cluster_id'] = db.labels_

    # 3. Aggregate Cluster Intelligence
    clusters = []
    for cid in df['cluster_id'].unique():
        c_df = df[df['cluster_id'] == cid]
        
        # Calculate Opportunity Score (0-100) based on FIRE Opportunity Scorecard
        # Demand (30) + Competitive Gap (30) + WTP (20) + Seasonality (20)
        demand_score = min(30, (c_df['total_reviews'].sum() / 200) * 30)
        gap_score = min(30, (c_df['frustration_score'].mean() * 30) + ((5 - c_df['avg_rating'].mean()) * 6))
        
        wtp_count = sum(1 for lang in HIGH_WTP_LANGUAGES if any(lang in str(x) for x in c_df['top_languages']))
        wtp_score = min(20, (wtp_count / len(c_df)) * 20)
        
        stability_score = c_df['stability_score'].mean() * 20
        
        total_opp_score = demand_score + gap_score + wtp_score + stability_score

        clusters.append({
            "cluster_name": f"Region {cid} (Near {c_df.iloc[0]['title'][:20]}...)",
            "opportunity_score": round(total_opp_score, 2),
            "demand_density": "High" if demand_score > 20 else "Medium",
            "frustration_index": round(c_df['frustration_score'].mean(), 2),
            "seasonality": {
                "stability_rating": round(c_df['stability_score'].mean(), 2),
                "winter_viability": "Strong" if stability_score > 15 else "Seasonal Risk"
            },
            "avg_competitor_rating": round(c_df['avg_rating'].mean(), 2),
            "recommended_pricing": round(c_df['parking_max_eur'].mean() * 1.2, 2) if not c_df['parking_max_eur'].isna().all() else 15.0,
            "top_performing_p4n_ids": c_df.nlargest(3, 'total_reviews')['p4n_id'].tolist()
        })

    # Sort by Opportunity Score (North Star Metric)
    clusters = sorted(clusters, key=lambda x: x['opportunity_score'], reverse=True)

    # 4. Final Strategic JSON
    analysis_result = {
        "strategic_recommendation": {
            "target_region": clusters[0]['cluster_name'],
            "opportunity_score": clusters[0]['opportunity_score'],
            "primary_moat": "High frustration index combined with winter stability.",
            "market_gap": "Current competitors are over-capacity and under-serving high-WTP demographics."
        },
        "clusters": clusters[:5],
        "three_critical_vulnerabilities": [
            "Data dependency on Park4Night seasonality samples.",
            "Regulatory 'License Creep' if adding more than 50 places.",
            "Lombard Loan interest rate volatility vs 10% target yield."
        ],
        "two_levers_for_passivity": [
            "Automated gate entry integrated with 'Review Velocity' price updates.",
            "Village Liaison model focused on cluster hotspots identified above."
        ]
    }

    with open(OUTPUT_JSON, 'w') as f:
        json.dump(analysis_result, indent=4, f)
    
    print(f"✅ Strategic Analysis complete. Top Opportunity: {clusters[0]['opportunity_score']} in {clusters[0]['cluster_name']}")

if __name__ == "__main__":
    run_analysis()
