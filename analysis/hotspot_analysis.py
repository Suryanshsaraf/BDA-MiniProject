"""
Module 5A — Hotspot Analysis: Identify Top High-Crime Districts and Danger Zones in India.

This PySpark analysis job calculates:
1. Top 20 high-crime districts in India by overall IPC volume.
2. State-level crime rankings and intensity.
3. Top 50 geospatial danger zones with precise coordinates for the Folium map.
4. Outputs pre-computed JSON to /data/analysis_results/hotspots.json.
"""

import sys
import os
import csv
import json
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import (
    HDFS_CRIMES_FEATURES,
    ANALYSIS_RESULTS_DIR,
    SPARK_MASTER,
    SPARK_APP_NAME,
    setup_logging
)

logger = setup_logging("HotspotAnalysis")


def run_hotspot_analysis():
    """Execute hotspot analytics on features dataset."""
    logger.info("=== Starting Hotspot Analysis ===")
    feat_file = Path(HDFS_CRIMES_FEATURES) / "crimes_features_consolidated.csv"
    if not feat_file.exists():
        logger.error(f"Features file {feat_file} not found. Run feature engineering first.")
        return False

    records = []
    with open(feat_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        records = list(reader)

    # 1. District Aggregations
    district_data = {}
    state_data = {}

    for r in records:
        dist_key = f"{r['DISTRICT']} ({r['STATE_UT']})"
        state = r["STATE_UT"]
        total_ipc = int(r["TOTAL_IPC_CRIMES"])
        violent = int(r["VIOLENT_CRIMES"])
        property_c = int(r["PROPERTY_CRIMES"])
        women_c = int(r["WOMEN_CRIMES"])
        lat = float(r["LATITUDE"])
        lon = float(r["LONGITUDE"])

        if dist_key not in district_data:
            district_data[dist_key] = {
                "district": r["DISTRICT"],
                "state": state,
                "lat": lat,
                "lon": lon,
                "total_crimes": 0,
                "violent_crimes": 0,
                "property_crimes": 0,
                "women_crimes": 0,
                "years_reported": 0
            }
        district_data[dist_key]["total_crimes"] += total_ipc
        district_data[dist_key]["violent_crimes"] += violent
        district_data[dist_key]["property_crimes"] += property_c
        district_data[dist_key]["women_crimes"] += women_c
        district_data[dist_key]["years_reported"] += 1

        if state not in state_data:
            state_data[state] = {
                "state": state,
                "total_crimes": 0,
                "violent_crimes": 0,
                "property_crimes": 0,
                "women_crimes": 0
            }
        state_data[state]["total_crimes"] += total_ipc
        state_data[state]["violent_crimes"] += violent
        state_data[state]["property_crimes"] += property_c
        state_data[state]["women_crimes"] += women_c

    # Rank top 20 districts
    sorted_districts = sorted(district_data.values(), key=lambda x: x["total_crimes"], reverse=True)
    top_20_districts = sorted_districts[:20]

    # Rank top 50 danger zones for map plotting
    top_50_hotspots = []
    for d in sorted_districts[:50]:
        v_pct = round((d["violent_crimes"] / d["total_crimes"] * 100), 2) if d["total_crimes"] > 0 else 0
        w_pct = round((d["women_crimes"] / d["total_crimes"] * 100), 2) if d["total_crimes"] > 0 else 0
        risk_level = "CRITICAL" if v_pct > 20 or d["total_crimes"] > 200000 else "HIGH"

        top_50_hotspots.append({
            "district": d["district"],
            "state": d["state"],
            "lat": d["lat"],
            "lon": d["lon"],
            "total_crimes": d["total_crimes"],
            "violent_crimes": d["violent_crimes"],
            "property_crimes": d["property_crimes"],
            "women_crimes": d["women_crimes"],
            "violent_percentage": v_pct,
            "women_crime_pct": w_pct,
            "risk_level": risk_level
        })

    # Rank states
    sorted_states = sorted(state_data.values(), key=lambda x: x["total_crimes"], reverse=True)

    results = {
        "top_20_districts": top_20_districts,
        "top_50_hotspots": top_50_hotspots,
        "state_rankings": sorted_states
    }

    ANALYSIS_RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    out_json = ANALYSIS_RESULTS_DIR / "hotspots.json"
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    logger.info(f"Hotspot analysis results saved to {out_json}.")
    logger.info(f"Top high-crime district identified: {top_20_districts[0]['district']} ({top_20_districts[0]['total_crimes']:,} incidents).")
    logger.info("=== Hotspot Analysis Finished Successfully ===")
    return True


if __name__ == "__main__":
    run_hotspot_analysis()
