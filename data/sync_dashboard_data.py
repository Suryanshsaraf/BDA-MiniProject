"""
Sync Analytical Datasets to React Dashboard.

Computes fresh regions_data.json from consolidated features and copies
all analytical outputs to react-dashboard directories.
"""

import sys
import os
import csv
import json
import shutil
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import HDFS_CRIMES_FEATURES, ANALYSIS_RESULTS_DIR, setup_logging

logger = setup_logging("SyncDashboardData")

STATE_TO_REGION = {
    'MADHYA PRADESH': 'Central',
    'CHHATTISGARH': 'Central',
    'MAHARASHTRA': 'Western',
    'RAJASTHAN': 'Western',
    'GUJARAT': 'Western',
    'GOA': 'Western',
    'DADRA & NAGAR HAVELI': 'Western',
    'DAMAN & DIU': 'Western',
    'TAMIL NADU': 'Southern',
    'ANDHRA PRADESH': 'Southern',
    'KERALA': 'Southern',
    'KARNATAKA': 'Southern',
    'TELANGANA': 'Southern',
    'PUDUCHERRY': 'Southern',
    'LAKSHADWEEP': 'Southern',
    'UTTAR PRADESH': 'Northern',
    'DELHI': 'Northern',
    'HARYANA': 'Northern',
    'PUNJAB': 'Northern',
    'JAMMU & KASHMIR': 'Northern',
    'HIMACHAL PRADESH': 'Northern',
    'UTTARAKHAND': 'Northern',
    'CHANDIGARH': 'Northern',
    'BIHAR': 'Eastern',
    'WEST BENGAL': 'Eastern',
    'ODISHA': 'Eastern',
    'JHARKHAND': 'Eastern',
    'ANDAMAN & NICOBAR ISLANDS': 'Eastern',
    'ASSAM': 'North-Eastern',
    'TRIPURA': 'North-Eastern',
    'MANIPUR': 'North-Eastern',
    'MIZORAM': 'North-Eastern',
    'ARUNACHAL PRADESH': 'North-Eastern',
    'MEGHALAYA': 'North-Eastern',
    'NAGALAND': 'North-Eastern',
    'SIKKIM': 'North-Eastern'
}

def generate_regions_data():
    feat_file = Path(HDFS_CRIMES_FEATURES) / "crimes_features_consolidated.csv"
    if not feat_file.exists():
        logger.error(f"Features file not found at {feat_file}")
        return None

    state_stats = {}
    region_stats = {
        "Central": {"region": "Central", "total_crimes": 0, "violent_crimes": 0, "property_crimes": 0, "women_crimes": 0, "states_count": 0},
        "Western": {"region": "Western", "total_crimes": 0, "violent_crimes": 0, "property_crimes": 0, "women_crimes": 0, "states_count": 0},
        "Southern": {"region": "Southern", "total_crimes": 0, "violent_crimes": 0, "property_crimes": 0, "women_crimes": 0, "states_count": 0},
        "Northern": {"region": "Northern", "total_crimes": 0, "violent_crimes": 0, "property_crimes": 0, "women_crimes": 0, "states_count": 0},
        "Eastern": {"region": "Eastern", "total_crimes": 0, "violent_crimes": 0, "property_crimes": 0, "women_crimes": 0, "states_count": 0},
        "North-Eastern": {"region": "North-Eastern", "total_crimes": 0, "violent_crimes": 0, "property_crimes": 0, "women_crimes": 0, "states_count": 0}
    }

    with open(feat_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            st = r["STATE_UT"].strip()
            st_key = st.upper()
            region = STATE_TO_REGION.get(st_key, "Northern")
            
            tot = int(r.get("TOTAL_IPC_CRIMES", 0))
            vio = int(r.get("VIOLENT_CRIMES", 0))
            prop = int(r.get("PROPERTY_CRIMES", 0))
            wom = int(r.get("WOMEN_CRIMES", 0))

            if st_key not in state_stats:
                state_stats[st_key] = {
                    "state": st,
                    "region": region,
                    "total_crimes": 0,
                    "violent_crimes": 0,
                    "property_crimes": 0,
                    "women_crimes": 0,
                    "violent_pct": 0.0
                }
            state_stats[st_key]["total_crimes"] += tot
            state_stats[st_key]["violent_crimes"] += vio
            state_stats[st_key]["property_crimes"] += prop
            state_stats[st_key]["women_crimes"] += wom

            region_stats[region]["total_crimes"] += tot
            region_stats[region]["violent_crimes"] += vio
            region_stats[region]["property_crimes"] += prop
            region_stats[region]["women_crimes"] += wom

    # Compute percentages & states count
    states_per_region = {}
    for st_key, d in state_stats.items():
        if d["total_crimes"] > 0:
            d["violent_pct"] = round(d["violent_crimes"] * 100.0 / d["total_crimes"], 1)
        reg = d["region"]
        states_per_region[reg] = states_per_region.get(reg, 0) + 1

    for reg, cnt in states_per_region.items():
        if reg in region_stats:
            region_stats[reg]["states_count"] = cnt

    # Build region_map mapping both uppercase and titlecase names to region
    region_map = {}
    for st_key, d in state_stats.items():
        st_name = d["state"]
        region = d["region"]
        region_map[st_key] = region
        region_map[st_name] = region
        region_map[st_name.upper()] = region
        region_map[st_name.title()] = region
    for k, v in STATE_TO_REGION.items():
        region_map[k] = v
        region_map[k.title()] = v

    # Sort states by total_crimes descending
    sorted_states = dict(sorted(state_stats.items(), key=lambda kv: kv[1]["total_crimes"], reverse=True))

    return {
        "regions": region_stats,
        "states": sorted_states,
        "region_map": region_map
    }

def sync_data():
    regions_payload = generate_regions_data()
    if regions_payload:
        out_regions = ANALYSIS_RESULTS_DIR / "regions_data.json"
        with open(out_regions, "w", encoding="utf-8") as f:
            json.dump(regions_payload, f, indent=2)
        logger.info(f"Generated fresh {out_regions}")

    target_dirs = [
        PROJECT_ROOT / "react-dashboard",
        PROJECT_ROOT / "react-dashboard" / "public",
        PROJECT_ROOT / "react-dashboard" / "src" / "data"
    ]

    files_to_sync = [
        "hotspots.json",
        "regions_data.json",
        "time_patterns.json",
        "crime_trends.json",
        "model_evaluation.json",
        "cluster_zones.json"
    ]

    for fname in files_to_sync:
        src = ANALYSIS_RESULTS_DIR / fname
        if not src.exists():
            continue
        for tdir in target_dirs:
            tdir.mkdir(parents=True, exist_ok=True)
            dst = tdir / fname
            shutil.copy2(src, dst)
            logger.info(f"Synced {fname} -> {dst}")

    logger.info("=== All Analytical Datasets Successfully Synchronized ===")

if __name__ == "__main__":
    sync_data()
