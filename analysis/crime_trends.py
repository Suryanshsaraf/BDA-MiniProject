"""
Module 5C — Crime Trends Analysis: Multi-Dataset Integration & Economic Impact.

This PySpark analysis job integrates:
1. Long-term IPC crime trends (2001–2014).
2. Property Stolen vs Recovered financial trends (₹ Crores) from NCRB Property dataset.
3. Crimes Against Women trajectory & state safety rankings.
4. Outputs pre-computed JSON to /data/analysis_results/crime_trends.json.
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
    RAW_DATA_DIR,
    HDFS_CRIMES_FEATURES,
    ANALYSIS_RESULTS_DIR,
    setup_logging
)

logger = setup_logging("CrimeTrendsAnalysis")


def analyze_property_recovery():
    """
    Parse 10_Property_stolen_and_recovered.csv to compute financial recovery metrics.
    
    Returns:
        list: Year-wise property stolen and recovered metrics in INR Crores.
    """
    prop_file = RAW_DATA_DIR / "10_Property_stolen_and_recovered.csv"
    if not prop_file.exists():
        logger.warning(f"{prop_file} not found.")
        return []

    yearly_prop = {}
    with open(prop_file, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                yr_val = row.get("Year") or row.get("YEAR")
                if not yr_val:
                    continue
                yr = int(yr_val.strip())
            except ValueError:
                continue

            try:
                stolen_val = float(row.get("Value_of_Property_Stolen", 0) or 0)
                rec_val = float(row.get("Value_of_Property_Recovered", 0) or 0)
                stolen_cases = int(float(row.get("Cases_Property_Stolen", 0) or 0))
                rec_cases = int(float(row.get("Cases_Property_Recovered", 0) or 0))
            except (ValueError, TypeError):
                continue

            if yr not in yearly_prop:
                yearly_prop[yr] = {
                    "year": yr,
                    "value_stolen_inr": 0.0,
                    "value_recovered_inr": 0.0,
                    "cases_stolen": 0,
                    "cases_recovered": 0
                }
            yearly_prop[yr]["value_stolen_inr"] += stolen_val
            yearly_prop[yr]["value_recovered_inr"] += rec_val
            yearly_prop[yr]["cases_stolen"] += stolen_cases
            yearly_prop[yr]["cases_recovered"] += rec_cases

    results = []
    for yr in sorted(yearly_prop.keys()):
        d = yearly_prop[yr]
        # Convert to ₹ Crores (1 Crore = 10,000,000 INR)
        stolen_cr = round(d["value_stolen_inr"] / 10000000.0, 2)
        rec_cr = round(d["value_recovered_inr"] / 10000000.0, 2)
        rec_rate = round((d["value_recovered_inr"] / d["value_stolen_inr"] * 100), 2) if d["value_stolen_inr"] > 0 else 0.0
        case_rec_rate = round((d["cases_recovered"] / d["cases_stolen"] * 100), 2) if d["cases_stolen"] > 0 else 0.0

        results.append({
            "year": yr,
            "stolen_inr_crores": stolen_cr,
            "recovered_inr_crores": rec_cr,
            "financial_recovery_rate_pct": rec_rate,
            "cases_stolen": d["cases_stolen"],
            "cases_recovered": d["cases_recovered"],
            "case_recovery_rate_pct": case_rec_rate
        })
    return results


def analyze_women_crimes():
    """
    Parse 42_District_wise_crimes_committed_against_women_2001_2012.csv for state totals.
    
    Returns:
        dict: State-wise summary of crimes against women.
    """
    women_file = RAW_DATA_DIR / "42_District_wise_crimes_committed_against_women_2001_2012.csv"
    if not women_file.exists():
        logger.warning(f"{women_file} not found.")
        return {}

    state_totals = {}
    with open(women_file, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for r in reader:
            state = r.get("STATE/UT") or r.get("State/UT") or "UNKNOWN"
            state = state.strip().title()

            try:
                rape = int(float(r.get("Rape", 0) or 0))
                kidnap = int(float(r.get("Kidnapping and Abduction", 0) or 0))
                dowry = int(float(r.get("Dowry Deaths", 0) or 0))
                assault = int(float(r.get("Assault on women with intent to outrage her modesty", 0) or 0))
                cruelty = int(float(r.get("Cruelty by Husband or his Relatives", 0) or 0))
            except (ValueError, TypeError):
                continue

            tot = rape + kidnap + dowry + assault + cruelty
            if state not in state_totals:
                state_totals[state] = {
                    "state": state,
                    "rape": 0,
                    "kidnapping": 0,
                    "dowry_deaths": 0,
                    "assault": 0,
                    "cruelty_by_husband": 0,
                    "total_crimes_against_women": 0
                }
            state_totals[state]["rape"] += rape
            state_totals[state]["kidnapping"] += kidnap
            state_totals[state]["dowry_deaths"] += dowry
            state_totals[state]["assault"] += assault
            state_totals[state]["cruelty_by_husband"] += cruelty
            state_totals[state]["total_crimes_against_women"] += tot

    sorted_states = sorted(state_totals.values(), key=lambda x: x["total_crimes_against_women"], reverse=True)
    return sorted_states


def run_crime_trends_analysis():
    """Execute multi-dataset trend synthesis."""
    logger.info("=== Starting Crime Trends Analysis ===")
    
    # 1. Property Recovery Trends
    property_trends = analyze_property_recovery()
    logger.info(f"Processed property recovery trends across {len(property_trends)} years.")

    # 2. Women Crimes Trends
    women_trends = analyze_women_crimes()
    logger.info(f"Processed crimes against women trends across {len(women_trends)} states.")

    # 3. Overall IPC Category Trends
    feat_file = Path(HDFS_CRIMES_FEATURES) / "crimes_features_consolidated.csv"
    yearly_cat = {}
    if feat_file.exists():
        with open(feat_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for r in reader:
                yr = int(r["YEAR"])
                if yr not in yearly_cat:
                    yearly_cat[yr] = {
                        "year": yr,
                        "violent": 0,
                        "property": 0,
                        "women": 0,
                        "economic": 0,
                        "other": 0,
                        "total": 0
                    }
                yearly_cat[yr]["violent"] += int(r["VIOLENT_CRIMES"])
                yearly_cat[yr]["property"] += int(r["PROPERTY_CRIMES"])
                yearly_cat[yr]["women"] += int(r["WOMEN_CRIMES"])
                yearly_cat[yr]["economic"] += int(r["ECONOMIC_CRIMES"])
                yearly_cat[yr]["other"] += int(r["OTHER_CRIMES"])
                yearly_cat[yr]["total"] += int(r["TOTAL_IPC_CRIMES"])

    category_trends = sorted(yearly_cat.values(), key=lambda x: x["year"])

    results = {
        "category_trends": category_trends,
        "property_financial_trends": property_trends,
        "women_crimes_state_rankings": women_trends
    }

    out_json = ANALYSIS_RESULTS_DIR / "crime_trends.json"
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    logger.info(f"Saved crime trends results to {out_json}.")
    logger.info("=== Crime Trends Analysis Complete ===")
    return True


if __name__ == "__main__":
    run_crime_trends_analysis()
