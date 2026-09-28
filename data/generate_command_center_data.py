"""
Generate high-speed analytical payloads for the React GIS Command Center:
1. yearly_state_snapshots.json (Year-by-year 2001-2022 timeline playback)
2. district_duel_data.json (Top 150 district DNA profiles for head-to-head comparison)
"""

import json
from pathlib import Path
import numpy as np
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
FEATURES_CSV = BASE_DIR / "data" / "crimes_features" / "crimes_features_consolidated.csv"
OUTPUT_DIR = BASE_DIR / "react-dashboard"


def generate_yearly_snapshots(df: pd.DataFrame):
    """Aggregate state-level and top-district stats for each year 2001-2022."""
    print("Generating yearly_state_snapshots.json...")
    snapshots = {}

    years = sorted(df["YEAR"].unique().tolist())
    for yr in years:
        sub = df[df["YEAR"] == yr]
        nat_total = int(sub["TOTAL_IPC_CRIMES"].sum())
        nat_violent = int(sub["VIOLENT_CRIMES"].sum())
        nat_women = int(sub["WOMEN_CRIMES"].sum())
        nat_violent_pct = round((nat_violent / max(nat_total, 1)) * 100, 1)

        # State aggregations
        state_aggs = (
            sub.groupby("STATE_UT")
            .agg(
                {
                    "TOTAL_IPC_CRIMES": "sum",
                    "VIOLENT_CRIMES": "sum",
                    "WOMEN_CRIMES": "sum",
                    "PROPERTY_CRIMES": "sum",
                }
            )
            .reset_index()
        )

        states_dict = {}
        for _, row in state_aggs.iterrows():
            st_name = str(row["STATE_UT"]).strip().upper()
            st_total = int(row["TOTAL_IPC_CRIMES"])
            st_violent = int(row["VIOLENT_CRIMES"])
            st_women = int(row["WOMEN_CRIMES"])
            st_prop = int(row["PROPERTY_CRIMES"])
            st_vpct = round((st_violent / max(st_total, 1)) * 100, 1)

            states_dict[st_name] = {
                "total_crimes": st_total,
                "violent_crimes": st_violent,
                "women_crimes": st_women,
                "property_crimes": st_prop,
                "violent_pct": st_vpct,
            }

        # Top 10 hotspot districts for this year
        top_districts = []
        top_df = sub.sort_values(by="TOTAL_IPC_CRIMES", ascending=False).head(10)
        for _, d in top_df.iterrows():
            top_districts.append(
                {
                    "district": str(d["DISTRICT"]),
                    "state": str(d["STATE_UT"]),
                    "lat": round(float(d["LATITUDE"]), 4),
                    "lon": round(float(d["LONGITUDE"]), 4),
                    "total_crimes": int(d["TOTAL_IPC_CRIMES"]),
                    "violent_crimes": int(d["VIOLENT_CRIMES"]),
                    "women_crimes": int(d["WOMEN_CRIMES"]),
                    "violent_pct": round(float(d["VIOLENT_CRIME_RATIO"]) * 100, 1),
                    "risk_level": "CRITICAL"
                    if float(d["DISTRICT_RISK_SCORE"]) > 0.6
                    else "ELEVATED",
                }
            )

        snapshots[str(yr)] = {
            "year": int(yr),
            "national_total": nat_total,
            "national_violent": nat_violent,
            "national_women": nat_women,
            "national_violent_pct": nat_violent_pct,
            "states": states_dict,
            "top_districts": top_districts,
        }

    out_file = OUTPUT_DIR / "yearly_state_snapshots.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(snapshots, f)
    print(f"Saved {len(snapshots)} yearly snapshots to {out_file}")


def generate_district_duel_data(df: pd.DataFrame):
    """Generate rich DNA profiles for top 120 most prominent districts."""
    print("Generating district_duel_data.json...")

    # Group by DISTRICT and STATE_UT over all years
    latest_df = df[df["YEAR"] >= 2018]
    agg_df = (
        df.groupby(["STATE_UT", "DISTRICT"])
        .agg(
            {
                "TOTAL_IPC_CRIMES": "sum",
                "VIOLENT_CRIMES": "sum",
                "PROPERTY_CRIMES": "sum",
                "WOMEN_CRIMES": "sum",
                "ECONOMIC_CRIMES": "sum",
                "CYBER_CRIMES": "sum",
                "LATITUDE": "first",
                "LONGITUDE": "first",
                "POPULATION": "mean",
                "POLICE_PER_LAKH": "mean",
                "CONVICTION_RATE": "mean",
                "CHARGESHEET_RATE": "mean",
                "LITERACY_RATE": "mean",
                "URBAN_PERCENTAGE": "mean",
                "DISTRICT_RISK_SCORE": "mean",
            }
        )
        .reset_index()
    )

    # Sort by total crimes and pick top 150
    top_districts_df = agg_df.sort_values(
        by="TOTAL_IPC_CRIMES", ascending=False
    ).head(150)

    districts_list = []
    for _, row in top_districts_df.iterrows():
        total = int(row["TOTAL_IPC_CRIMES"])
        violent = int(row["VIOLENT_CRIMES"])
        property_c = int(row["PROPERTY_CRIMES"])
        women = int(row["WOMEN_CRIMES"])
        econ = int(row["ECONOMIC_CRIMES"])
        cyber = int(row["CYBER_CRIMES"]) if pd.notna(row["CYBER_CRIMES"]) else 0
        pop = int(row["POPULATION"]) if pd.notna(row["POPULATION"]) else 1500000

        v_pct = round((violent / max(total, 1)) * 100, 1)
        p_pct = round((property_c / max(total, 1)) * 100, 1)
        w_pct = round((women / max(total, 1)) * 100, 1)
        e_pct = round((econ / max(total, 1)) * 100, 1)
        c_pct = round((cyber / max(total, 1)) * 100, 2)

        # Crime Rate per 100k
        rate_100k = round((total / max(pop, 100000)) * 100000, 1)

        # Determine Primary Crime Archetype
        if v_pct > 26:
            archetype = "Violent Hotspot Corridor"
        elif w_pct > 18:
            archetype = "High Women Safety Vulnerability"
        elif p_pct > 40:
            archetype = "High-Density Property Theft Hub"
        elif e_pct > 8 or c_pct > 1.5:
            archetype = "Commercial & Cyber Fraud Zone"
        else:
            archetype = "Mixed Urban IPC Density"

        # Composite Safety Score (0-100, 100 being safest)
        risk = float(row["DISTRICT_RISK_SCORE"])
        conviction = (
            float(row["CONVICTION_RATE"])
            if pd.notna(row["CONVICTION_RATE"])
            else 45.0
        )
        police_strength = (
            float(row["POLICE_PER_LAKH"])
            if pd.notna(row["POLICE_PER_LAKH"])
            else 120.0
        )

        safety_score = max(
            15.0,
            min(
                95.0,
                round(
                    100.0
                    - (risk * 50.0)
                    - (v_pct * 0.8)
                    + (conviction * 0.25)
                    + (min(police_strength, 200) * 0.05),
                    1,
                ),
            ),
        )

        districts_list.append(
            {
                "district": str(row["DISTRICT"]),
                "state": str(row["STATE_UT"]),
                "lat": round(float(row["LATITUDE"]), 4),
                "lon": round(float(row["LONGITUDE"]), 4),
                "population": pop,
                "total_crimes": total,
                "crime_rate_per_100k": rate_100k,
                "safety_score": safety_score,
                "archetype": archetype,
                "police_per_lakh": round(police_strength, 1),
                "conviction_rate": round(conviction, 1),
                "chargesheet_rate": round(
                    float(row["CHARGESHEET_RATE"])
                    if pd.notna(row["CHARGESHEET_RATE"])
                    else 78.0,
                    1,
                ),
                "literacy_rate": round(
                    float(row["LITERACY_RATE"])
                    if pd.notna(row["LITERACY_RATE"])
                    else 74.0,
                    1,
                ),
                "urban_pct": round(
                    float(row["URBAN_PERCENTAGE"])
                    if pd.notna(row["URBAN_PERCENTAGE"])
                    else 35.0,
                    1,
                ),
                "dna": {
                    "violent_pct": v_pct,
                    "property_pct": p_pct,
                    "women_pct": w_pct,
                    "economic_pct": e_pct,
                    "cyber_pct": c_pct,
                    "other_pct": round(
                        max(0, 100 - (v_pct + p_pct + w_pct + e_pct)), 1
                    ),
                },
            }
        )

    out_file = OUTPUT_DIR / "district_duel_data.json"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump({"districts": districts_list}, f)
    print(f"Saved {len(districts_list)} district DNA profiles to {out_file}")


def main():
    if not FEATURES_CSV.exists():
        print(f"Error: {FEATURES_CSV} not found!")
        return

    df = pd.read_csv(FEATURES_CSV)
    generate_yearly_snapshots(df)
    generate_district_duel_data(df)
    print("All Command Center data generated successfully!")


if __name__ == "__main__":
    main()
