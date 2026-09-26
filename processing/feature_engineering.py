"""
Module 3 — Feature Engineering: Feature Creation and Severity Indexing.

This PySpark feature engineering job calculates:
1. Category aggregates:
   - VIOLENT: Murder, Attempt to Murder, Culpable Homicide, Dacoity, Robbery, Hurt, Kidnapping.
   - PROPERTY: Burglary, Theft, Auto Theft.
   - WOMEN_CHILDREN: Rape, Dowry Deaths, Assault on Women, Cruelty by Husband, Insult.
   - ECONOMIC: Cheating, Arson.
   - OTHER: Residual IPC crimes.
2. Ratios & Indices:
   - violent_crime_ratio: Violent crimes / Total IPC crimes.
   - property_crime_ratio: Property crimes / Total IPC crimes.
   - women_crime_ratio: Crimes against women / Total IPC crimes.
   - district_risk: Historical average crime volume for the district.
   - state_risk: Average crime volume for the state.
3. Target Label:
   - high_severity_flag: 1 if a district's TOTAL_IPC_CRIMES is at/above the
     national 75th percentile for that year (i.e. a top-quartile crime-volume
     district), else 0. Defined purely from total volume, independent of the
     VIOLENT/PROPERTY/WOMEN/ECONOMIC crime ratios used as model features -
     this keeps the label and the predictors derived from disjoint signals,
     rather than a rule like "VIOLENT_CRIME_RATIO > 0.25" that would let the
     model trivially recover the label from one of its own input features.
4. Saves output dataset to HDFS at /data/crimes_features/ as Parquet.
"""

import sys
import os
import csv
import statistics
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import (
    HDFS_CRIMES_CLEAN,
    HDFS_CRIMES_FEATURES,
    SPARK_MASTER,
    SPARK_APP_NAME,
    setup_logging
)

logger = setup_logging("FeatureEngineering")


def load_auxiliary_data():
    """Load census demographics, police infrastructure, and cybercrime datasets if available."""
    from config import RAW_DATA_DIR
    demo_map = {}
    police_map = {}
    cyber_map = {}
    
    demo_file = RAW_DATA_DIR / "district_census_demographics.csv"
    if demo_file.exists():
        with open(demo_file, "r", encoding="utf-8", errors="replace") as f:
            for r in csv.DictReader(f):
                key = (r["STATE_UT"].strip().upper(), r["DISTRICT"].strip().upper())
                demo_map[key] = {
                    "pop": int(r.get("POPULATION", 1500000)),
                    "density": float(r.get("POPULATION_DENSITY", 500.0)),
                    "lit_rate": float(r.get("LITERACY_RATE", 74.0)),
                    "urban_pct": float(r.get("URBAN_PERCENTAGE", 30.0))
                }
                
    police_file = RAW_DATA_DIR / "police_infrastructure_and_justice.csv"
    if police_file.exists():
        with open(police_file, "r", encoding="utf-8", errors="replace") as f:
            for r in csv.DictReader(f):
                key = (r["STATE_UT"].strip().upper(), r["DISTRICT"].strip().upper())
                police_map[key] = {
                    "police_per_lakh": float(r.get("POLICE_PER_LAKH_POPULATION", 160.0)),
                    "chargesheet_rate": float(r.get("CHARGESHEET_RATE_PCT", 78.0)),
                    "conviction_rate": float(r.get("CONVICTION_RATE_PCT", 45.0))
                }

    cyber_file = RAW_DATA_DIR / "cybercrime_state_district_2017_2022.csv"
    if cyber_file.exists():
        with open(cyber_file, "r", encoding="utf-8", errors="replace") as f:
            for r in csv.DictReader(f):
                key = (r["STATE_UT"].strip().upper(), r["DISTRICT"].strip().upper(), int(r.get("YEAR", 0)))
                cyber_map[key] = int(r.get("TOTAL_CYBER_CRIMES", 0))

    return demo_map, police_map, cyber_map


def compute_features_in_memory(cleaned_records: list) -> list:
    """
    Compute domain features and indices for each district-year record.
    
    Args:
        cleaned_records (list): Cleaned records from Module 2.
        
    Returns:
        list: Enriched feature records list.
    """
    logger.info("Computing crime categories, ratios, and risk scores...")
    demo_map, police_map, cyber_map = load_auxiliary_data()
    
    # 1. First pass: Compute historical district/state crime volume, and the
    # national 75th-percentile TOTAL_IPC_CRIMES threshold per year (used for
    # an independent, non-leaky HIGH_SEVERITY_FLAG target below).
    district_totals = {}
    district_counts = {}
    state_totals = {}
    state_counts = {}
    year_totals = {}

    for r in cleaned_records:
        dist = r["DISTRICT"].upper()
        state = r["STATE_UT"].upper()
        year = int(r["YEAR"])
        total_ipc = int(r["TOTAL_IPC_CRIMES"])

        district_totals[dist] = district_totals.get(dist, 0) + total_ipc
        district_counts[dist] = district_counts.get(dist, 0) + 1

        state_totals[state] = state_totals.get(state, 0) + total_ipc
        state_counts[state] = state_counts.get(state, 0) + 1

        year_totals.setdefault(year, []).append(total_ipc)

    district_avg = {d: district_totals[d] / district_counts[d] for d in district_totals}
    state_avg = {s: state_totals[s] / state_counts[s] for s in state_totals}

    year_p75_threshold = {}
    for year, totals in year_totals.items():
        if len(totals) >= 4:
            year_p75_threshold[year] = statistics.quantiles(totals, n=4)[2]
        else:
            year_p75_threshold[year] = max(totals)

    # 2. Second pass: Calculate categorical breakdowns and target label
    feature_records = []
    for r in cleaned_records:
        dist = r["DISTRICT"].upper()
        state = r["STATE_UT"].upper()
        total_ipc = max(int(r["TOTAL_IPC_CRIMES"]), 1)

        violent_crime = (
            int(r.get("MURDER", 0)) + int(r.get("ATTEMPT_TO_MURDER", 0)) +
            int(r.get("CULPABLE_HOMICIDE", 0)) + int(r.get("DACOITY", 0)) +
            int(r.get("ROBBERY", 0)) + int(r.get("HURT", 0)) +
            int(r.get("KIDNAPPING_ABDUCTION", 0))
        )

        property_crime = (
            int(r.get("BURGLARY", 0)) + int(r.get("THEFT", 0)) +
            int(r.get("AUTO_THEFT", 0))
        )

        women_crime = (
            int(r.get("RAPE", 0)) + int(r.get("DOWRY_DEATHS", 0)) +
            int(r.get("ASSAULT_ON_WOMEN", 0)) + int(r.get("INSULT_TO_MODESTY_OF_WOMEN", 0)) +
            int(r.get("CRUELTY_BY_HUSBAND", 0))
        )

        economic_crime = (
            int(r.get("CHEATING", 0)) + int(r.get("ARSON", 0))
        )

        other_crime = max(0, total_ipc - (violent_crime + property_crime + women_crime + economic_crime))

        # Risk and Ratios
        v_ratio = round(violent_crime / total_ipc, 4)
        p_ratio = round(property_crime / total_ipc, 4)
        w_ratio = round(women_crime / total_ipc, 4)
        e_ratio = round(economic_crime / total_ipc, 4)

        dist_risk = round(district_avg.get(dist, 0), 2)
        st_risk = round(state_avg.get(state, 0), 2)

        # Auxiliary demographics and policing enrichment
        dist_key = (state, dist)
        demo = demo_map.get(dist_key, {"pop": 1500000, "density": 500.0, "lit_rate": 74.0, "urban_pct": 30.0})
        police = police_map.get(dist_key, {"police_per_lakh": 160.0, "chargesheet_rate": 78.0, "conviction_rate": 45.0})
        pop = max(demo["pop"], 50000)
        
        crime_rate_100k = round((total_ipc / pop) * 100000, 2)
        violent_rate_100k = round((violent_crime / pop) * 100000, 2)
        
        year = int(r["YEAR"])
        cyber_cases = cyber_map.get((state, dist, year), 0)

        # Target High Severity Flag: 1 if total_ipc >= 75th percentile for year
        high_severity = 1 if total_ipc >= year_p75_threshold.get(year, total_ipc) else 0

        feat_row = {
            "STATE_UT": r["STATE_UT"],
            "DISTRICT": r["DISTRICT"],
            "YEAR": year,
            "LATITUDE": float(r["LATITUDE"]),
            "LONGITUDE": float(r["LONGITUDE"]),
            "TOTAL_IPC_CRIMES": total_ipc,
            "VIOLENT_CRIMES": violent_crime,
            "PROPERTY_CRIMES": property_crime,
            "WOMEN_CRIMES": women_crime,
            "ECONOMIC_CRIMES": economic_crime,
            "OTHER_CRIMES": other_crime,
            "VIOLENT_CRIME_RATIO": v_ratio,
            "PROPERTY_CRIME_RATIO": p_ratio,
            "WOMEN_CRIME_RATIO": w_ratio,
            "ECONOMIC_CRIME_RATIO": e_ratio,
            "DISTRICT_RISK_SCORE": dist_risk,
            "STATE_RISK_SCORE": st_risk,
            "POPULATION": pop,
            "POPULATION_DENSITY": demo["density"],
            "LITERACY_RATE": demo["lit_rate"],
            "URBAN_PERCENTAGE": demo["urban_pct"],
            "CRIME_RATE_PER_100K": crime_rate_100k,
            "VIOLENT_RATE_PER_100K": violent_rate_100k,
            "POLICE_PER_LAKH": police["police_per_lakh"],
            "CHARGESHEET_RATE": police["chargesheet_rate"],
            "CONVICTION_RATE": police["conviction_rate"],
            "CYBER_CRIMES": cyber_cases,
            "HIGH_SEVERITY_FLAG": high_severity,
            "LOCATION_CLUSTER": 0  # To be updated by KMeans
        }
        feature_records.append(feat_row)

    logger.info(f"Engineered {len(feature_records)} feature vectors with {len(feature_records[0])} dimensions.")
    return feature_records


def run_spark_feature_engineering(clean_path: str, features_path: str) -> bool:
    """
    Run PySpark feature engineering if Spark cluster is available.
    
    Args:
        clean_path (str): Cleaned parquet source.
        features_path (str): Feature parquet target.
        
    Returns:
        bool: True if Spark execution succeeded.
    """
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, when, avg, round as spark_round, expr
        from pyspark.sql.window import Window

        logger.info("Executing PySpark feature engineering job...")
        spark = (
            SparkSession.builder
            .appName(f"{SPARK_APP_NAME}_Features")
            .master(SPARK_MASTER)
            .getOrCreate()
        )

        df = spark.read.parquet(clean_path)

        # Categorical aggregation
        df_feats = (
            df.withColumn(
                "VIOLENT_CRIMES",
                col("MURDER") + col("ATTEMPT_TO_MURDER") + col("CULPABLE_HOMICIDE") +
                col("DACOITY") + col("ROBBERY") + col("HURT") + col("KIDNAPPING_ABDUCTION")
            )
            .withColumn("PROPERTY_CRIMES", col("BURGLARY") + col("THEFT") + col("AUTO_THEFT"))
            .withColumn(
                "WOMEN_CRIMES",
                col("RAPE") + col("DOWRY_DEATHS") + col("ASSAULT_ON_WOMEN") +
                col("INSULT_TO_MODESTY_OF_WOMEN") + col("CRUELTY_BY_HUSBAND")
            )
            .withColumn("ECONOMIC_CRIMES", col("CHEATING") + col("ARSON"))
            .withColumn(
                "VIOLENT_CRIME_RATIO",
                spark_round(col("VIOLENT_CRIMES") / col("TOTAL_IPC_CRIMES"), 4)
            )
        )

        # High Severity Flag: top quartile of TOTAL_IPC_CRIMES nationally within
        # the same year - independent of the ratio features, to avoid label leakage.
        year_window = Window.partitionBy("YEAR")
        df_feats = (
            df_feats
            .withColumn("YEAR_P75_TOTAL", expr("percentile_approx(TOTAL_IPC_CRIMES, 0.75)").over(year_window))
            .withColumn(
                "HIGH_SEVERITY_FLAG",
                when(col("TOTAL_IPC_CRIMES") >= col("YEAR_P75_TOTAL"), 1).otherwise(0)
            )
            .drop("YEAR_P75_TOTAL")
        )

        # Window over District for historical risk
        dist_window = Window.partitionBy("DISTRICT")
        df_final = df_feats.withColumn("DISTRICT_RISK_SCORE", spark_round(avg("TOTAL_IPC_CRIMES").over(dist_window), 2))

        df_final.write.mode("overwrite").partitionBy("YEAR", "STATE_UT").parquet(features_path)
        logger.info("PySpark feature engineering finished and persisted.")
        spark.stop()
        return True
    except Exception as exc:
        logger.warning(f"PySpark feature job not executed: {exc}")
        return False


def run_feature_engineering():
    """Execute feature engineering pipeline."""
    logger.info("=== Starting Module 3: Feature Engineering ===")
    
    clean_file = Path(HDFS_CRIMES_CLEAN) / "crimes_clean_consolidated.csv"
    if not clean_file.exists():
        logger.error(f"{clean_file} not found. Please run cleaning first.")
        from processing.clean import run_cleaning
        run_cleaning()

    cleaned_records = []
    with open(clean_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        cleaned_records = list(reader)

    feature_records = compute_features_in_memory(cleaned_records)

    # Save to storage
    out_dir = Path(HDFS_CRIMES_FEATURES)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "crimes_features_consolidated.csv"

    fieldnames = list(feature_records[0].keys())
    with open(out_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(feature_records)

    logger.info(f"Saved {len(feature_records)} feature vectors to {out_file}.")

    # Try PySpark execution
    run_spark_feature_engineering(HDFS_CRIMES_CLEAN, HDFS_CRIMES_FEATURES)

    logger.info("=== Module 3: Feature Engineering Finished Successfully ===")
    return True


if __name__ == "__main__":
    run_feature_engineering()
