"""
Hive Query Executor: Runs Analytical SQL Queries via PySpark spark.sql().

Executes external Hive table analytical queries against HDFS Parquet data,
displaying results in tabulated format with local fallback for verification.
"""

import sys
import os
import csv
import sqlite3
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import (
    HDFS_CRIMES_FEATURES,
    SPARK_MASTER,
    SPARK_APP_NAME,
    setup_logging
)

logger = setup_logging("HiveQueryExecutor")


def run_queries_spark():
    """Execute analytical Hive queries using PySpark spark.sql()."""
    try:
        from pyspark.sql import SparkSession
        logger.info("Connecting to PySpark with Hive support...")
        spark = (
            SparkSession.builder
            .appName(f"{SPARK_APP_NAME}_HiveQueries")
            .master(SPARK_MASTER)
            .enableHiveSupport()
            .getOrCreate()
        )

        logger.info("Loading external table 'crimes_features'...")
        df = spark.read.parquet(HDFS_CRIMES_FEATURES)
        df.createOrReplaceTempView("crimes_features")

        # Query 1
        print("\n--- HIVE QUERY 1: TOP 10 HIGH CRIME DISTRICTS ---")
        q1 = """
        SELECT STATE_UT, DISTRICT, 
               SUM(TOTAL_IPC_CRIMES) AS total_crimes,
               SUM(VIOLENT_CRIMES) AS total_violent,
               ROUND(SUM(VIOLENT_CRIMES) * 100.0 / SUM(TOTAL_IPC_CRIMES), 2) AS violent_pct
        FROM crimes_features
        GROUP BY STATE_UT, DISTRICT
        ORDER BY total_crimes DESC
        LIMIT 10
        """
        spark.sql(q1).show(truncate=False)

        # Query 2
        print("\n--- HIVE QUERY 2: YEAR-WISE NATIONAL CRIME TRENDS ---")
        q2 = """
        SELECT YEAR,
               SUM(TOTAL_IPC_CRIMES) AS national_total,
               SUM(VIOLENT_CRIMES) AS violent_total,
               SUM(PROPERTY_CRIMES) AS property_total,
               SUM(WOMEN_CRIMES) AS women_crimes_total
        FROM crimes_features
        GROUP BY YEAR
        ORDER BY YEAR ASC
        """
        spark.sql(q2).show(truncate=False)

        spark.stop()
        return True
    except Exception as exc:
        logger.warning(f"PySpark Hive execution not available: {exc}")
        return False


def run_queries_fallback():
    """Execute analytical queries locally using SQLite in-memory engine."""
    logger.info("Running Hive analytical queries via SQLite engine...")
    feat_file = Path(HDFS_CRIMES_FEATURES) / "crimes_features_consolidated.csv"
    if not feat_file.exists():
        logger.error(f"{feat_file} does not exist.")
        return

    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()

    with open(feat_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fields = reader.fieldnames
        cols_sql = ", ".join([f'"{col}" {"INTEGER" if "CRIME" in col or "YEAR" in col or "FLAG" in col else "REAL" if "RATIO" in col or "SCORE" in col or "LAT" in col or "LON" in col else "TEXT"}' for col in fields])
        cursor.execute(f"CREATE TABLE crimes_features ({cols_sql})")

        placeholders = ", ".join(["?"] * len(fields))
        for row in reader:
            cursor.execute(f"INSERT INTO crimes_features VALUES ({placeholders})", [row[c] for c in fields])

    conn.commit()

    print("\n===================================================================")
    print(" HIVE QUERY 1: TOP 10 HIGH CRIME DISTRICTS IN INDIA")
    print("===================================================================")
    q1 = """
    SELECT STATE_UT, DISTRICT, 
           SUM(TOTAL_IPC_CRIMES) AS total_crimes,
           SUM(VIOLENT_CRIMES) AS total_violent,
           ROUND(SUM(VIOLENT_CRIMES) * 100.0 / SUM(TOTAL_IPC_CRIMES), 2) AS violent_pct
    FROM crimes_features
    GROUP BY STATE_UT, DISTRICT
    ORDER BY total_crimes DESC
    LIMIT 10;
    """
    for r in cursor.execute(q1).fetchall():
        print(f"State: {r[0]:<20} | District: {r[1]:<22} | Total Crimes: {r[2]:<8} | Violent: {r[3]:<7} ({r[4]}%)")

    print("\n===================================================================")
    print(" HIVE QUERY 2: YEAR-WISE NATIONAL CRIME VOLUME (2001-2014)")
    print("===================================================================")
    q2 = """
    SELECT YEAR,
           SUM(TOTAL_IPC_CRIMES) AS national_total,
           SUM(VIOLENT_CRIMES) AS violent_total,
           SUM(PROPERTY_CRIMES) AS property_total,
           SUM(WOMEN_CRIMES) AS women_crimes_total
    FROM crimes_features
    GROUP BY YEAR
    ORDER BY YEAR ASC;
    """
    for r in cursor.execute(q2).fetchall():
        print(f"Year {r[0]}: Total: {r[1]:<10} | Violent: {r[2]:<8} | Property: {r[3]:<8} | Against Women: {r[4]}")

    conn.close()


def main():
    """Execute queries using best available engine."""
    logger.info("=== Starting Module 4: Hive Analytical Queries ===")
    if not run_queries_spark():
        run_queries_fallback()
    logger.info("=== Module 4: Hive Analytical Queries Complete ===")


if __name__ == "__main__":
    main()
