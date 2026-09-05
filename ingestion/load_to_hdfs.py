"""
Module 1 — Data Ingestion: Ingest Real NCRB Crime CSVs and Store as Parquet.

This job reads official NCRB district-wise crime datasets across 2001–2014,
unifies differing schemas across years, normalizes column names, and writes
optimized Parquet files partitioned by Year and State/UT to HDFS (or local fallback).
"""

import sys
import os
import csv
from pathlib import Path
from typing import Optional

# Setup import path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import (
    RAW_DATA_DIR,
    HDFS_CRIMES_RAW,
    SPARK_MASTER,
    SPARK_APP_NAME,
    SPARK_DRIVER_MEMORY,
    SPARK_EXECUTOR_MEMORY,
    setup_logging
)

logger = setup_logging("LoadToHDFS")

# Standardized Core Columns
CORE_COLUMNS = [
    "STATE_UT", "DISTRICT", "YEAR", "MURDER", "ATTEMPT_TO_MURDER",
    "CULPABLE_HOMICIDE", "RAPE", "KIDNAPPING_ABDUCTION", "DACOITY",
    "ROBBERY", "BURGLARY", "THEFT", "AUTO_THEFT", "RIOTS",
    "CHEATING", "ARSON", "HURT", "DOWRY_DEATHS", "ASSAULT_ON_WOMEN",
    "INSULT_TO_MODESTY_OF_WOMEN", "CRUELTY_BY_HUSBAND", "TOTAL_IPC_CRIMES"
]


def create_spark_session():
    """
    Initialize and configure an Apache Spark session with Hive and Parquet support.
    
    Returns:
        pyspark.sql.SparkSession or None if Spark cannot be initialized.
    """
    try:
        from pyspark.sql import SparkSession
        logger.info(f"Initializing Spark Session '{SPARK_APP_NAME}' on {SPARK_MASTER}...")
        spark = (
            SparkSession.builder
            .appName(SPARK_APP_NAME)
            .master(SPARK_MASTER)
            .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
            .config("spark.executor.memory", SPARK_EXECUTOR_MEMORY)
            .config("spark.sql.parquet.compression.codec", "snappy")
            .config("spark.sql.adaptive.enabled", "true")
            .getOrCreate()
        )
        logger.info("Spark session successfully created.")
        return spark
    except Exception as exc:
        logger.warning(f"PySpark initialization unavailable in current environment: {exc}")
        return None


def normalize_raw_csv(filepath: Path) -> list:
    """
    Read a raw NCRB CSV file, normalize column headers to standard names,
    and convert numeric fields.
    
    Args:
        filepath (Path): Absolute path to the raw CSV file.
        
    Returns:
        list: A list of dicts with standardized keys.
    """
    records = []
    if not filepath.exists():
        logger.error(f"Input file not found: {filepath}")
        return records

    logger.info(f"Reading and normalizing {filepath.name}...")
    with open(filepath, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        try:
            raw_headers = next(reader)
        except StopIteration:
            return records

        # Mapping variations of column names
        header_map = {}
        for idx, h in enumerate(raw_headers):
            clean_h = h.strip().upper().replace("/", "_").replace("&", "AND").replace(" ", "_")
            header_map[idx] = clean_h

        for row in reader:
            if not row or len(row) < 4:
                continue

            state = ""
            district = ""
            year = 0
            stats = {col: 0 for col in CORE_COLUMNS if col not in ("STATE_UT", "DISTRICT", "YEAR")}

            for idx, val in enumerate(row):
                if idx not in header_map:
                    continue
                col = header_map[idx]
                val_str = val.strip().replace(",", "")

                if col in ("STATE_UT", "STATES_UTS", "STATE"):
                    state = val_str.upper()
                elif col in ("DISTRICT", "DISTRICTS"):
                    district = val_str.upper()
                elif col in ("YEAR",):
                    try:
                        year = int(float(val_str))
                    except (ValueError, TypeError):
                        year = 0
                else:
                    # Map crime counts
                    num = 0
                    try:
                        num = int(float(val_str))
                    except (ValueError, TypeError):
                        num = 0

                    if "TOTAL" in col and "IPC" in col:
                        stats["TOTAL_IPC_CRIMES"] = max(stats["TOTAL_IPC_CRIMES"], num)
                    elif col.startswith("MURDER"):
                        stats["MURDER"] += num
                    elif "ATTEMPT" in col and "MURDER" in col:
                        stats["ATTEMPT_TO_MURDER"] += num
                    elif "CULPABLE_HOMICIDE" in col:
                        stats["CULPABLE_HOMICIDE"] += num
                    elif "RAPE" in col and "ATTEMPT" not in col and "CUSTODIAL" not in col:
                        stats["RAPE"] += num
                    elif "KIDNAPPING" in col:
                        stats["KIDNAPPING_ABDUCTION"] += num
                    elif "DACOITY" in col:
                        stats["DACOITY"] += num
                    elif "ROBBERY" in col:
                        stats["ROBBERY"] += num
                    elif "BURGLARY" in col:
                        stats["BURGLARY"] += num
                    elif "THEFT" in col:
                        stats["THEFT"] += num
                    elif "AUTO_THEFT" in col:
                        stats["AUTO_THEFT"] += num
                    elif "RIOTS" in col:
                        stats["RIOTS"] += num
                    elif "CHEATING" in col:
                        stats["CHEATING"] += num
                    elif "ARSON" in col:
                        stats["ARSON"] += num
                    elif "HURT" in col:
                        stats["HURT"] += num
                    elif "DOWRY" in col:
                        stats["DOWRY_DEATHS"] += num
                    elif "ASSAULT_ON_WOMEN" in col:
                        stats["ASSAULT_ON_WOMEN"] += num
                    elif "CRUELTY" in col and "HUSBAND" in col:
                        stats["CRUELTY_BY_HUSBAND"] += num

            if state and district and year > 0:
                record = {
                    "STATE_UT": state,
                    "DISTRICT": district,
                    "YEAR": year,
                    **stats
                }
                # If TOTAL_IPC_CRIMES wasn't explicitly present, calculate sum
                if record["TOTAL_IPC_CRIMES"] == 0:
                    record["TOTAL_IPC_CRIMES"] = sum(stats.values())
                records.append(record)

    logger.info(f"Loaded {len(records)} records from {filepath.name}.")
    return records


def ingest_data_pyspark(spark, all_records: list, output_path: str):
    """
    Ingest records into a PySpark DataFrame and persist as partitioned Parquet.
    
    Args:
        spark (SparkSession): Active Spark session.
        all_records (list): Consolidated record list.
        output_path (str): HDFS or local destination path.
    """
    logger.info("Creating PySpark DataFrame using DataFrame API...")
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType
    )

    schema = StructType([
        StructField("STATE_UT", StringType(), False),
        StructField("DISTRICT", StringType(), False),
        StructField("YEAR", IntegerType(), False),
        *[StructField(col, IntegerType(), True) for col in CORE_COLUMNS if col not in ("STATE_UT", "DISTRICT", "YEAR")]
    ])

    df = spark.createDataFrame(all_records, schema=schema)
    logger.info(f"PySpark DataFrame created with {df.count()} rows and {len(df.columns)} columns.")

    logger.info(f"Writing Parquet partitioned by (YEAR, STATE_UT) to: {output_path}...")
    (
        df.write
        .mode("overwrite")
        .partitionBy("YEAR", "STATE_UT")
        .parquet(output_path)
    )
    logger.info("Successfully persisted Parquet dataset to storage.")


def save_fallback_parquet(all_records: list, output_dir: Path):
    """
    Fallback method to persist data when PySpark / JVM is not directly available.
    
    Args:
        all_records (list): Consolidated records list.
        output_dir (Path): Local target directory.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_file = output_dir / "crimes_raw_consolidated.csv"
    logger.info(f"Saving standardized CSV to fallback path: {summary_file}...")
    
    with open(summary_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CORE_COLUMNS)
        writer.writeheader()
        writer.writerows(all_records)
    logger.info(f"Fallback write complete ({len(all_records)} records saved).")


def run_ingestion():
    """Execute the full data ingestion job."""
    logger.info("=== Starting Module 1: Data Ingestion (NCRB to Parquet/HDFS) ===")
    
    # 1. Gather all NCRB dataset files
    csv_files = [
        RAW_DATA_DIR / "01_District_wise_crimes_committed_IPC_2001_2012.csv",
        RAW_DATA_DIR / "01_District_wise_crimes_committed_IPC_2013.csv",
        RAW_DATA_DIR / "01_District_wise_crimes_committed_IPC_2014.csv"
    ]

    all_records = []
    for csv_file in csv_files:
        records = normalize_raw_csv(csv_file)
        all_records.extend(records)

    if not all_records:
        logger.error("No records loaded. Please ensure datasets are downloaded first.")
        return False

    logger.info(f"Total unified crime records across years: {len(all_records)}")

    # 2. Ingest via PySpark DataFrame API
    spark = create_spark_session()
    if spark:
        try:
            ingest_data_pyspark(spark, all_records, HDFS_CRIMES_RAW)
            spark.stop()
        except Exception as e:
            logger.warning(f"PySpark write encountered error: {e}. Falling back to standard format.")
            save_fallback_parquet(all_records, Path(HDFS_CRIMES_RAW))
    else:
        save_fallback_parquet(all_records, Path(HDFS_CRIMES_RAW))

    logger.info("=== Module 1: Data Ingestion Finished Successfully ===")
    return True


if __name__ == "__main__":
    run_ingestion()
