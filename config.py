"""
Centralized Configuration for India Crime Pattern Analysis & Prediction System.

This module defines all configurable constants, file paths, HDFS paths,
Kafka topics, Spark settings, ML hyperparameters, and logging utilities.
"""

import os
import logging
from pathlib import Path

# Base Paths
PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
ANALYSIS_RESULTS_DIR = DATA_DIR / "analysis_results"
LOCAL_MODELS_DIR = PROJECT_ROOT / "saved_models"

# Ensure essential local directories exist
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
ANALYSIS_RESULTS_DIR.mkdir(parents=True, exist_ok=True)
LOCAL_MODELS_DIR.mkdir(parents=True, exist_ok=True)

# HDFS Configurations
HDFS_ENABLED = os.environ.get("HDFS_ENABLED", "false").lower() == "true"
HDFS_NAMENODE = os.environ.get("HDFS_NAMENODE", "hdfs://namenode:9000")

def get_storage_path(relative_path: str) -> str:
    """
    Return HDFS path if HDFS_ENABLED is true, else fallback to local filesystem.
    
    Args:
        relative_path (str): Relative data path like '/data/crimes/'
        
    Returns:
        str: Fully qualified HDFS URI or local absolute path.
    """
    clean_path = relative_path.strip("/")
    if HDFS_ENABLED:
        return f"{HDFS_NAMENODE}/{clean_path}"
    # Strip leading data/ if present since DATA_DIR is already the data directory
    if clean_path.startswith("data/"):
        sub_path = clean_path[len("data/"):]
    else:
        sub_path = clean_path
    return str(DATA_DIR / sub_path)

HDFS_CRIMES_RAW = get_storage_path("/data/crimes")
HDFS_CRIMES_CLEAN = get_storage_path("/data/crimes_clean")
HDFS_CRIMES_FEATURES = get_storage_path("/data/crimes_features")
HDFS_MODELS_DIR = f"{HDFS_NAMENODE}/models" if HDFS_ENABLED else str(LOCAL_MODELS_DIR)

# Dataset URLs (NCRB Official District-Wise & State-Wise Data)
NCRB_BASE_URL = "https://raw.githubusercontent.com/aritra0309/hadoop-crime-project/main/data"
DATASET_FILES = {
    "ipc_2001_2012": "01_District_wise_crimes_committed_IPC_2001_2012.csv",
    "ipc_2013": "01_District_wise_crimes_committed_IPC_2013.csv",
    "ipc_2014": "01_District_wise_crimes_committed_IPC_2014.csv",
    "property": "10_Property_stolen_and_recovered.csv",
    "women_crimes": "42_District_wise_crimes_committed_against_women_2001_2012.csv",
    "india_geojson": "india_states.geojson"
}

# Kafka Streaming Settings
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "live_crimes")
KAFKA_RATE_LIMIT_PER_SEC = int(os.environ.get("KAFKA_RATE_LIMIT", "100"))

# Spark Settings
SPARK_MASTER = os.environ.get("SPARK_MASTER", "local[*]")
SPARK_APP_NAME = "IndiaCrimeAnalyticsPipeline"
SPARK_DRIVER_MEMORY = os.environ.get("SPARK_DRIVER_MEMORY", "2g")
SPARK_EXECUTOR_MEMORY = os.environ.get("SPARK_EXECUTOR_MEMORY", "2g")

# Machine Learning Hyperparameters
KMEANS_K = 15
KMEANS_SEED = 42
KMEANS_MAX_ITER = 30

RF_NUM_TREES = 100
RF_MAX_DEPTH = 10
RF_SEED = 42
TRAIN_TEST_SPLIT = [0.8, 0.2]

# Geospatial Settings (India Centroid)
INDIA_CENTER_LAT = 20.5937
INDIA_CENTER_LON = 78.9629
DEFAULT_MAP_ZOOM = 5

# Logging Setup
LOG_FORMAT = "%(asctime)s [%(levelname)s] [%(name)s]: %(message)s"
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

def setup_logging(logger_name: str) -> logging.Logger:
    """
    Configure and return a standardized logger instance.
    
    Args:
        logger_name (str): The name of the logger, typically __name__.
        
    Returns:
        logging.Logger: Configured logger.
    """
    logger = logging.getLogger(logger_name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(LOG_FORMAT)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
    return logger
