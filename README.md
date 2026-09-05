# India Crime Pattern Analysis & Prediction System using PySpark

[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.0-FDEE21?logo=apachespark&logoColor=black)](https://spark.apache.org/)
[![Hadoop HDFS](https://img.shields.io/badge/Hadoop%20HDFS-3.2.1-66CCFF?logo=apachehadoop&logoColor=black)](https://hadoop.apache.org/)
[![Apache Hive](https://img.shields.io/badge/Apache%20Hive-2.3.2-FDEE21?logo=apachehive&logoColor=black)](https://hive.apache.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-7.3.0-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.7.1-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.28-FF4B4B?logo=streamlit&logoColor=white)](https://streamlit.io/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

An end-to-end Big Data Analytics and Machine Learning pipeline that ingests, cleans, analyzes, clusters, and predicts crime patterns across India using **100% authentic National Crime Records Bureau (NCRB)** multi-year datasets (2001–2014) on **Apache PySpark**, **HDFS**, **Apache Hive**, **Spark MLlib**, and **Apache Airflow**, visualized through an interactive **Streamlit + Folium dashboard**.

---

## Architecture Diagram (ASCII)

```
====================================================================================================
                               INDIA CRIME BIG DATA PIPELINE ARCHITECTURE
====================================================================================================

               +-------------------------------------------------------------+
               |                  OFFICIAL NCRB DATASETS                     |
               |  - District IPC Crimes (2001-2014, 10,000+ records)         |
               |  - Property Stolen & Recovered (₹ Crores)                   |
               |  - Crimes Against Women (District & State level)            |
               |  - India States & District Geospatial Boundaries (GeoJSON)  |
               +------------------------------+------------------------------+
                                              |
                                              v
               +-------------------------------------------------------------+
               |               MODULE 1: INGESTION & STREAMING               |
               |  load_to_hdfs.py (PySpark)  |  kafka_producer.py (100/sec)  |
               +----------------------+----------------------+---------------+
                                      |                      |
                                      v                      v
                       +------------------------------+  +--------------------+
                       |  HDFS Parquet: /data/crimes  |  | Kafka: live_crimes |
                       +--------------+---------------+  +--------------------+
                                      |
                                      v
               +-------------------------------------------------------------+
               |                  MODULE 2: DATA CLEANING                    |
               |  clean.py (PySpark DataFrame API)                           |
               |  - Canonical State/District normalization (35 States/UTs)   |
               |  - Geospatial coordinate imputation from District Gazetteer |
               |  - Deduplication on (State, District, Year)                 |
               +------------------------------+------------------------------+
                                              |
                                              v
                       +--------------------------------------+
                       | HDFS Parquet: /data/crimes_clean     |
                       +----------------------+---------------+
                                              |
                                              v
               +-------------------------------------------------------------+
               |             MODULE 3: FEATURE ENGINEERING                   |
               |  feature_engineering.py                                     |
               |  - IPC Category Aggregations (Violent, Property, Women, Eco)|
               |  - Severity Ratios & Historical District Risk Scores        |
               |  - Target High Severity / Arrest Likelihood Label           |
               +------------------------------+------------------------------+
                                              |
                                              v
                       +--------------------------------------+
                       | HDFS Parquet: /data/crimes_features  |
                       +--------------+-----------------------+
                                      |
       +------------------------------+------------------------------+
       |                              |                              |
       v                              v                              v
+------------------+     +--------------------------+     +---------------------+
|  MODULE 4: HIVE  |     |   MODULE 5: ANALYTICS    |     |   MODULE 6: MLLIB   |
| create_tables.sql|     | hotspot_analysis.py      |     | kmeans_clustering.py|
| spark.sql()      |     | time_pattern.py          |     | random_forest.py    |
| - crimes_raw     |     | crime_trends.py          |     | evaluate.py         |
| - crimes_clean   |     +------------+-------------+     +----------+----------+
| - crimes_features|                  |                              |
+------------------+                  v                              v
                         +--------------------------+     +---------------------+
                         |  /data/analysis_results/ |     | HDFS /models/       |
                         |  - hotspots.json         |     | - kmeans_zones      |
                         |  - time_patterns.json    |     | - rf_crime_model    |
                         |  - crime_trends.json     |     +----------+----------+
                         |  - model_evaluation.json |                |
                         +------------+-------------+                |
                                      |                              |
                                      +--------------+---------------+
                                                     |
                                                     v
                                      +-------------------------------+
                                      |      MODULE 8: DASHBOARD      |
                                      | streamlit_app.py (5 Pages)    |
                                      | folium_map.py (Leaflet Heat)  |
                                      +---------------+---------------+
                                                      ^
                                                      |
                                      +---------------+---------------+
                                      |    MODULE 7: ORCHESTRATION    |
                                      | airflow/crime_pipeline_dag.py |
                                      | Schedule: @daily              |
                                      +-------------------------------+
```

---

## Dataset Description

The system processes real, official datasets from the **National Crime Records Bureau (NCRB), Ministry of Home Affairs, Government of India**:

1. **District-Wise Crimes Committed under IPC (2001–2012, 2013, 2014)**:
   - 10,000+ district-year records across 35 States & Union Territories.
   - 30+ IPC crime heads including Murder, Attempt to Murder, Rape, Kidnapping & Abduction, Dacoity, Robbery, Burglary, Theft, Riots, Cheating, Arson, Hurt, Dowry Deaths, Cruelty by Husband, and Total Cognizable IPC Crimes.
2. **Property Stolen and Recovered (`10_Property_stolen_and_recovered.csv`)**:
   - Financial valuation of stolen vs recovered property in **₹ Crores** and police case recovery rates.
3. **Crimes Against Women (`42_District_wise_crimes_committed_against_women_2001_2012.csv`)**:
   - Granular district statistics on women safety.
4. **India States & District Coordinates (`india_states.geojson` & `district_coordinates.json`)**:
   - Official polygon boundaries and centroid coordinates for geospatial hotspot mapping.

---

## Project Structure

```
crime-pattern-pyspark/
├── config.py                     # Central configuration (HDFS, Spark, Kafka, ML)
├── requirements.txt              # Production Python dependencies
├── docker-compose.yml             # Full 10-service Docker stack
├── hadoop.env                    # HDFS namenode & datanode environment variables
├── data/
│   ├── download_real_data.py     # Automated NCRB dataset downloader
│   ├── district_coordinates.json # Official district latitude/longitude gazetteer
│   ├── raw/                      # Downloaded NCRB CSVs & GeoJSON
│   ├── crimes/                   # Raw partitioned Parquet storage
│   ├── crimes_clean/             # Cleaned Parquet storage
│   ├── crimes_features/          # Engineered feature vectors
│   └── analysis_results/         # Pre-computed analytical summaries (JSON)
├── ingestion/
│   ├── load_to_hdfs.py           # PySpark ingestion & Parquet partitioner
│   └── kafka_producer.py         # Real-time crime incident stream generator
├── processing/
│   ├── clean.py                  # PySpark cleaning & coordinate imputation
│   └── feature_engineering.py    # Category grouping, risk scores & severity targets
├── hive/
│   ├── create_tables.sql         # Hive external DDL & analytical queries
│   └── execute_hive_queries.py   # Query execution via spark.sql()
├── analysis/
│   ├── hotspot_analysis.py       # District and state rankings & top 50 danger zones
│   ├── time_pattern.py           # Multi-year trajectories & 24x7 heatmap matrix
│   └── crime_trends.py           # Multi-dataset trend synthesis & financial impact
├── ml/
│   ├── kmeans_clustering.py      # Spatial clustering into 15 Indian crime corridors
│   ├── random_forest.py          # Random Forest Classifier (100 Trees, Depth 10)
│   └── evaluate.py               # Confusion matrix & feature importance evaluator
├── airflow/
│   └── crime_pipeline_dag.py     # Apache Airflow DAG (@daily schedule)
├── dashboard/
│   ├── streamlit_app.py          # 5-Page interactive Streamlit application
│   └── folium_map.py             # Interactive Folium Leaflet HeatMap & Marker Clusters
└── tests/
    └── test_pipeline.py          # Automated unit & integration test suite
```

---

## Machine Learning Performance Benchmark

Trained on 10,186 authentic district-year feature vectors across India (80/20 train/test split):

| Metric | Random Forest Score | Benchmark Standard | Status |
| :--- | :--- | :--- | :--- |
| **Accuracy** | **70.61%** | > 65.0% | ✅ Passed |
| **AUC-ROC** | **0.9365** | > 0.850 | ✅ Passed |
| **Precision** | **63.46%** | > 60.0% | ✅ Passed |
| **Recall** | **97.85%** | > 85.0% | ✅ Passed |
| **F1-Score** | **0.7699** | > 0.700 | ✅ Passed |

### Confusion Matrix (Test Set: 2,038 Districts)
```
                         | Predicted Low-Risk | Predicted High-Risk |
-------------------------+--------------------+---------------------+
  Actual Low-Risk        |        437         |         577         |
  Actual High-Risk       |         22         |        1,002        |
-------------------------+--------------------+---------------------+
```

### Feature Importance Ranking
1. **`VIOLENT_CRIME_RATIO`** (52.89%) — Primary indicator of danger severity.
2. **`DISTRICT_RISK_SCORE`** (21.67%) — Historical crime volume of the district.
3. **`PROPERTY_CRIME_RATIO`** (12.46%) — Frequency of burglaries and thefts.
4. **`WOMEN_CRIME_RATIO`** (8.69%) — Incidents of crimes against women.
5. **`ECONOMIC_CRIME_RATIO`** (2.72%) — Fraud, cheating, and arson.
6. **`YEAR`** (1.57%) — Macro temporal shift.

---

## Docker Compose Quickstart

The project includes a complete multi-container Big Data stack in `docker-compose.yml`:
- **HDFS**: NameNode (9870) & DataNode (9864)
- **Spark**: Master (8080/7077) & 2 Workers
- **Hive**: HiveServer2 (10000) & Metastore
- **Kafka**: Broker (9092) & ZooKeeper (2181)
- **Airflow**: Webserver (8085) & Scheduler
- **Streamlit**: Dashboard (8501)

### Start Services
```bash
docker compose up -d
```

### Service Web Interfaces:
- **Spark Master UI**: [http://localhost:8080](http://localhost:8080)
- **HDFS NameNode WebUI**: [http://localhost:9870](http://localhost:9870)
- **Airflow Web UI**: [http://localhost:8085](http://localhost:8085) *(Username: `admin`, Password: `admin`)*
- **Streamlit Dashboard**: [http://localhost:8501](http://localhost:8501)

---

## Local Standalone Execution Guide

All modules can also be run locally without Docker using the automated fallback pipeline:

### 1. Download Real NCRB Datasets
```bash
python3 data/download_real_data.py
```

### 2. Run Data Ingestion & Storage
```bash
python3 ingestion/load_to_hdfs.py
```

### 3. Run Data Cleaning & Coordinate Imputation
```bash
python3 processing/clean.py
```

### 4. Run Feature Engineering
```bash
python3 processing/feature_engineering.py
```

### 5. Execute Hive Analytical Queries
```bash
python3 hive/execute_hive_queries.py
```

### 6. Run Analytics Jobs
```bash
python3 analysis/hotspot_analysis.py
python3 analysis/time_pattern.py
python3 analysis/crime_trends.py
```

### 7. Train & Evaluate ML Models
```bash
python3 ml/kmeans_clustering.py
python3 ml/random_forest.py
python3 ml/evaluate.py
```

### 8. Run Automated Test Suite
```bash
python3 -m unittest tests/test_pipeline.py
```

### 9. Launch Streamlit Web Dashboard
```bash
streamlit run dashboard/streamlit_app.py
```

---

## Streamlit Dashboard Walkthrough (5 Pages)

1. **Page 1: National Overview**:
   - Total IPC Crimes KPI metric card (30.9M+ records).
   - Multi-year crime trajectory chart (2001–2014) across violent, property, and women crimes.
   - Top 10 high-crime States and Districts table.
2. **Page 2: India Hotspot Map**:
   - Interactive Folium Leaflet HeatMap centered on India (`[20.5937, 78.9629]`).
   - MarkerCluster layer highlighting top 50 high-crime danger corridors with rich popups.
   - Filter by risk tier (CRITICAL, HIGH) and minimum volume.
3. **Page 3: Time & Seasonal Patterns**:
   - 24×7 incident timing heatmap (Hour vs Day of week).
   - Indian seasonal breakdown (Monsoon, Summer, Winter, Festive).
4. **Page 4: Multi-Dataset Trends**:
   - Stacked category compositions.
   - Property Stolen vs. Recovered comparison in ₹ Crores.
5. **Page 5: Crime Severity & Risk Predictor**:
   - Real-time prediction form for any Indian district/state profile.
   - Displays "High Severity Alert ⚠️" vs "Moderate/Low Risk ✅" with confidence probability.

---

## Team Members
- **Suryansh Saraf** (Lead Engineer & Big Data Pipeline Developer)

---

## License
This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.
