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
                       +--------------+---------------+  +---------+----------+
                                      |                            |
                                      |                            v
                                      |               +-------------------------+
                                      |               | kafka_consumer.py       |
                                      |               | live aggregation ->     |
                                      |               | live_stream_stats.json  |
                                      |               +-------------------------+
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
│   ├── kafka_producer.py         # Real-time crime incident stream generator
│   └── kafka_consumer.py         # Live stream aggregator (consumes 'live_crimes' topic)
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
├── react-dashboard/
│   ├── index.html                # Standalone React + Leaflet + Chart.js GIS dashboard (CDN-based, no build step)
│   └── serve.py / serve_dashboard.py  # Lightweight HTTP server for the React dashboard (port 3000)
└── tests/
    └── test_pipeline.py          # Automated unit & integration test suite
```

### Alternative Dashboard: React + Leaflet GIS Intelligence UI

Alongside the Streamlit dashboard, `react-dashboard/index.html` is a second, self-contained dashboard: a single-file React 18 + Leaflet + Chart.js app loaded via CDN scripts and in-browser Babel (no `npm install`/build step required). It reads the same pre-computed JSON outputs (`hotspots.json`, `regions_data.json`, `time_patterns.json`, `crime_trends.json`, `model_evaluation.json`, `india_states.json`) and adds a regional choropleth map, a district explorer/search table, and a client-side risk predictor. Because it must be fetched over HTTP (not opened via `file://`), serve it with:

```bash
python3 serve_dashboard.py
# Dashboard available at http://localhost:3000
```

Note: the React dashboard's risk predictor recomputes a client-side approximation using the trained model's feature-importance weights (JavaScript can't load a scikit-learn `.joblib` file in-browser) — for genuine live model inference (`.predict_proba()` on the real trained model), use the Streamlit dashboard's Page 5.

---

## Machine Learning Performance Benchmark

`ml/random_forest.py` trains a genuine **scikit-learn `RandomForestClassifier`** (100 trees, max depth 10) on 10,186 authentic district-year feature vectors across India (80/20 train/test split), and serializes the fitted model to `saved_models/rf_crime_model.joblib` via `joblib`. The Streamlit dashboard's Page 5 predictor loads this exact artifact and calls `.predict_proba()` for live inference — it is not a hardcoded formula. When PySpark is available, `run_spark_random_forest()` additionally trains an equivalent Spark MLlib model and persists it to HDFS; when neither scikit-learn nor PySpark is installed, a dependency-free correlation-heuristic scorer is used as a last-resort fallback (clearly labeled `model_type: "correlation_heuristic"` in the saved metadata).

**Target label (`HIGH_SEVERITY_FLAG`) is defined independently of the model's input features**, by design: `processing/feature_engineering.py` flags a district-year as high-severity when its `TOTAL_IPC_CRIMES` sits at/above the national 75th percentile for that year — a pure volume-based, peer-relative threshold. It deliberately does **not** use `VIOLENT_CRIME_RATIO` or any other ratio fed to the model, so the classifier can't trivially recover the label from one of its own inputs (an earlier version defined the label as `VIOLENT_CRIME_RATIO > 0.25`, while also using that same ratio as a feature — a leak that inflated accuracy to a hollow 96.7%).

| Metric | Random Forest Score | Benchmark Standard | Status |
| :--- | :--- | :--- | :--- |
| **Accuracy** | **93.96%** | > 65.0% | ✅ Passed |
| **AUC-ROC** | **0.9839** | > 0.850 | ✅ Passed |
| **Precision** | **90.51%** | > 60.0% | ✅ Passed |
| **Recall** | **84.62%** | > 85.0% | ⚠️ Marginal |
| **F1-Score** | **0.8746** | > 0.700 | ✅ Passed |

### Confusion Matrix (Test Set: 2,038 Districts)
```
                         | Predicted Low-Risk | Predicted High-Risk |
-------------------------+--------------------+---------------------+
  Actual Low-Risk        |       1,486        |          45         |
  Actual High-Risk       |         78         |         429         |
-------------------------+--------------------+---------------------+
```

### Feature Importance Ranking
1. **`DISTRICT_RISK_SCORE`** (84.29%) — A district's long-run average crime volume is by far the strongest predictor of whether it's *currently* in the high-volume tier (crime levels are highly persistent year-to-year).
2. **`VIOLENT_CRIME_RATIO`** (3.77%) — Composition still adds modest signal beyond raw historical volume.
3. **`PROPERTY_CRIME_RATIO`** (3.57%)
4. **`WOMEN_CRIME_RATIO`** (3.56%)
5. **`ECONOMIC_CRIME_RATIO`** (3.13%)
6. **`YEAR`** (1.69%) — Macro temporal shift.

Regenerate this benchmark at any time with `python3 processing/feature_engineering.py && python3 ml/kmeans_clustering.py && python3 ml/random_forest.py` (the first rewrites the label/feature file, the second re-clusters it, the third retrains and rewrites `data/analysis_results/model_evaluation.json` and `saved_models/rf_crime_model.joblib`).

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

Optionally, exercise the real-time streaming path (producer + consumer). Without a live Kafka broker, both scripts run in a deterministic offline fallback mode so the streaming logic can still be verified end-to-end:
```bash
python3 ingestion/kafka_producer.py    # publishes simulated incidents to 'live_crimes'
python3 ingestion/kafka_consumer.py    # aggregates them into data/analysis_results/live_stream_stats.json
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
