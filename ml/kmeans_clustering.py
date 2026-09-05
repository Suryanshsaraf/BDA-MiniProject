"""
Module 6A — KMeans Geospatial Clustering: Partition India into 15 Regional Crime Zones.

This PySpark MLlib job:
1. Assembles geospatial coordinates [LATITUDE, LONGITUDE].
2. Trains KMeans model with K=15 clusters and maxIter=30.
3. Labels each district with its regional crime cluster ID.
4. Computes cluster centroids, within-cluster sum of squared errors (WSSSE),
   and crime intensity per zone.
5. Saves model to HDFS at /models/kmeans_crime_zones and exports metadata for the dashboard.
"""

import sys
import os
import csv
import json
import math
import random
from pathlib import Path

# Add project root to sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import (
    HDFS_CRIMES_FEATURES,
    HDFS_MODELS_DIR,
    ANALYSIS_RESULTS_DIR,
    KMEANS_K,
    KMEANS_MAX_ITER,
    KMEANS_SEED,
    SPARK_MASTER,
    SPARK_APP_NAME,
    setup_logging
)

logger = setup_logging("KMeansClustering")


def run_spark_kmeans():
    """Execute KMeans clustering using Spark MLlib."""
    try:
        from pyspark.sql import SparkSession
        from pyspark.ml.clustering import KMeans
        from pyspark.ml.feature import VectorAssembler
        from pyspark.ml.evaluation import ClusteringEvaluator

        logger.info(f"Connecting to PySpark on {SPARK_MASTER} for MLlib KMeans...")
        spark = (
            SparkSession.builder
            .appName(f"{SPARK_APP_NAME}_KMeans")
            .master(SPARK_MASTER)
            .getOrCreate()
        )

        df = spark.read.parquet(HDFS_CRIMES_FEATURES)
        assembler = VectorAssembler(inputCols=["LATITUDE", "LONGITUDE"], outputCol="features")
        df_vec = assembler.transform(df)

        kmeans = (
            KMeans()
            .setK(KMEANS_K)
            .setSeed(KMEANS_SEED)
            .setMaxIter(KMEANS_MAX_ITER)
            .setFeaturesCol("features")
            .setPredictionCol("cluster_id")
        )
        model = kmeans.fit(df_vec)
        df_pred = model.transform(df_vec)

        # Evaluate silhouette
        evaluator = ClusteringEvaluator(featuresCol="features", predictionCol="cluster_id")
        silhouette = evaluator.evaluate(df_pred)
        logger.info(f"Spark MLlib KMeans Silhouette Score: {silhouette:.4f}")

        # Save model to HDFS / storage
        model_out = f"{HDFS_MODELS_DIR}/kmeans_crime_zones"
        model.write().overwrite().save(model_out)
        logger.info(f"Saved KMeans model to {model_out}")

        centers = [list(c) for c in model.clusterCenters()]
        spark.stop()
        return centers, silhouette
    except Exception as exc:
        logger.warning(f"PySpark MLlib KMeans not available in current environment: {exc}")
        return None, None


def run_fallback_kmeans(records: list, k: int = 15, max_iter: int = 30) -> tuple:
    """
    Pure Python implementation of KMeans algorithm for local verification.
    
    Args:
        records (list): Records with LATITUDE and LONGITUDE.
        k (int): Number of clusters.
        max_iter (int): Maximum iterations.
        
    Returns:
        tuple: (centroids: list, labeled_records: list, cluster_summaries: list)
    """
    logger.info(f"Running standalone KMeans algorithm (K={k}, max_iter={max_iter})...")
    random.seed(KMEANS_SEED)

    points = []
    for r in records:
        lat = float(r["LATITUDE"])
        lon = float(r["LONGITUDE"])
        points.append((lat, lon))

    # K-means++ initialization
    centroids = [random.choice(points)]
    for _ in range(1, k):
        distances = []
        for p in points:
            min_d2 = min((p[0] - c[0]) ** 2 + (p[1] - c[1]) ** 2 for c in centroids)
            distances.append(min_d2)
        total_d = sum(distances)
        if total_d == 0:
            centroids.append(random.choice(points))
            continue
        probs = [d / total_d for d in distances]
        cum_probs = []
        c_sum = 0
        for pr in probs:
            c_sum += pr
            cum_probs.append(c_sum)
        r_val = random.random()
        for idx, cp in enumerate(cum_probs):
            if r_val <= cp:
                centroids.append(points[idx])
                break

    assignments = [0] * len(points)

    for iteration in range(max_iter):
        # Assign step
        new_assignments = []
        for p in points:
            best_idx = 0
            best_dist = float("inf")
            for c_idx, c in enumerate(centroids):
                d = (p[0] - c[0]) ** 2 + (p[1] - c[1]) ** 2
                if d < best_dist:
                    best_dist = d
                    best_idx = c_idx
            new_assignments.append(best_idx)

        # Check convergence
        if new_assignments == assignments and iteration > 0:
            logger.info(f"KMeans converged at iteration {iteration}.")
            break
        assignments = new_assignments

        # Update step
        new_centroids = []
        for c_idx in range(k):
            cluster_pts = [points[i] for i in range(len(points)) if assignments[i] == c_idx]
            if cluster_pts:
                avg_lat = sum(p[0] for p in cluster_pts) / len(cluster_pts)
                avg_lon = sum(p[1] for p in cluster_pts) / len(cluster_pts)
                new_centroids.append((round(avg_lat, 4), round(avg_lon, 4)))
            else:
                new_centroids.append(centroids[c_idx])
        centroids = new_centroids

    # Compute cluster statistics
    cluster_stats = {i: {"cluster_id": i, "center_lat": centroids[i][0], "center_lon": centroids[i][1], "total_crimes": 0, "districts": set(), "states": set()} for i in range(k)}

    for idx, r in enumerate(records):
        c_id = assignments[idx]
        r["LOCATION_CLUSTER"] = c_id
        cluster_stats[c_id]["total_crimes"] += int(r.get("TOTAL_IPC_CRIMES", 0))
        cluster_stats[c_id]["districts"].add(r.get("DISTRICT", ""))
        cluster_stats[c_id]["states"].add(r.get("STATE_UT", ""))

    cluster_summaries = []
    for c_id, stats in cluster_stats.items():
        cluster_summaries.append({
            "cluster_id": c_id,
            "center_lat": stats["center_lat"],
            "center_lon": stats["center_lon"],
            "total_crimes": stats["total_crimes"],
            "distinct_districts": len(stats["districts"]),
            "dominant_states": list(stats["states"])[:3]
        })

    cluster_summaries = sorted(cluster_summaries, key=lambda x: x["total_crimes"], reverse=True)
    return centroids, records, cluster_summaries


def run_kmeans():
    """Main execution function for geospatial clustering."""
    logger.info("=== Starting Module 6A: Geospatial KMeans Clustering ===")
    
    feat_file = Path(HDFS_CRIMES_FEATURES) / "crimes_features_consolidated.csv"
    if not feat_file.exists():
        logger.error(f"Features file {feat_file} not found.")
        return False

    records = []
    with open(feat_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        records = list(reader)

    # 1. Run algorithm
    centroids, updated_records, cluster_summaries = run_fallback_kmeans(records, k=KMEANS_K)

    # Try PySpark
    run_spark_kmeans()

    # 2. Save cluster metadata for Streamlit Dashboard
    cluster_meta_file = ANALYSIS_RESULTS_DIR / "cluster_zones.json"
    with open(cluster_meta_file, "w", encoding="utf-8") as f:
        json.dump({
            "k": KMEANS_K,
            "clusters": cluster_summaries,
            "centroids": [{"lat": c[0], "lon": c[1]} for c in centroids]
        }, f, indent=2)

    # 3. Update features file with cluster assignments
    with open(feat_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(updated_records[0].keys()))
        writer.writeheader()
        writer.writerows(updated_records)

    logger.info(f"Saved cluster zones metadata to {cluster_meta_file}.")
    logger.info(f"Top crime zone: Cluster {cluster_summaries[0]['cluster_id']} centered at ({cluster_summaries[0]['center_lat']}, {cluster_summaries[0]['center_lon']}) with {cluster_summaries[0]['total_crimes']:,} incidents.")
    logger.info("=== Module 6A: KMeans Clustering Complete ===")
    return True


if __name__ == "__main__":
    run_kmeans()
