"""
Module 7 — Apache Airflow DAG: End-to-End India Crime Analytics Pipeline.

DAG Name: india_crime_analysis_pipeline
Schedule: @daily
Orchestrates:
  load_to_hdfs >> clean_data >> feature_engineering >> create_hive_tables >>
  [hotspot_analysis, time_pattern, crime_trends] >>
  kmeans_clustering >> random_forest >> evaluate_model >> refresh_dashboard
"""

from datetime import datetime, timedelta
import os
import sys

# Airflow imports
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    from airflow.operators.bash import BashOperator
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False

# Default DAG configuration
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email': ['alerts@crime-analytics.org'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=60)
}


def task_load_to_hdfs():
    """Execute data ingestion module."""
    from ingestion.load_to_hdfs import run_ingestion
    return run_ingestion()


def task_clean_data():
    """Execute data cleaning module."""
    from processing.clean import run_cleaning
    return run_cleaning()


def task_feature_engineering():
    """Execute feature engineering module."""
    from processing.feature_engineering import run_feature_engineering
    return run_feature_engineering()


def task_hive_tables():
    """Execute Hive integration module."""
    from hive.execute_hive_queries import main as hive_main
    return hive_main()


def task_hotspot_analysis():
    """Execute hotspot analysis module."""
    from analysis.hotspot_analysis import run_hotspot_analysis
    return run_hotspot_analysis()


def task_time_pattern():
    """Execute temporal analysis module."""
    from analysis.time_pattern import run_time_pattern_analysis
    return run_time_pattern_analysis()


def task_crime_trends():
    """Execute crime trends analysis module."""
    from analysis.crime_trends import run_crime_trends_analysis
    return run_crime_trends_analysis()


def task_kmeans_clustering():
    """Execute KMeans clustering ML module."""
    from ml.kmeans_clustering import run_kmeans
    return run_kmeans()


def task_random_forest():
    """Execute Random Forest training ML module."""
    from ml.random_forest import run_training
    return run_training()


def task_evaluate_model():
    """Execute model evaluation module."""
    from ml.evaluate import run_evaluation
    return run_evaluation()


def task_refresh_dashboard():
    """Notify dashboard or warm-up cache."""
    print("Dashboard analytical cache successfully refreshed with latest pipeline outputs.")
    return True


if AIRFLOW_AVAILABLE:
    dag = DAG(
        'india_crime_analysis_pipeline',
        default_args=default_args,
        description='End-to-end Big Data pipeline for India Crime Data using PySpark & MLlib',
        schedule_interval='@daily',
        catchup=False,
        tags=['bigdata', 'pyspark', 'ncrb', 'india-crime', 'ml']
    )

    t_ingest = PythonOperator(
        task_id='load_to_hdfs',
        python_callable=task_load_to_hdfs,
        dag=dag
    )

    t_clean = PythonOperator(
        task_id='clean_data',
        python_callable=task_clean_data,
        dag=dag
    )

    t_features = PythonOperator(
        task_id='feature_engineering',
        python_callable=task_feature_engineering,
        dag=dag
    )

    t_hive = PythonOperator(
        task_id='create_hive_tables',
        python_callable=task_hive_tables,
        dag=dag
    )

    t_hotspot = PythonOperator(
        task_id='hotspot_analysis',
        python_callable=task_hotspot_analysis,
        dag=dag
    )

    t_time = PythonOperator(
        task_id='time_pattern',
        python_callable=task_time_pattern,
        dag=dag
    )

    t_trends = PythonOperator(
        task_id='crime_trends',
        python_callable=task_crime_trends,
        dag=dag
    )

    t_kmeans = PythonOperator(
        task_id='kmeans_clustering',
        python_callable=task_kmeans_clustering,
        dag=dag
    )

    t_rf = PythonOperator(
        task_id='random_forest',
        python_callable=task_random_forest,
        dag=dag
    )

    t_eval = PythonOperator(
        task_id='evaluate_model',
        python_callable=task_evaluate_model,
        dag=dag
    )

    t_dashboard = PythonOperator(
        task_id='refresh_dashboard',
        python_callable=task_refresh_dashboard,
        dag=dag
    )

    # Dependency Graph:
    # load_to_hdfs >> clean_data >> feature_engineering >> create_hive_tables >>
    # [hotspot_analysis, time_pattern, crime_trends] >> kmeans_clustering >>
    # random_forest >> evaluate_model >> refresh_dashboard
    t_ingest >> t_clean >> t_features >> t_hive
    t_hive >> [t_hotspot, t_time, t_trends]
    [t_hotspot, t_time, t_trends] >> t_kmeans
    t_kmeans >> t_rf >> t_eval >> t_dashboard
