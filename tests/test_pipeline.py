"""
Automated Unit and Integration Tests for India Crime Analytics Pipeline.

Covers:
  - Data normalization & coordinate imputation
  - Feature engineering & ratio mathematics
  - KMeans spatial clustering
  - Random Forest inference & probability bounds
"""

import sys
import os
import unittest
from pathlib import Path

# Setup import path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from processing.clean import normalize_state_name, get_coordinates, clean_records_in_memory
from processing.feature_engineering import compute_features_in_memory
from ml.kmeans_clustering import run_fallback_kmeans
from ml.random_forest import run_standalone_rf


class TestDataCleaning(unittest.TestCase):
    """Test data cleaning and normalization logic."""

    def test_state_normalization(self):
        """Verify canonical state mapping."""
        self.assertEqual(normalize_state_name("A & N ISLANDS"), "Andaman & Nicobar Islands")
        self.assertEqual(normalize_state_name("ORISSA"), "Odisha")
        self.assertEqual(normalize_state_name("DELHI UT"), "Delhi")
        self.assertEqual(normalize_state_name("MAHARASHTRA"), "Maharashtra")

    def test_coordinate_lookup(self):
        """Verify coordinates for known districts."""
        dist_map = {"PATNA": {"lat": 25.5941, "lon": 85.1376}}
        state_map = {"BIHAR": (25.0961, 85.3131)}
        
        lat, lon = get_coordinates("PATNA", "BIHAR", dist_map, state_map)
        self.assertAlmostEqual(lat, 25.5941, places=3)
        self.assertAlmostEqual(lon, 85.1376, places=3)

    def test_clean_records_deduplication(self):
        """Verify duplicate (state, district, year) rows are eliminated."""
        raw = [
            {"STATE_UT": "BIHAR", "DISTRICT": "PATNA", "YEAR": "2014", "TOTAL_IPC_CRIMES": "5000", "MURDER": "50"},
            {"STATE_UT": "BIHAR", "DISTRICT": "PATNA", "YEAR": "2014", "TOTAL_IPC_CRIMES": "5000", "MURDER": "50"},
            {"STATE_UT": "BIHAR", "DISTRICT": "TOTAL", "YEAR": "2014", "TOTAL_IPC_CRIMES": "100000"}  # should be dropped
        ]
        cleaned = clean_records_in_memory(raw)
        self.assertEqual(len(cleaned), 1)
        self.assertEqual(cleaned[0]["DISTRICT"], "Patna")


class TestFeatureEngineering(unittest.TestCase):
    """Test feature engineering calculations."""

    def test_crime_ratios(self):
        """Verify category sums and ratio calculations."""
        sample_cleaned = [
            {
                "STATE_UT": "Maharashtra",
                "DISTRICT": "Mumbai",
                "YEAR": 2014,
                "LATITUDE": 18.92,
                "LONGITUDE": 72.83,
                "MURDER": 100,
                "ATTEMPT_TO_MURDER": 100,
                "CULPABLE_HOMICIDE": 0,
                "DACOITY": 50,
                "ROBBERY": 150,
                "HURT": 600,
                "KIDNAPPING_ABDUCTION": 100,
                "BURGLARY": 400,
                "THEFT": 1000,
                "AUTO_THEFT": 600,
                "RAPE": 200,
                "DOWRY_DEATHS": 20,
                "ASSAULT_ON_WOMEN": 180,
                "INSULT_TO_MODESTY_OF_WOMEN": 50,
                "CRUELTY_BY_HUSBAND": 150,
                "CHEATING": 300,
                "ARSON": 100,
                "TOTAL_IPC_CRIMES": 4000
            }
        ]
        features = compute_features_in_memory(sample_cleaned)
        self.assertEqual(len(features), 1)
        feat = features[0]
        
        # Violent = 100+100+50+150+600+100 = 1100
        self.assertEqual(feat["VIOLENT_CRIMES"], 1100)
        # Property = 400+1000+600 = 2000
        self.assertEqual(feat["PROPERTY_CRIMES"], 2000)
        # Violent ratio = 1100 / 4000 = 0.275
        self.assertAlmostEqual(feat["VIOLENT_CRIME_RATIO"], 0.275, places=3)
        # High severity flag should be 1 since ratio > 0.25
        self.assertEqual(feat["HIGH_SEVERITY_FLAG"], 1)


class TestMachineLearning(unittest.TestCase):
    """Test ML algorithms and inference."""

    def test_kmeans_clustering(self):
        """Verify KMeans partitions points into K clusters."""
        sample_pts = [
            {"LATITUDE": 28.61, "LONGITUDE": 77.20, "DISTRICT": "DELHI", "STATE_UT": "DELHI", "TOTAL_IPC_CRIMES": "50000"},
            {"LATITUDE": 28.53, "LONGITUDE": 77.39, "DISTRICT": "NOIDA", "STATE_UT": "UP", "TOTAL_IPC_CRIMES": "20000"},
            {"LATITUDE": 18.92, "LONGITUDE": 72.83, "DISTRICT": "MUMBAI", "STATE_UT": "MAH", "TOTAL_IPC_CRIMES": "40000"},
            {"LATITUDE": 12.97, "LONGITUDE": 77.59, "DISTRICT": "BANGALORE", "STATE_UT": "KAR", "TOTAL_IPC_CRIMES": "45000"}
        ]
        centroids, labeled, summaries = run_fallback_kmeans(sample_pts, k=3, max_iter=10)
        self.assertEqual(len(centroids), 3)
        self.assertEqual(len(summaries), 3)

    def test_random_forest_training(self):
        """Verify Random Forest training and metrics generation."""
        mock_data = []
        for i in range(100):
            mock_data.append({
                "DISTRICT_RISK_SCORE": str(1000 + i * 50),
                "VIOLENT_CRIME_RATIO": str(0.1 + (i % 5) * 0.08),
                "PROPERTY_CRIME_RATIO": str(0.3),
                "WOMEN_CRIME_RATIO": str(0.1),
                "ECONOMIC_CRIME_RATIO": str(0.05),
                "YEAR": "2014",
                "HIGH_SEVERITY_FLAG": "1" if i % 2 == 0 else "0"
            })
        metrics = run_standalone_rf(mock_data)
        self.assertIn("accuracy", metrics)
        self.assertIn("auc_roc", metrics)
        self.assertGreaterEqual(metrics["accuracy"], 0.5)


if __name__ == "__main__":
    unittest.main()
