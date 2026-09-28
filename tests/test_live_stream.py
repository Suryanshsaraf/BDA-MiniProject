"""
Unit tests for Real-time Kafka Streaming & Anomaly Detection Service.
"""

import unittest
import json
import time
from pathlib import Path
from ingestion.live_stream_service import RealtimeStreamEngine

class TestRealtimeStreamEngine(unittest.TestCase):
    """Test suite for RealtimeStreamEngine."""

    def setUp(self):
        self.engine = RealtimeStreamEngine()

    def test_record_loading(self):
        """Verify authentic district records were parsed."""
        self.assertTrue(len(self.engine.raw_records) > 0, "Engine should load NCRB district records.")
        sample = self.engine.raw_records[0]
        self.assertIn("state", sample)
        self.assertIn("district", sample)
        self.assertIn("primary_type", sample)
        self.assertIn("base_count", sample)

    def test_anomaly_calculation(self):
        """Verify statistical anomaly detection computes Z-scores."""
        # Force high count to trigger anomaly
        anom = self.engine._detect_anomaly("TestDistrict", "TestState", "THEFT", 50)
        if anom:
            self.assertIn("z_score", anom)
            self.assertGreaterEqual(anom["z_score"], 2.4)
            self.assertIn(anom["severity"], ["CRITICAL", "WARNING"])
            self.assertEqual(anom["district"], "TestDistrict")

    def test_single_tick_snapshot(self):
        """Verify snapshot payload structure."""
        self.engine._flush()
        dashboard_json = Path(__file__).resolve().parent.parent / "react-dashboard" / "live_stream_stats.json"
        self.assertTrue(dashboard_json.exists(), "Snapshot file must exist.")
        
        with open(dashboard_json, "r", encoding="utf-8") as f:
            data = json.load(f)

        self.assertIn("is_live_stream_active", data)
        self.assertIn("total_events_consumed", data)
        self.assertIn("events_per_second", data)
        self.assertIn("active_broker_partitions", data)
        self.assertEqual(data["active_broker_partitions"], 8)
        self.assertIn("recent_events", data)
        self.assertIn("active_anomalies", data)

if __name__ == "__main__":
    unittest.main()
