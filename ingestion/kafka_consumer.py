"""
Kafka Consumer for Real-Time Crime Event Aggregation.

This module completes Module 1's streaming path: it consumes the JSON
incident events published by kafka_producer.py on the 'live_crimes' topic,
maintains running per-district/per-crime-type aggregates, and periodically
flushes a live snapshot to data/analysis_results/live_stream_stats.json so
the dashboards can surface real-time activity alongside the batch-computed
historical analytics.

If a live Kafka broker is not reachable (no kafka-python, or no broker
running), the consumer falls back to replaying the same deterministic
sample-event generator the producer uses, so the aggregation logic is still
exercised end-to-end offline.
"""

import sys
import time
import json
import signal
from pathlib import Path
from collections import defaultdict

# Setup import path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import (
    ANALYSIS_RESULTS_DIR,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    setup_logging
)

logger = setup_logging("KafkaConsumer")

LIVE_STATS_FILE = ANALYSIS_RESULTS_DIR / "live_stream_stats.json"
FLUSH_EVERY_N_EVENTS = 200
OFFLINE_REPLAY_EVENT_CAP = 5000

running = True


def handle_sigint(sig, frame):
    """Graceful shutdown signal handler."""
    global running
    logger.info("Termination signal received. Stopping Kafka consumer...")
    running = False


signal.signal(signal.SIGINT, handle_sigint)
signal.signal(signal.SIGTERM, handle_sigint)


class LiveAggregator:
    """Maintains running per-district and per-crime-type incident counts."""

    def __init__(self):
        self.total_events = 0
        self.district_counts = defaultdict(int)
        self.district_meta = {}
        self.crime_type_counts = defaultdict(int)
        self.started_at = time.strftime("%Y-%m-%dT%H:%M:%SZ")

    def ingest(self, event: dict):
        self.total_events += 1
        district = event.get("district", "UNKNOWN")
        state = event.get("state", "UNKNOWN")
        key = f"{state}::{district}"
        count = int(event.get("incident_count", 1))

        self.district_counts[key] += count
        self.district_meta[key] = {"state": state, "district": district}
        self.crime_type_counts[event.get("primary_type", "OTHER")] += count

    def snapshot(self) -> dict:
        top_districts = sorted(self.district_counts.items(), key=lambda kv: kv[1], reverse=True)[:20]
        return {
            "last_updated": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "stream_started_at": self.started_at,
            "total_events_consumed": self.total_events,
            "top_live_districts": [
                {
                    "state": self.district_meta[key]["state"],
                    "district": self.district_meta[key]["district"],
                    "live_incident_count": count
                }
                for key, count in top_districts
            ],
            "crime_type_breakdown": dict(self.crime_type_counts)
        }

    def flush(self):
        snapshot = self.snapshot()
        with open(LIVE_STATS_FILE, "w", encoding="utf-8") as f:
            json.dump(snapshot, f, indent=2)
        logger.info(
            f"Flushed live aggregate snapshot: {self.total_events} events consumed, "
            f"{len(self.district_counts)} distinct districts active."
        )


def replay_offline_events():
    """
    Deterministic offline replacement for a live Kafka source: reuses the
    producer's sample-event generator directly so the aggregation pipeline
    can be exercised without a running broker.
    """
    from ingestion.kafka_producer import get_sample_events
    for event in get_sample_events():
        yield event


def run_consumer():
    """Start consuming crime events and aggregating live statistics."""
    global running
    logger.info(f"Connecting to Kafka cluster at {KAFKA_BOOTSTRAP_SERVERS}...")

    aggregator = LiveAggregator()
    consumer = None

    try:
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id="crime-analytics-live-aggregator",
            consumer_timeout_ms=1000
        )
        logger.info(f"Connected to Kafka broker. Consuming from topic '{KAFKA_TOPIC}'...")
    except Exception as exc:
        logger.warning(
            f"Could not connect to live Kafka broker ({exc}). "
            f"Falling back to offline replay of sample events."
        )

    if consumer is not None:
        while running:
            for message in consumer:
                if not running:
                    break
                aggregator.ingest(message.value)
                if aggregator.total_events % FLUSH_EVERY_N_EVENTS == 0:
                    aggregator.flush()
        consumer.close()
    else:
        for event in replay_offline_events():
            if not running or aggregator.total_events >= OFFLINE_REPLAY_EVENT_CAP:
                break
            aggregator.ingest(event)
            if aggregator.total_events % FLUSH_EVERY_N_EVENTS == 0:
                aggregator.flush()

    aggregator.flush()
    logger.info(f"Kafka consumer stopped. Total events aggregated: {aggregator.total_events}.")


if __name__ == "__main__":
    run_consumer()
