"""
Kafka Producer for Real-Time Crime Event Simulation.

This module reads crime records and streams them to the Apache Kafka
topic 'live_crimes' at a configurable rate (default 100 events/second),
simulating real-time police incident feeds.
"""

import sys
import time
import json
import signal
from pathlib import Path

# Setup import path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import (
    RAW_DATA_DIR,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_RATE_LIMIT_PER_SEC,
    setup_logging
)

logger = setup_logging("KafkaProducer")

running = True


def handle_sigint(sig, frame):
    """Graceful shutdown signal handler."""
    global running
    logger.info("Termination signal received. Stopping Kafka producer...")
    running = False


signal.signal(signal.SIGINT, handle_sigint)
signal.signal(signal.SIGTERM, handle_sigint)


def get_sample_events():
    """
    Yield structured crime events derived from real NCRB records.
    
    Yields:
        dict: A single crime incident event dictionary.
    """
    import csv
    input_file = RAW_DATA_DIR / "01_District_wise_crimes_committed_IPC_2014.csv"
    if not input_file.exists():
        logger.error(f"Sample data file {input_file} does not exist.")
        return

    with open(input_file, "r", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        event_id = 100000
        for row in reader:
            state = row.get("States/UTs") or row.get("STATE/UT") or "UNKNOWN"
            district = row.get("District") or row.get("DISTRICT") or "UNKNOWN"
            year = row.get("Year") or "2014"

            # Skip aggregate total rows
            if "TOTAL" in district.upper():
                continue

            crime_types = [
                ("MURDER", row.get("Murder", 0)),
                ("ROBBERY", row.get("Robbery", 0)),
                ("BURGLARY", row.get("Criminal Trespass/Burglary", 0) or row.get("BURGLARY", 0)),
                ("THEFT", row.get("Theft", 0)),
                ("RIOTS", row.get("Riots", 0)),
                ("CHEATING", row.get("Cheating", 0)),
                ("ARSON", row.get("Arson", 0)),
            ]

            for ctype, count in crime_types:
                try:
                    num = int(float(count))
                except (ValueError, TypeError):
                    num = 0

                # If crimes happened in this district, generate an event
                if num > 0:
                    event_id += 1
                    event = {
                        "event_id": f"IND-FIR-{event_id}",
                        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "state": state.strip(),
                        "district": district.strip(),
                        "year": int(year),
                        "primary_type": ctype,
                        "incident_count": min(num, 5),
                        "source": "NCRB_STATE_POLICE_STREAM"
                    }
                    yield event


def run_producer():
    """Start streaming crime events to Kafka."""
    global running
    logger.info(f"Connecting to Kafka cluster at {KAFKA_BOOTSTRAP_SERVERS}...")
    
    producer = None
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=3,
            acks='all'
        )
        logger.info(f"Connected to Kafka broker. Publishing to topic '{KAFKA_TOPIC}'...")
    except Exception as exc:
        logger.warning(
            f"Could not connect to live Kafka broker ({exc}). Running in simulation mode (logging only)."
        )

    delay = 1.0 / KAFKA_RATE_LIMIT_PER_SEC
    sent_count = 0
    start_time = time.time()

    logger.info(f"Publishing stream at target rate: {KAFKA_RATE_LIMIT_PER_SEC} events/sec...")

    while running:
        for event in get_sample_events():
            if not running:
                break

            if producer:
                producer.send(KAFKA_TOPIC, value=event)
            
            sent_count += 1
            if sent_count % 500 == 0:
                elapsed = time.time() - start_time
                actual_rate = sent_count / elapsed if elapsed > 0 else 0
                logger.info(f"Streamed {sent_count} crime events to '{KAFKA_TOPIC}' ({actual_rate:.1f} msgs/sec).")

            time.sleep(delay)
            if sent_count >= 5000:
                # In test run, complete 5000 events
                logger.info("Completed simulation batch of 5,000 events.")
                break
        break

    if producer:
        producer.flush()
        producer.close()
    logger.info(f"Kafka producer stopped. Total events emitted: {sent_count}.")


if __name__ == "__main__":
    run_producer()
