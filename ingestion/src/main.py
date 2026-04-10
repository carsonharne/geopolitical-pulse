"""
Main Entry Point for The Geopolitical Supply Chain Pulse Ingestion Pipeline

This module orchestrates the GDELT data ingestion pipeline:
1. Fetches the latest GDELT 2.0 event data using GdeltFetcher
2. Filters for China-related supply chain events
3. Produces records to the Kafka topic 'gdelt-china-events'

Technical Stack:
- Python 3.x
- confluent-kafka: Kafka client for Redpanda
- httpx: HTTP client for GDELT API
- polars: DataFrame processing
"""

from __future__ import annotations

import logging
import sys
from typing import Any

from gdelt_fetcher import GdeltFetcher
from kafka_producer import PulseProducer

# Configure logging for clean console output
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Configuration constants
KAFKA_BROKER = "localhost:19092"
KAFKA_TOPIC = "gdelt-china-events"


def run_ingestion_pipeline(
    broker: str = KAFKA_BROKER, topic: str = KAFKA_TOPIC
) -> int:
    """
    Execute the full GDELT ingestion pipeline.

    This function coordinates the fetch and produce operations:
    1. Fetches GDELT data using GdeltFetcher
    2. If records are found, initializes PulseProducer
    3. Produces each record to the specified Kafka topic
    4. Ensures all messages are flushed before exiting

    Args:
        broker: Kafka broker address (default: localhost:19092)
        topic: Kafka topic name (default: gdelt-china-events)

    Returns:
        Exit code: 0 for success, 1 for failure
    """
    logger.info("=" * 60)
    logger.info("The Geopolitical Supply Chain Pulse - Ingestion Pipeline")
    logger.info("=" * 60)
    logger.info(f"Target Kafka Broker: {broker}")
    logger.info(f"Target Topic: {topic}")
    logger.info("")

    # Step 1: Fetch GDELT data
    logger.info("Starting GDELT data fetch...")
    fetcher = GdeltFetcher()
    records: list[dict[str, Any]] | None = fetcher.run()

    if records is None:
        logger.error("Failed to fetch GDELT data. Exiting.")
        return 1

    if not records:
        logger.info("No matching records found for the current time period.")
        return 0

    logger.info(f"Fetched {len(records)} records from GDELT")
    logger.info("")

    # Step 2: Initialize producer and send records to Kafka
    logger.info(f"Initializing Kafka producer to {broker}...")
    producer: PulseProducer | None = None

    try:
        producer = PulseProducer(broker_address=broker)

        logger.info(f"Producing records to topic '{topic}'...")
        logger.info("-" * 60)

        # Produce each record to Kafka
        for idx, record in enumerate(records, start=1):
            try:
                producer.produce(topic=topic, record=record)
                # Log progress every 10 records
                if idx % 10 == 0:
                    logger.info(f"Produced {idx}/{len(records)} records...")
            except Exception as e:
                logger.error(f"Error producing record {idx}: {e}")
                # Continue with other records instead of failing completely
                continue

        logger.info("-" * 60)
        logger.info(f"Produced {len(records)} records to Kafka")
        logger.info("")

        # Step 3: Flush all messages to ensure delivery
        logger.info("Flushing producer queue...")
        remaining = producer.flush()

        if remaining > 0:
            logger.warning(f"{remaining} messages may not have been delivered")
        else:
            logger.info("All messages delivered successfully")

    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user")
        if producer:
            producer.flush()
        return 130  # Standard exit code for Ctrl+C

    except Exception as e:
        logger.error(f"Pipeline error: {e}")
        return 1

    finally:
        # Ensure producer resources are cleaned up
        if producer:
            producer.close()

    logger.info("")
    logger.info("=" * 60)
    logger.info("Pipeline completed successfully")
    logger.info("=" * 60)
    return 0


def main() -> None:
    """
    Main entry point for the ingestion pipeline.

    Handles command-line execution and error handling.
    """
    try:
        exit_code = run_ingestion_pipeline()
        sys.exit(exit_code)
    except Exception as e:
        logger.critical(f"Unexpected error in main: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
