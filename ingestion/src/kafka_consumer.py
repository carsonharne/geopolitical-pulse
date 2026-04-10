"""
File: ingestion/src/kafka_consumer.py
Description: Kafka Consumer for The Geopolitical Supply Chain Pulse
Subscribes to gdelt-china-events topic and sinks data to PostgreSQL warehouse.
"""

import json
import signal
import sys
from contextlib import contextmanager
from typing import Any

import psycopg2
from confluent_kafka import Consumer, KafkaError

# Configuration
KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:19092",
    "group.id": "gdelt-consumer-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True,
}

POSTGRES_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "geopolitical_pulse",
    "user": "admin",
    "password": "pulse_password_123",
}

TOPIC = "gdelt-china-events"


class GracefulShutdown:
    """Context manager for graceful shutdown on SIGINT/SIGTERM."""

    def __init__(self) -> None:
        self.shutdown = False
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum: int, frame: Any) -> None:
        print(f"\nReceived signal {signum}, initiating graceful shutdown...")
        self.shutdown = True

    def __enter__(self) -> "GracefulShutdown":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        pass


@contextmanager
def get_kafka_consumer():
    """Context manager for Kafka consumer."""
    consumer = Consumer(KAFKA_CONFIG)
    try:
        consumer.subscribe([TOPIC])
        print(f"Subscribed to topic: {TOPIC}")
        yield consumer
    finally:
        consumer.close()
        print("Kafka consumer closed")


@contextmanager
def get_postgres_connection():
    """Context manager for PostgreSQL connection."""
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    try:
        yield conn
    finally:
        conn.close()
        print("PostgreSQL connection closed")


def insert_record(cur, record: dict) -> None:
    """Insert a GDELT record into the raw_gdelt_events table."""
    insert_sql = """
        INSERT INTO raw_gdelt_events (
            globaleventid, sqldate, eventcode, goldsteinscale,
            nummentions, actiongeo_countrycode, sourceurl
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (globaleventid) DO NOTHING
    """
    cur.execute(
        insert_sql,
        (
            record.get("GLOBALEVENTID"),
            str(record.get("SQLDATE")),
            record.get("EventCode"),
            record.get("GoldsteinScale"),
            record.get("NumMentions"),
            record.get("ActionGeo_CountryCode"),
            record.get("SOURCEURL"),
        ),
    )


def process_message(msg, pg_conn) -> bool:
    """Process a single Kafka message and insert into Postgres."""
    try:
        record = json.loads(msg.value().decode("utf-8"))
        with pg_conn.cursor() as cur:
            insert_record(cur, record)
            pg_conn.commit()
        return True
    except json.JSONDecodeError as e:
        print(f"JSON decode error: {e}")
        return False
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        pg_conn.rollback()
        return False
    except Exception as e:
        print(f"Unexpected error processing message: {e}")
        return False


def main() -> None:
    """Main entry point for the Kafka consumer."""
    print("=" * 60)
    print("The Geopolitical Supply Chain Pulse - Kafka Consumer")
    print("=" * 60)
    print(f"Kafka Broker: {KAFKA_CONFIG['bootstrap.servers']}")
    print(f"Topic: {TOPIC}")
    print(f"Postgres: {POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['dbname']}")
    print("=" * 60)
    print()

    processed = 0
    errors = 0

    with GracefulShutdown() as shutdown:
        with get_kafka_consumer() as consumer:
            with get_postgres_connection() as pg_conn:
                print("Consumer ready. Press Ctrl+C to exit.")
                print()

                while not shutdown.shutdown:
                    msg = consumer.poll(timeout=1.0)

                    if msg is None:
                        continue

                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            print(f"End of partition reached: {msg.topic()} [{msg.partition()}]")
                        else:
                            print(f"Kafka error: {msg.error()}")
                            errors += 1
                        continue

                    if process_message(msg, pg_conn):
                        processed += 1
                        if processed % 100 == 0:
                            print(f"Processed {processed} records...")

    print()
    print("=" * 60)
    print(f"Consumer shutting down. Processed: {processed}, Errors: {errors}")
    print("=" * 60)


if __name__ == "__main__":
    main()
