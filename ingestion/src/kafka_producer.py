"""
Kafka Producer Module for The Geopolitical Supply Chain Pulse

This module provides a Kafka producer class for publishing GDELT event data
to a Redpanda/Kafka topic. It handles JSON serialization, delivery callbacks,
and proper resource cleanup.

Technical Stack:
- Python 3.x
- confluent-kafka: High-performance Kafka client library
"""

from __future__ import annotations

import json
import logging
from typing import Any

from confluent_kafka import KafkaError, Producer

# Configure module logger
logger = logging.getLogger(__name__)


class PulseProducer:
    """
    A Kafka producer for publishing GDELT event records to Redpanda/Kafka.

    This class wraps the confluent-kafka Producer with JSON serialization
    and delivery reporting capabilities. It handles the conversion of
    Python dictionaries to JSON-encoded UTF-8 messages.

    Attributes:
        broker_address (str): The Kafka broker address (e.g., 'localhost:19092')
        producer (Producer): The underlying confluent-kafka Producer instance

    Example:
        >>> producer = PulseProducer("localhost:19092")
        >>> record = {"event_id": 123, "country": "CH"}
        >>> producer.produce("gdelt-china-events", record)
        >>> producer.flush()
    """

    def __init__(self, broker_address: str) -> None:
        """
        Initialize the PulseProducer with a Kafka broker address.

        Args:
            broker_address: The Kafka broker address in 'host:port' format.
                           Example: 'localhost:19092'

        Raises:
            ValueError: If the broker_address is empty or invalid.
        """
        if not broker_address or not isinstance(broker_address, str):
            raise ValueError("broker_address must be a non-empty string")

        self.broker_address = broker_address
        self._config = {
            "bootstrap.servers": broker_address,
            "client.id": "pulse-producer",
            "retries": 3,
            "retry.backoff.ms": 1000,
        }

        try:
            self.producer = Producer(self._config)
            logger.info(f"PulseProducer initialized with broker: {broker_address}")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            raise

    def _delivery_report(
        self, err: KafkaError | None, msg: Any
    ) -> None:
        """
        Delivery report callback invoked once for each message produced.

        This callback is called asynchronously by the Kafka client when the
        message delivery result is known.

        Args:
            err: KafkaError if the delivery failed, None if successful.
            msg: The Message object containing metadata about the delivered message.
        """
        if err is not None:
            logger.error(
                f"Message delivery failed: {err.str()}"
            )
        else:
            logger.info(
                f"Message delivered to {msg.topic()} [{msg.partition()}] "
                f"at offset {msg.offset()}"
            )

    def produce(self, topic: str, record: dict[str, Any]) -> None:
        """
        Produce a single record to the specified Kafka topic.

        The record dictionary is serialized to JSON and encoded as UTF-8
        before being sent to Kafka. The delivery status is reported
        asynchronously via the _delivery_report callback.

        Args:
            topic: The name of the Kafka topic to produce to.
            record: A Python dictionary containing the event data.

        Raises:
            ValueError: If topic is empty or record is not a dictionary.
            BufferError: If the local producer queue is full.
            Exception: For serialization errors or other Kafka errors.

        Example:
            >>> producer.produce("gdelt-china-events", {
            ...     "GLOBALEVENTID": 123456789,
            ...     "SQLDATE": 20240101,
            ...     "EventCode": "035"
            ... })
        """
        if not topic or not isinstance(topic, str):
            raise ValueError("topic must be a non-empty string")

        if not isinstance(record, dict):
            raise ValueError("record must be a dictionary")

        try:
            # Serialize the dictionary to JSON string and encode to UTF-8
            value = json.dumps(record, default=str).encode("utf-8")

            # Produce the message with async delivery callback
            self.producer.produce(
                topic=topic,
                value=value,
                callback=self._delivery_report,
            )

            # Poll to handle delivery callbacks (non-blocking)
            self.producer.poll(0)

        except json.JSONEncodeError as e:
            logger.error(f"JSON serialization error: {e}")
            raise
        except BufferError as e:
            logger.error(f"Producer queue is full: {e}")
            # Flush to free up queue space, then retry once
            self.flush()
            raise
        except Exception as e:
            logger.error(f"Unexpected error producing message: {e}")
            raise

    def flush(self, timeout: float = 30.0) -> int:
        """
        Flush all outstanding messages from the producer queue.

        This method blocks until all messages are delivered or the
        timeout is reached. It should be called before shutting down
        the producer to ensure no messages are lost.

        Args:
            timeout: Maximum time to wait in seconds (default: 30.0).

        Returns:
            Number of messages still in queue after flush.

        Example:
            >>> remaining = producer.flush(timeout=10.0)
            >>> if remaining > 0:
            ...     print(f"Warning: {remaining} messages not delivered")
        """
        logger.info(f"Flushing producer queue (timeout: {timeout}s)...")
        remaining = self.producer.flush(timeout=timeout)
        if remaining == 0:
            logger.info("All messages flushed successfully")
        else:
            logger.warning(f"{remaining} messages still in queue after flush")
        return remaining

    def close(self) -> None:
        """
        Close the producer and clean up resources.

        This method flushes any outstanding messages and releases
        the producer resources. It is safe to call multiple times.
        """
        if hasattr(self, "producer") and self.producer:
            logger.info("Closing PulseProducer...")
            self.flush()
            # No explicit close() on confluent-kafka Producer,
            # flush() handles cleanup
            self.producer = None

    def __enter__(self) -> PulseProducer:
        """Context manager entry: returns self."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit: ensures producer is closed."""
        self.close()
