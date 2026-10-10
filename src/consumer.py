import logging
import threading
from typing import Any

from pydantic import ValidationError

from src.models import RawEvent

logger = logging.getLogger(__name__)


class FeatureConsumer:
    def __init__(self, bootstrap_servers: str, topic: str, db_manager: Any):
        from confluent_kafka import Consumer, KafkaError, KafkaException

        self._topic = topic
        self._db_manager = db_manager
        self._kafka_error = KafkaError
        self._kafka_exception = KafkaException
        self._stop_event = threading.Event()
        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": "feature-store-group",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )

    def start_consuming(self) -> None:
        self._consumer.subscribe([self._topic])
        logger.info("Subscribed to Kafka topic %s", self._topic)
        try:
            while not self._stop_event.is_set():
                try:
                    message = self._consumer.poll(1.0)
                except self._kafka_exception:
                    logger.exception("Kafka poll failed; retrying")
                    self._stop_event.wait(1)
                    continue
                if message is None:
                    continue
                if message.error():
                    if message.error().code() == self._kafka_error._PARTITION_EOF:
                        continue
                    logger.error("Kafka message error: %s", message.error())
                    continue
                self._handle_message(message)
        finally:
            try:
                self._consumer.close()
            except self._kafka_exception:
                logger.exception("Error while closing Kafka consumer")
            logger.info("Kafka consumer stopped and left its group")

    def _handle_message(self, message: Any) -> None:
        try:
            event = RawEvent.model_validate_json(message.value())
        except (ValidationError, ValueError, TypeError):
            logger.exception(
                "Skipping invalid event at %s[%s] offset %s",
                message.topic(),
                message.partition(),
                message.offset(),
            )
            self._commit(message)
            return

        delay = 1.0
        while not self._stop_event.is_set():
            try:
                was_new = self._db_manager.process_event(event)
                self._commit(message)
                logger.info(
                    "Processed event_id=%s entity_id=%s duplicate=%s",
                    event.event_id,
                    event.entity_id,
                    not was_new,
                )
                return
            except Exception:
                logger.exception("Feature write failed for event_id=%s; retrying", event.event_id)
                self._stop_event.wait(delay)
                delay = min(delay * 2, 30.0)

    def _commit(self, message: Any) -> None:
        try:
            self._consumer.commit(message=message, asynchronous=False)
        except self._kafka_exception:
            logger.exception(
                "Offset commit failed for %s[%s] offset %s",
                message.topic(),
                message.partition(),
                message.offset(),
            )

    def stop(self) -> None:
        self._stop_event.set()