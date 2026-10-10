import argparse
import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import KafkaException, Producer
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("event_producer")


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Required environment variable {name} is not set")
    return value


def delivery_report(error, message) -> None:
    if error is not None:
        logger.error("Delivery failed: %s", error)
    else:
        logger.info(
            "Delivered event_id=%s to %s[%s] offset=%s",
            message.key().decode("utf-8"),
            message.topic(),
            message.partition(),
            message.offset(),
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Produce simulated feature-store events")
    parser.add_argument("--count", type=int, default=10_000)
    parser.add_argument("--entity-id", help="Use one entity ID to make API checks easy")
    args = parser.parse_args()
    if args.count < 1:
        parser.error("--count must be at least 1")

    producer = Producer(
        {
            "bootstrap.servers": required_env("KAFKA_BOOTSTRAP_SERVERS"),
            "client.id": "feature-store-event-producer",
            "enable.idempotence": True,
        }
    )
    topic = required_env("RAW_EVENTS_TOPIC")
    actions = ("click", "purchase", "view")
    entity_ids = [f"user-{index}" for index in range(1, 101)]

    for _ in range(args.count):
        event_id = str(uuid.uuid4())
        event = {
            "event_id": event_id,
            "entity_id": args.entity_id or random.choice(entity_ids),
            "action_type": random.choice(actions),
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
        }
        payload = json.dumps(event).encode("utf-8")
        while True:
            try:
                producer.produce(
                    topic,
                    key=event_id.encode("utf-8"),
                    value=payload,
                    callback=delivery_report,
                )
                break
            except BufferError:
                producer.poll(1.0)
            except KafkaException:
                logger.exception("Kafka producer error; retrying event_id=%s", event_id)
                time.sleep(1)
                producer.poll(1.0)
        producer.poll(0)

    outstanding = producer.flush(30)
    if outstanding:
        logger.error("Producer shutdown with %s messages still queued", outstanding)
    else:
        logger.info("Finished producing %s events", args.count)


if __name__ == "__main__":
    main()