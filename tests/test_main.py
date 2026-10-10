from datetime import datetime, timezone
import sys
import types
from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from src.models import RawEvent


def test_raw_event_accepts_valid_payload() -> None:
    event = RawEvent.model_validate(
        {
            "event_id": "event-1",
            "entity_id": "user-1",
            "action_type": "click",
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
        }
    )
    assert event.event_id == "event-1"


@pytest.mark.parametrize(
    "payload",
    [
        {"entity_id": "user-1", "action_type": "click"},
        {"event_id": "", "entity_id": "user-1", "action_type": "click"},
        {"event_id": "event-1", "entity_id": "user-1", "action_type": "click", "extra": True},
    ],
)
def test_raw_event_rejects_invalid_payload(payload: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        RawEvent.model_validate(payload)


def test_feature_endpoint_returns_features(monkeypatch: pytest.MonkeyPatch) -> None:
    from src import main

    fake_db = Mock()
    fake_db.get_features.return_value = {"last_action": "purchase", "user_activity_count": "3"}
    fake_consumer = Mock()

    class FakeThread:
        def __init__(self, target, **kwargs):
            self.target = target
            self.started = False

        def start(self) -> None:
            self.started = True

        def join(self, timeout=None) -> None:
            return None

        def is_alive(self) -> bool:
            return False

    monkeypatch.setattr(main, "PostgreSQLManager", lambda dsn: fake_db)
    monkeypatch.setattr(main, "FeatureConsumer", lambda *args: fake_consumer)
    monkeypatch.setattr(main.threading, "Thread", FakeThread)
    monkeypatch.setenv("POSTGRES_USER", "test_user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "test_password")
    monkeypatch.setenv("POSTGRES_DB", "test_db")
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    monkeypatch.setenv("RAW_EVENTS_TOPIC", "raw-events")

    with TestClient(main.app) as client:
        response = client.get("/features/user-1")

    assert response.status_code == 200
    assert response.json() == {
        "entity_id": "user-1",
        "features": {"last_action": "purchase", "user_activity_count": "3"},
    }
    fake_db.close.assert_called_once()
    fake_consumer.stop.assert_called_once()


def test_feature_endpoint_returns_empty_features_for_unknown_entity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from src import main

    fake_db = Mock()
    fake_db.get_features.return_value = {}

    class FakeThread:
        def __init__(self, target, **kwargs):
            pass

        def start(self) -> None:
            pass

        def join(self, timeout=None) -> None:
            pass

        def is_alive(self) -> bool:
            return False

    monkeypatch.setattr(main, "PostgreSQLManager", lambda dsn: fake_db)
    monkeypatch.setattr(main, "FeatureConsumer", lambda *args: Mock())
    monkeypatch.setattr(main.threading, "Thread", FakeThread)
    monkeypatch.setenv("POSTGRES_USER", "test_user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "test_password")
    monkeypatch.setenv("POSTGRES_DB", "test_db")
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    monkeypatch.setenv("RAW_EVENTS_TOPIC", "raw-events")

    with TestClient(main.app) as client:
        response = client.get("/features/missing")

    assert response.status_code == 200
    assert response.json() == {"entity_id": "missing", "features": {}}


def test_consumer_processes_event_before_committing(monkeypatch: pytest.MonkeyPatch) -> None:
    from src.consumer import FeatureConsumer

    fake_kafka = types.ModuleType("confluent_kafka")
    fake_kafka.Consumer = Mock()
    fake_kafka.KafkaError = type("KafkaError", (), {"_PARTITION_EOF": -1})
    fake_kafka.KafkaException = type("KafkaException", (Exception,), {})
    monkeypatch.setitem(sys.modules, "confluent_kafka", fake_kafka)

    database = Mock()
    database.process_event.return_value = True
    consumer = FeatureConsumer("broker:29092", "raw-events", database)
    message = Mock()
    message.value.return_value = (
        b'{"event_id":"event-1","entity_id":"user-1","action_type":"click"}'
    )
    message.topic.return_value = "raw-events"
    message.partition.return_value = 0
    message.offset.return_value = 12

    consumer._handle_message(message)

    database.process_event.assert_called_once()
    consumer._consumer.commit.assert_called_once_with(message=message, asynchronous=False)


def test_consumer_skips_invalid_event_and_commits_offset(monkeypatch: pytest.MonkeyPatch) -> None:
    from src.consumer import FeatureConsumer

    fake_kafka = types.ModuleType("confluent_kafka")
    fake_kafka.Consumer = Mock()
    fake_kafka.KafkaError = type("KafkaError", (), {"_PARTITION_EOF": -1})
    fake_kafka.KafkaException = type("KafkaException", (Exception,), {})
    monkeypatch.setitem(sys.modules, "confluent_kafka", fake_kafka)

    database = Mock()
    consumer = FeatureConsumer("broker:29092", "raw-events", database)
    message = Mock()
    message.value.return_value = b'{"entity_id":"user-1"}'
    message.topic.return_value = "raw-events"
    message.partition.return_value = 0
    message.offset.return_value = 13

    consumer._handle_message(message)

    database.process_event.assert_not_called()
    consumer._consumer.commit.assert_called_once_with(message=message, asynchronous=False)