import logging
from typing import Any

import psycopg2
from psycopg2.pool import ThreadedConnectionPool

from src.models import RawEvent

logger = logging.getLogger(__name__)


class PostgreSQLManager:
    def __init__(self, dsn: str, min_connections: int = 1, max_connections: int = 10):
        self._pool = ThreadedConnectionPool(min_connections, max_connections, dsn)

    def _execute(self, operation: Any) -> Any:
        connection = self._pool.getconn()
        broken = False
        try:
            with connection:
                with connection.cursor() as cursor:
                    return operation(cursor)
        except psycopg2.Error:
            broken = connection.closed != 0
            logger.exception("PostgreSQL operation failed")
            raise
        finally:
            self._pool.putconn(connection, close=broken)

    def save_feature(self, entity_id: str, feature_name: str, feature_value: str) -> None:
        def upsert(cursor: Any) -> None:
            cursor.execute(
                """
                INSERT INTO features (entity_id, feature_name, feature_value, timestamp)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (entity_id, feature_name)
                DO UPDATE SET feature_value = EXCLUDED.feature_value, timestamp = NOW()
                """,
                (entity_id, feature_name, feature_value),
            )

        self._execute(upsert)

    def process_event(self, event: RawEvent) -> bool:
        """Apply an event once, atomically recording its ID and derived features."""
        def update(cursor: Any) -> bool:
            cursor.execute(
                """
                INSERT INTO processed_events (event_id)
                VALUES (%s)
                ON CONFLICT (event_id) DO NOTHING
                RETURNING event_id
                """,
                (event.event_id,),
            )
            if cursor.fetchone() is None:
                return False

            event_time = event.event_timestamp
            cursor.execute(
                """
                INSERT INTO features (entity_id, feature_name, feature_value, timestamp)
                VALUES (%s, 'user_activity_count', '1', %s)
                ON CONFLICT (entity_id, feature_name)
                DO UPDATE SET
                    feature_value = (features.feature_value::BIGINT + 1)::TEXT,
                    timestamp = GREATEST(features.timestamp, EXCLUDED.timestamp)
                """,
                (event.entity_id, event_time),
            )
            cursor.execute(
                """
                INSERT INTO features (entity_id, feature_name, feature_value, timestamp)
                VALUES (%s, 'last_action', %s, %s)
                ON CONFLICT (entity_id, feature_name)
                DO UPDATE SET feature_value = EXCLUDED.feature_value, timestamp = EXCLUDED.timestamp
                WHERE features.timestamp <= EXCLUDED.timestamp
                """,
                (event.entity_id, event.action_type, event_time),
            )
            return True

        return self._execute(update)

    def get_features(self, entity_id: str) -> dict[str, str]:
        def fetch(cursor: Any) -> dict[str, str]:
            cursor.execute(
                "SELECT feature_name, feature_value FROM features WHERE entity_id = %s",
                (entity_id,),
            )
            return {feature_name: feature_value for feature_name, feature_value in cursor.fetchall()}

        return self._execute(fetch)

    def close(self) -> None:
        self._pool.closeall()