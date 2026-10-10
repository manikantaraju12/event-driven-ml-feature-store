#!/bin/sh
set -eu

POSTGRES_HOST="${POSTGRES_HOST:?POSTGRES_HOST must be configured}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS must be configured}"
KAFKA_HOST="${KAFKA_BOOTSTRAP_SERVERS%%:*}"
KAFKA_PORT="${KAFKA_BOOTSTRAP_SERVERS##*:}"

until nc -z "$POSTGRES_HOST" "$POSTGRES_PORT"; do
    echo "Waiting for PostgreSQL at ${POSTGRES_HOST}:${POSTGRES_PORT}..."
    sleep 2
done

until nc -z "$KAFKA_HOST" "$KAFKA_PORT"; do
    echo "Waiting for Kafka at ${KAFKA_HOST}:${KAFKA_PORT}..."
    sleep 2
done

exec uvicorn src.main:app --host 0.0.0.0 --port 8000