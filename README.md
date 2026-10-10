# Event-Driven ML Feature Store

A small Kafka-to-PostgreSQL feature pipeline with a FastAPI read endpoint.

## Run locally

1. Copy `.env.example` to `.env` and set a non-placeholder PostgreSQL password.
2. Start the stack with `docker compose up --build`.
3. In another terminal, load the host-side settings from `.env`, then run `python producer.py --count 10000 --entity-id demo-user`.
4. Query `http://localhost:8000/features/demo-user`.

The API returns an empty `features` object for an entity without stored features. The `user_activity_count` and `last_action` features are updated transactionally with event-ID deduplication, so replayed Kafka messages do not increment the counter again.

## Tests

Install `requirements.txt`, then run `pytest`. The tests cover event validation and API behavior without requiring Kafka or PostgreSQL to be running.