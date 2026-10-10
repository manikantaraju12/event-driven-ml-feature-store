import logging
import os
import threading
from contextlib import asynccontextmanager
from urllib.parse import quote_plus

from fastapi import FastAPI, HTTPException, Request

from src.consumer import FeatureConsumer
from src.db_manager import PostgreSQLManager
from src.models import FeatureResponse

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Required environment variable {name} is not set")
    return value


def _database_dsn() -> str:
    user = quote_plus(_required_env("POSTGRES_USER"))
    password = quote_plus(_required_env("POSTGRES_PASSWORD"))
    database = quote_plus(_required_env("POSTGRES_DB"))
    host = _required_env("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT", "5432")
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


@asynccontextmanager
async def lifespan(app: FastAPI):
    db_manager = PostgreSQLManager(_database_dsn())
    consumer = FeatureConsumer(
        _required_env("KAFKA_BOOTSTRAP_SERVERS"),
        _required_env("RAW_EVENTS_TOPIC"),
        db_manager,
    )
    consumer_thread = threading.Thread(
        target=consumer.start_consuming,
        name="kafka-feature-consumer",
        daemon=False,
    )
    app.state.db_manager = db_manager
    app.state.consumer = consumer
    app.state.consumer_thread = consumer_thread
    consumer_thread.start()
    logger.info("Feature service started")
    try:
        yield
    finally:
        consumer.stop()
        consumer_thread.join(timeout=10)
        if consumer_thread.is_alive():
            logger.error("Kafka consumer thread did not stop within the shutdown timeout")
        db_manager.close()
        logger.info("Feature service stopped")


app = FastAPI(title="Event-Driven ML Feature Store", lifespan=lifespan)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/features/{entity_id}", response_model=FeatureResponse)
def get_features(entity_id: str, request: Request) -> FeatureResponse:
    try:
        features = request.app.state.db_manager.get_features(entity_id)
    except Exception as exc:
        logger.exception("Feature lookup failed for entity_id=%s", entity_id)
        raise HTTPException(status_code=503, detail="Feature store is temporarily unavailable") from exc
    return FeatureResponse(entity_id=entity_id, features=features)