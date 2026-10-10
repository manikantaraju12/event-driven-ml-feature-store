from datetime import datetime, timezone

from pydantic import BaseModel, ConfigDict, Field


class RawEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(min_length=1, max_length=255)
    entity_id: str = Field(min_length=1, max_length=255)
    action_type: str = Field(min_length=1, max_length=255)
    event_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class FeatureResponse(BaseModel):
    entity_id: str
    features: dict[str, str]