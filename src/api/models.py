from datetime import datetime, timezone
from uuid import uuid4

from api.lib import generate_uuid4_int
from pydantic import UUID4, BaseModel, Field


class DemoSession(BaseModel):
    session_id: UUID4 = Field(default_factory=lambda: generate_uuid4_int())
    api_key: str
    active: bool = (Field(default=True),)
    start_time: str = datetime.now().astimezone(timezone.utc).isoformat()
    end_time: str | None = None
    accused_steamids: list[str] | None = None
    fake_ip: str
    map: str
    query_by_fake_ip_data: dict | None = None
    confirmed_cheater_steamids: list[str] | None = None
    parsed_steamids: list[str] | None = None
    created_at: str | None = None
    updated_a: str | None = None
