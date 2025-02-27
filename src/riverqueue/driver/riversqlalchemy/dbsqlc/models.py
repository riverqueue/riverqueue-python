# Code generated by sqlc. DO NOT EDIT.
# versions:
#   sqlc v1.27.0
import dataclasses
import datetime
import enum
from typing import Any, List, Optional


class RiverJobState(str, enum.Enum):
    AVAILABLE = "available"
    CANCELLED = "cancelled"
    COMPLETED = "completed"
    DISCARDED = "discarded"
    PENDING = "pending"
    RETRYABLE = "retryable"
    RUNNING = "running"
    SCHEDULED = "scheduled"


@dataclasses.dataclass()
class RiverJob:
    id: int
    args: Any
    attempt: int
    attempted_at: Optional[datetime.datetime]
    attempted_by: Optional[List[str]]
    created_at: datetime.datetime
    errors: Optional[List[Any]]
    finalized_at: Optional[datetime.datetime]
    kind: str
    max_attempts: int
    metadata: Any
    priority: int
    queue: str
    state: RiverJobState
    scheduled_at: datetime.datetime
    tags: List[str]
    unique_key: Optional[memoryview]
    unique_states: Optional[Any]
