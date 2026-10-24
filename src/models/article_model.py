from enum import Enum
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

# status article enums
class Status(str, Enum):
    DRAFT = "DRAFT"
    PUBLSIHED = "PUBLISHED"
    ARCHIVED = "ARCHIVED"

@dataclass
class Article:
    title: str
    content: str
    status: Status
    author_id: UUID
    category_id: int
    published_at: datetime | None
    created_at: datetime
    updated_at: datetime
    deleted_at: datetime | None

    def to_tuple(self) -> tuple:
        # convert to SQL obj params
        return (
            self.title,
            self.content,
            self.status.value,
            self.author_id,
            self.category_id,
            self.published_at,
            self.created_at,
            self.updated_at,
            self.deleted_at,
        )