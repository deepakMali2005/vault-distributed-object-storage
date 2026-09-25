"""API schemas for the VAULT coordinator."""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class ObjectMetadataResponse(BaseModel):
    """Public metadata returned for a stored object."""

    object_id: UUID
    object_key: str
    size: int
    checksum: str
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class ObjectListResponse(BaseModel):
    """Response containing stored object metadata."""

    objects: list[ObjectMetadataResponse]