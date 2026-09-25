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


class StorageNodeResponse(BaseModel):
    """Internal metadata describing a registered storage node."""

    node_id: UUID
    name: str
    url: str
    status: str
    capacity_bytes: int | None
    used_bytes: int | None
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class StorageNodeListResponse(BaseModel):
    """Response containing registered storage nodes."""

    nodes: list[StorageNodeResponse]