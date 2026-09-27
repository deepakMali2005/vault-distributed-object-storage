"""API schemas for the VAULT storage node."""

from uuid import UUID

from pydantic import BaseModel


class StoredObject(BaseModel):
    """Metadata returned after an object is persisted."""

    object_id: UUID
    size: int
    checksum: str


class StoredObjectListResponse(BaseModel):
    """Response containing the physical objects stored on a node."""

    objects: list[StoredObject]