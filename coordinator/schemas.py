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


class ObjectReplicaResponse(BaseModel):
    """Internal metadata describing one object replica."""

    replica_id: UUID
    object_id: UUID
    node_id: UUID
    state: str
    created_at: datetime
    updated_at: datetime

    model_config = ConfigDict(from_attributes=True)


class ObjectReplicaListResponse(BaseModel):
    """Response containing replica metadata for an object."""

    replicas: list[ObjectReplicaResponse]


class UnderReplicatedObjectResponse(BaseModel):
    """Internal status describing one under-replicated object."""

    object_id: UUID
    object_key: str
    replication_factor: int
    healthy_replica_count: int
    missing_replicas: int


class UnderReplicatedObjectListResponse(BaseModel):
    """Response containing objects that need replica repair."""

    objects: list[UnderReplicatedObjectResponse]


class RepairResultResponse(BaseModel):
    """Internal result of an object repair attempt."""

    object_id: UUID
    object_key: str
    healthy_replica_count: int
    target_replica_count: int
    repaired_replica_count: int
    remaining_missing_replicas: int


class RepairResultListResponse(BaseModel):
    """Response containing repair results."""

    results: list[RepairResultResponse]


class PhysicalOrphanResponse(BaseModel):
    """Physical object that has no corresponding coordinator metadata."""

    object_id: UUID
    node_id: UUID


class ReconciliationResultResponse(BaseModel):
    """Result of reconciling one object against physical storage."""

    object_id: UUID
    object_key: str
    desired_node_ids: list[UUID]
    healthy_node_ids: list[UUID]
    repaired_node_ids: list[UUID]
    removed_node_ids: list[UUID]
    stale_node_ids: list[UUID]
    missing_node_ids: list[UUID]
    checksum_mismatch_node_ids: list[UUID]
    orphan_node_ids: list[UUID]
    unavailable_node_ids: list[UUID]


class ReconciliationResultListResponse(BaseModel):
    """Response containing reconciliation results and orphan objects."""

    results: list[ReconciliationResultResponse]
    orphan_objects: list[PhysicalOrphanResponse]