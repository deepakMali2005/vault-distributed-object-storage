"""Metadata repositories for the VAULT coordinator."""

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from coordinator.models import ObjectMetadata, StorageNode


class ObjectNotFoundError(Exception):
    """Raised when object metadata does not exist."""


class StorageNodeNotFoundError(Exception):
    """Raised when storage-node metadata does not exist."""


class ObjectRepository:
    """Provides persistence operations for object metadata."""

    def __init__(self, db: Session) -> None:
        self.db = db

    def create(
        self,
        *,
        object_id: UUID,
        object_key: str,
        size: int,
        checksum: str,
    ) -> ObjectMetadata:
        metadata = ObjectMetadata(
            object_id=object_id,
            object_key=object_key,
            size=size,
            checksum=checksum,
        )

        self.db.add(metadata)
        self.db.commit()
        self.db.refresh(metadata)

        return metadata

    def get_by_key(self, object_key: str) -> ObjectMetadata:
        statement = select(ObjectMetadata).where(
            ObjectMetadata.object_key == object_key
        )

        metadata = self.db.scalar(statement)

        if metadata is None:
            raise ObjectNotFoundError(object_key)

        return metadata

    def get_by_id(self, object_id: UUID) -> ObjectMetadata:
        metadata = self.db.get(ObjectMetadata, object_id)

        if metadata is None:
            raise ObjectNotFoundError(object_id)

        return metadata

    def list_all(self) -> list[ObjectMetadata]:
        statement = select(ObjectMetadata).order_by(
            ObjectMetadata.created_at,
            ObjectMetadata.object_id,
        )

        return list(self.db.scalars(statement).all())

    def delete(self, object_id: UUID) -> None:
        metadata = self.get_by_id(object_id)

        self.db.delete(metadata)
        self.db.commit()


class StorageNodeRepository:
    """Provides persistence operations for storage-node metadata."""

    def __init__(self, db: Session) -> None:
        self.db = db

    def upsert(
        self,
        *,
        name: str,
        url: str,
        status: str = "ACTIVE",
        capacity_bytes: int | None = None,
        used_bytes: int | None = None,
    ) -> StorageNode:
        statement = select(StorageNode).where(StorageNode.url == url)
        node = self.db.scalar(statement)

        if node is None:
            node = StorageNode(
                name=name,
                url=url,
                status=status,
                capacity_bytes=capacity_bytes,
                used_bytes=used_bytes,
            )
            self.db.add(node)
        else:
            node.name = name
            node.status = status
            node.capacity_bytes = capacity_bytes
            node.used_bytes = used_bytes

        self.db.commit()
        self.db.refresh(node)

        return node

    def list_all(self) -> list[StorageNode]:
        statement = select(StorageNode).order_by(StorageNode.name)
        return list(self.db.scalars(statement).all())

    def list_active(self) -> list[StorageNode]:
        statement = (
            select(StorageNode)
            .where(StorageNode.status == "ACTIVE")
            .order_by(StorageNode.name)
        )
        return list(self.db.scalars(statement).all())

    def get_by_id(self, node_id: UUID) -> StorageNode:
        node = self.db.get(StorageNode, node_id)

        if node is None:
            raise StorageNodeNotFoundError(node_id)

        return node