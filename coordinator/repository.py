"""Metadata repositories for the VAULT coordinator."""

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from coordinator.models import ObjectMetadata, ObjectReplica, StorageNode


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
        url_statement = select(StorageNode).where(
            StorageNode.url == url
        )
        name_statement = select(StorageNode).where(
            StorageNode.name == name
        )

        node_by_url = self.db.scalar(url_statement)
        node_by_name = self.db.scalar(name_statement)

        if (
            node_by_url is not None
            and node_by_name is not None
            and node_by_url.node_id != node_by_name.node_id
        ):
            raise ValueError(
                "Storage node name and URL already belong to different nodes"
            )

        node = node_by_url or node_by_name

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
            node.url = url
            node.status = status
            node.capacity_bytes = capacity_bytes
            node.used_bytes = used_bytes

        self.db.commit()
        self.db.refresh(node)

        return node

    def list_all(self) -> list[StorageNode]:
        statement = select(StorageNode).order_by(
            StorageNode.name
        )

        return list(self.db.scalars(statement).all())

    def list_active(self) -> list[StorageNode]:
        statement = (
            select(StorageNode)
            .where(StorageNode.status == "ACTIVE")
            .order_by(StorageNode.name)
        )

        return list(self.db.scalars(statement).all())

    def update_status(
        self,
        node_id: UUID,
        status: str,
    ) -> StorageNode:
        """Update the liveness state of a registered storage node."""

        node = self.get_by_id(node_id)
        node.status = status
        self.db.commit()
        self.db.refresh(node)

        return node

    def get_by_id(self, node_id: UUID) -> StorageNode:
        node = self.db.get(StorageNode, node_id)

        if node is None:
            raise StorageNodeNotFoundError(node_id)

        return node


class ObjectReplicaRepository:
    """Provides persistence operations for object-replica metadata."""

    def __init__(self, db: Session) -> None:
        self.db = db

    def create(
        self,
        *,
        object_id: UUID,
        node_id: UUID,
        state: str = "PENDING",
    ) -> ObjectReplica:
        replica = ObjectReplica(
            object_id=object_id,
            node_id=node_id,
            state=state,
        )

        self.db.add(replica)
        self.db.commit()
        self.db.refresh(replica)

        return replica

    def create_many(
        self,
        *,
        object_id: UUID,
        node_ids: list[UUID],
        state: str = "PENDING",
    ) -> list[ObjectReplica]:
        replicas = [
            ObjectReplica(
                object_id=object_id,
                node_id=node_id,
                state=state,
            )
            for node_id in node_ids
        ]

        self.db.add_all(replicas)
        self.db.commit()

        for replica in replicas:
            self.db.refresh(replica)

        return replicas

    def mark_failed_for_node(
        self,
        node_id: UUID,
    ) -> int:
        """Mark all non-failed replicas on an unavailable node as FAILED."""

        statement = (
            select(ObjectReplica)
            .where(
                ObjectReplica.node_id == node_id,
                ObjectReplica.state != "FAILED",
            )
        )

        replicas = list(self.db.scalars(statement).all())

        for replica in replicas:
            replica.state = "FAILED"

        if replicas:
            self.db.commit()

        return len(replicas)

    def update_state(
        self,
        replica_id: UUID,
        state: str,
    ) -> ObjectReplica:
        replica = self.db.get(
            ObjectReplica,
            replica_id,
        )

        if replica is None:
            raise ValueError(
                f"Replica not found: {replica_id}"
            )

        replica.state = state

        self.db.commit()
        self.db.refresh(replica)

        return replica

    def delete(
        self,
        replica_id: UUID,
    ) -> None:
        replica = self.db.get(
            ObjectReplica,
            replica_id,
        )

        if replica is None:
            return

        self.db.delete(replica)
        self.db.commit()

    def list_for_object(
        self,
        object_id: UUID,
    ) -> list[ObjectReplica]:
        statement = (
            select(ObjectReplica)
            .where(ObjectReplica.object_id == object_id)
            .order_by(
                ObjectReplica.created_at,
                ObjectReplica.replica_id,
            )
        )

        return list(
            self.db.scalars(statement).all()
        )

    def list_active_for_object(
        self,
        object_id: UUID,
    ) -> list[ObjectReplica]:
        statement = (
            select(ObjectReplica)
            .where(
                ObjectReplica.object_id == object_id,
                ObjectReplica.state == "ACTIVE",
            )
            .order_by(
                ObjectReplica.created_at,
                ObjectReplica.replica_id,
            )
        )

        return list(
            self.db.scalars(statement).all()
        )