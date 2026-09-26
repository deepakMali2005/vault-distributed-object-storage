"""Under-replication detection and replica repair for the VAULT coordinator."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from tempfile import SpooledTemporaryFile
from typing import Callable
from uuid import UUID

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from coordinator.models import ObjectMetadata, ObjectReplica, StorageNode
from coordinator.repository import (
    ObjectReplicaRepository,
    ObjectRepository,
    StorageNodeRepository,
)
from coordinator.storage_client import StorageNodeClient, StorageNodeError

ACTIVE = "ACTIVE"
FAILED = "FAILED"


@dataclass(frozen=True)
class UnderReplicatedObject:
    """Object whose effective healthy replica count is below the target."""

    object_id: UUID
    object_key: str
    replication_factor: int
    healthy_replica_count: int
    missing_replicas: int


@dataclass(frozen=True)
class RepairResult:
    """Outcome of one object's repair attempt."""

    object_id: UUID
    object_key: str
    healthy_replica_count: int
    target_replica_count: int
    repaired_replica_count: int
    remaining_missing_replicas: int


class ReplicaRepairService:
    """Find and repair objects whose effective replica count is too low."""

    def __init__(
        self,
        session_factory: Callable[[], Session],
        *,
        replication_factor: int,
    ) -> None:
        if replication_factor < 1:
            raise ValueError(
                "Replication factor must be at least 1"
            )

        self.session_factory = session_factory
        self.replication_factor = replication_factor

    def find_under_replicated(self) -> list[UnderReplicatedObject]:
        """Return objects with fewer healthy replicas than the target."""

        with self.session_factory() as db:
            objects = ObjectRepository(db).list_all()
            nodes = StorageNodeRepository(db).list_all()
            replicas = ObjectReplicaRepository(db)

            active_node_ids = {
                node.node_id
                for node in nodes
                if node.status == ACTIVE
            }

            result: list[UnderReplicatedObject] = []

            for obj in objects:
                healthy_count = sum(
                    1
                    for replica in replicas.list_for_object(
                        obj.object_id
                    )
                    if replica.state == ACTIVE
                    and replica.node_id in active_node_ids
                )

                if healthy_count < self.replication_factor:
                    result.append(
                        UnderReplicatedObject(
                            object_id=obj.object_id,
                            object_key=obj.object_key,
                            replication_factor=self.replication_factor,
                            healthy_replica_count=healthy_count,
                            missing_replicas=(
                                self.replication_factor
                                - healthy_count
                            ),
                        )
                    )

            return result

    def repair_all(self) -> list[RepairResult]:
        """Attempt repair for every currently under-replicated object."""

        objects = self.find_under_replicated()

        return [
            self.repair_object(obj.object_id)
            for obj in objects
        ]

    def repair_object(
        self,
        object_id: UUID,
    ) -> RepairResult:
        """Repair one object using an existing healthy replica as the source."""

        with self.session_factory() as db:
            object_repository = ObjectRepository(db)
            replica_repository = ObjectReplicaRepository(db)
            node_repository = StorageNodeRepository(db)

            obj = object_repository.get_by_id(object_id)
            nodes = node_repository.list_all()
            node_by_id = {
                node.node_id: node
                for node in nodes
            }

            replicas = replica_repository.list_for_object(
                object_id
            )

            # Failure detection normally updates replica state immediately.
            # Repair also normalizes stale metadata so direct/manual repair
            # remains correct if a node was already marked FAILED.
            failed_node_ids = {
                node.node_id
                for node in nodes
                if node.status == FAILED
            }

            for node_id in failed_node_ids:
                replica_repository.mark_failed_for_node(
                    node_id
                )

            replicas = replica_repository.list_for_object(
                object_id
            )

            healthy_replicas = [
                replica
                for replica in replicas
                if replica.state == ACTIVE
                and node_by_id.get(replica.node_id) is not None
                and node_by_id[replica.node_id].status == ACTIVE
            ]

            if len(healthy_replicas) >= self.replication_factor:
                return RepairResult(
                    object_id=obj.object_id,
                    object_key=obj.object_key,
                    healthy_replica_count=len(
                        healthy_replicas
                    ),
                    target_replica_count=self.replication_factor,
                    repaired_replica_count=0,
                    remaining_missing_replicas=0,
                )

            source = self._find_healthy_source(
                obj,
                healthy_replicas,
                node_by_id,
            )

            if source is None:
                return RepairResult(
                    object_id=obj.object_id,
                    object_key=obj.object_key,
                    healthy_replica_count=len(
                        healthy_replicas
                    ),
                    target_replica_count=self.replication_factor,
                    repaired_replica_count=0,
                    remaining_missing_replicas=(
                        self.replication_factor
                        - len(healthy_replicas)
                    ),
                )

            existing_node_ids = {
                replica.node_id
                for replica in replicas
            }

            candidates = [
                node
                for node in nodes
                if node.status == ACTIVE
                and node.node_id not in existing_node_ids
            ]

            candidates.sort(
                key=lambda node: (
                    self._placement_distance(
                        obj.object_id,
                        node,
                    ),
                    node.name,
                    str(node.node_id),
                )
            )

            missing = (
                self.replication_factor
                - len(healthy_replicas)
            )
            repaired = 0

            for destination in candidates[:missing]:
                if self._copy_replica(
                    obj,
                    node_by_id[source.node_id],
                    destination,
                ):
                    try:
                        replica_repository.create(
                            object_id=obj.object_id,
                            node_id=destination.node_id,
                            state=ACTIVE,
                        )
                    except IntegrityError:
                        db.rollback()

                        existing = self._find_replica(
                            replica_repository.list_for_object(
                                obj.object_id
                            ),
                            destination.node_id,
                        )

                        if existing is None:
                            continue

                        if existing.state != ACTIVE:
                            replica_repository.update_state(
                                existing.replica_id,
                                ACTIVE,
                            )

                    repaired += 1

            final_replicas = (
                replica_repository.list_for_object(
                    obj.object_id
                )
            )

            final_healthy_count = sum(
                1
                for replica in final_replicas
                if replica.state == ACTIVE
                and node_by_id.get(replica.node_id) is not None
                and node_by_id[replica.node_id].status == ACTIVE
            )

            return RepairResult(
                object_id=obj.object_id,
                object_key=obj.object_key,
                healthy_replica_count=final_healthy_count,
                target_replica_count=self.replication_factor,
                repaired_replica_count=repaired,
                remaining_missing_replicas=max(
                    0,
                    self.replication_factor
                    - final_healthy_count,
                ),
            )

    @staticmethod
    def _find_replica(
        replicas: list[ObjectReplica],
        node_id: UUID,
    ) -> ObjectReplica | None:
        for replica in replicas:
            if replica.node_id == node_id:
                return replica

        return None

    @staticmethod
    def _placement_distance(
        object_id: UUID,
        node: StorageNode,
    ) -> int:
        """Provide deterministic ordering for repair candidates."""

        digest = sha256(
            f"{object_id}:{node.node_id}".encode()
        ).digest()

        return int.from_bytes(
            digest[:8],
            "big",
        )

    def _find_healthy_source(
        self,
        obj: ObjectMetadata,
        replicas: list[ObjectReplica],
        node_by_id: dict[UUID, StorageNode],
    ) -> ObjectReplica | None:
        """Return a source replica whose physical object passes validation."""

        for replica in replicas:
            node = node_by_id.get(replica.node_id)

            if node is None or node.status != ACTIVE:
                continue

            if self._verify_replica(
                obj,
                node,
            ):
                return replica

        return None

    @staticmethod
    def _verify_replica(
        obj: ObjectMetadata,
        node: StorageNode,
    ) -> bool:
        """Verify that a source node contains the expected object."""

        client = StorageNodeClient(node.url)

        try:
            response = client.head_object(
                obj.object_id
            )

            content_length = response.headers.get(
                "content-length"
            )

            response.close()

            return (
                content_length is None
                or int(content_length) == obj.size
            )

        except (
            StorageNodeError,
            ValueError,
        ):
            return False

        finally:
            client.close()

    @staticmethod
    def _copy_replica(
        obj: ObjectMetadata,
        source: StorageNode,
        destination: StorageNode,
    ) -> bool:
        """Copy and verify one replica from source to destination."""

        source_client = StorageNodeClient(
            source.url
        )
        destination_client = StorageNodeClient(
            destination.url
        )

        buffered = SpooledTemporaryFile(
            max_size=8 * 1024 * 1024,
            mode="w+b",
        )

        digest = sha256()
        size = 0

        try:
            with source_client.stream_object(
                obj.object_id
            ) as response:
                content_length = response.headers.get(
                    "content-length"
                )

                if (
                    content_length is not None
                    and int(content_length) != obj.size
                ):
                    return False

                for chunk in response.iter_bytes(
                    1024 * 1024
                ):
                    buffered.write(chunk)
                    digest.update(chunk)
                    size += len(chunk)

            if (
                size != obj.size
                or digest.hexdigest() != obj.checksum
            ):
                return False

            buffered.seek(0)

            result = destination_client.put_object(
                obj.object_id,
                buffered,
            )

            return (
                result.get("object_id")
                == str(obj.object_id)
                and result.get("size") == obj.size
                and result.get("checksum")
                == obj.checksum
            )

        except (
            StorageNodeError,
            ValueError,
        ):
            return False

        finally:
            buffered.close()
            source_client.close()
            destination_client.close()