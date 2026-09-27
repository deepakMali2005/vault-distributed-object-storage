"""Reconcile coordinator metadata with physical storage state."""

from __future__ import annotations

from dataclasses import dataclass, field
from hashlib import sha256
from tempfile import SpooledTemporaryFile
from typing import Callable
from uuid import UUID

from sqlalchemy.orm import Session

from coordinator.models import ObjectMetadata, StorageNode
from coordinator.placement import PlacementError, select_replicas
from coordinator.repository import (
    ObjectReplicaRepository,
    ObjectRepository,
    StorageNodeRepository,
)
from coordinator.storage_client import StorageNodeClient, StorageNodeError

ACTIVE = "ACTIVE"
FAILED = "FAILED"


@dataclass(frozen=True)
class PhysicalObject:
    """Physical object metadata reported by a storage node."""

    object_id: UUID
    size: int
    checksum: str


@dataclass(frozen=True)
class PhysicalOrphan:
    """Physical object with no corresponding coordinator metadata."""

    object_id: UUID
    node_id: UUID


@dataclass(frozen=True)
class ReconciliationResult:
    """Outcome of reconciling one object."""

    object_id: UUID
    object_key: str
    desired_node_ids: tuple[UUID, ...]
    healthy_node_ids: tuple[UUID, ...]
    repaired_node_ids: tuple[UUID, ...] = ()
    removed_node_ids: tuple[UUID, ...] = ()
    stale_node_ids: tuple[UUID, ...] = ()
    missing_node_ids: tuple[UUID, ...] = ()
    checksum_mismatch_node_ids: tuple[UUID, ...] = ()
    orphan_node_ids: tuple[UUID, ...] = ()
    unavailable_node_ids: tuple[UUID, ...] = ()


@dataclass
class NodeInventory:
    """Inventory returned by one active storage node."""

    node: StorageNode
    objects: dict[UUID, PhysicalObject] = field(default_factory=dict)
    available: bool = True


class ReconciliationService:
    """Make physical storage converge toward the current placement policy."""

    def __init__(
        self,
        session_factory: Callable[[], Session],
        *,
        replication_factor: int,
    ) -> None:
        """Initialize reconciliation with the database factory and replica target."""
        if replication_factor < 1:
            raise ValueError("Replication factor must be at least 1")

        self.session_factory = session_factory
        self.replication_factor = replication_factor

    def reconcile_all(self) -> list[ReconciliationResult]:
        """Reconcile every metadata object against the current active cluster."""

        with self.session_factory() as db:
            objects = ObjectRepository(db).list_all()
            nodes = StorageNodeRepository(db).list_active()

        inventories = self._collect_inventories(nodes)

        return [
            self._reconcile_object(
                object_id=obj.object_id,
                inventories=inventories,
            )
            for obj in objects
        ]

    def reconcile_object(
        self,
        object_id: UUID,
    ) -> ReconciliationResult:
        """Reconcile one object using the current active-node inventory."""

        with self.session_factory() as db:
            obj = ObjectRepository(db).get_by_id(object_id)
            nodes = StorageNodeRepository(db).list_active()

        inventories = self._collect_inventories(nodes)

        return self._reconcile_object(
            object_id=obj.object_id,
            inventories=inventories,
        )

    def synchronize_recovered_node(
        self,
        node_id: UUID,
    ) -> list[ReconciliationResult]:
        """Synchronize objects that should be stored on a recovered node.

        This operation is intentionally narrower than normal reconciliation.
        It only repairs the recovered node. It does not repair unrelated
        under-replicated objects and does not remove obsolete replicas.
        """

        with self.session_factory() as db:
            node = StorageNodeRepository(db).get_by_id(node_id)

            if node.status != ACTIVE:
                return []

            objects = ObjectRepository(db).list_all()
            nodes = StorageNodeRepository(db).list_active()

        inventories = self._collect_inventories(nodes)

        recovered_inventory = inventories.get(node_id)

        if recovered_inventory is None or not recovered_inventory.available:
            return []

        active_nodes = [
            inventory.node
            for inventory in inventories.values()
            if inventory.available
        ]

        results: list[ReconciliationResult] = []

        for obj in objects:
            try:
                desired_nodes = select_replicas(
                    obj.object_id,
                    active_nodes,
                    self.replication_factor,
                )
            except PlacementError:
                continue

            desired_node_ids = {
                desired_node.node_id
                for desired_node in desired_nodes
            }

            if node_id not in desired_node_ids:
                continue

            results.append(
                self._synchronize_recovered_object(
                    obj=obj,
                    recovered_node_id=node_id,
                    desired_nodes=desired_nodes,
                    inventories=inventories,
                )
            )

        return results

    def find_orphan_objects(self) -> list[PhysicalOrphan]:
        """Find physical objects that have no coordinator metadata."""

        with self.session_factory() as db:
            metadata_ids = {
                obj.object_id
                for obj in ObjectRepository(db).list_all()
            }
            nodes = StorageNodeRepository(db).list_active()

        inventories = self._collect_inventories(nodes)
        orphans: list[PhysicalOrphan] = []

        for node_id, inventory in inventories.items():
            for object_id in inventory.objects:
                if object_id not in metadata_ids:
                    orphans.append(
                        PhysicalOrphan(
                            object_id=object_id,
                            node_id=node_id,
                        )
                    )

        return orphans

    def _collect_inventories(
        self,
        nodes: list[StorageNode],
    ) -> dict[UUID, NodeInventory]:
        """Query active nodes for their physical object inventory."""

        inventories: dict[UUID, NodeInventory] = {}

        for node in nodes:
            client = StorageNodeClient(node.url)

            try:
                payload = client.list_objects()
                objects: dict[UUID, PhysicalObject] = {}

                for item in payload:
                    try:
                        object_id = UUID(str(item["object_id"]))
                        size = int(item["size"])
                        checksum = str(item["checksum"])
                    except (KeyError, TypeError, ValueError):
                        continue

                    objects[object_id] = PhysicalObject(
                        object_id=object_id,
                        size=size,
                        checksum=checksum,
                    )

                inventories[node.node_id] = NodeInventory(
                    node=node,
                    objects=objects,
                )

            except StorageNodeError:
                inventories[node.node_id] = NodeInventory(
                    node=node,
                    available=False,
                )

            finally:
                client.close()

        return inventories

    def _synchronize_recovered_object(
        self,
        *,
        obj: ObjectMetadata,
        recovered_node_id: UUID,
        desired_nodes: list[StorageNode],
        inventories: dict[UUID, NodeInventory],
    ) -> ReconciliationResult:
        """Synchronize one recovered node without repairing other replicas.

        The recovered node is the only node that this method is allowed to
        modify. Other desired replicas are used only as verified copy sources.
        """

        desired_node_ids = tuple(
            node.node_id
            for node in desired_nodes
        )

        unavailable_node_ids = tuple(
            node_id
            for node_id, inventory in inventories.items()
            if not inventory.available
        )

        recovered_inventory = inventories[recovered_node_id]
        recovered_physical = recovered_inventory.objects.get(
            obj.object_id
        )

        recovered_is_valid = (
            recovered_physical is not None
            and recovered_physical.size == obj.size
            and recovered_physical.checksum == obj.checksum
        )

        missing_node_ids: tuple[UUID, ...] = ()
        checksum_mismatch_node_ids: tuple[UUID, ...] = ()
        stale_node_ids: tuple[UUID, ...] = ()
        repaired_node_ids: tuple[UUID, ...] = ()

        if recovered_physical is None:
            missing_node_ids = (recovered_node_id,)

        elif not recovered_is_valid:
            stale_node_ids = (recovered_node_id,)
            checksum_mismatch_node_ids = (recovered_node_id,)

        if not recovered_is_valid:
            source_node = self._find_healthy_source(
                obj=obj,
                recovered_node_id=recovered_node_id,
                inventories=inventories,
                desired_node_ids=set(desired_node_ids),
            )

            if source_node is not None:
                if self._copy_object(
                    obj,
                    source_node,
                    recovered_inventory.node,
                ):
                    recovered_is_valid = True
                    repaired_node_ids = (recovered_node_id,)

        with self.session_factory() as db:
            replica_repository = ObjectReplicaRepository(db)

            replicas = replica_repository.list_for_object(
                obj.object_id
            )

            replica_by_node = {
                replica.node_id: replica
                for replica in replicas
            }

            if recovered_is_valid:
                replica = replica_by_node.get(
                    recovered_node_id
                )

                if replica is None:
                    replica_repository.create(
                        object_id=obj.object_id,
                        node_id=recovered_node_id,
                        state=ACTIVE,
                    )

                elif replica.state != ACTIVE:
                    replica_repository.update_state(
                        replica.replica_id,
                        ACTIVE,
                    )

                db.commit()

            healthy_node_ids = tuple(
                node_id
                for node_id in desired_node_ids
                if (
                    node_id in inventories
                    and inventories[node_id].available
                    and (
                        physical := inventories[node_id].objects.get(
                            obj.object_id
                        )
                    ) is not None
                    and physical.size == obj.size
                    and physical.checksum == obj.checksum
                )
            )

            orphan_node_ids = tuple(
                node_id
                for node_id, inventory in inventories.items()
                if (
                    inventory.available
                    and obj.object_id in inventory.objects
                    and node_id not in desired_node_ids
                )
            )

        return ReconciliationResult(
            object_id=obj.object_id,
            object_key=obj.object_key,
            desired_node_ids=desired_node_ids,
            healthy_node_ids=healthy_node_ids,
            repaired_node_ids=repaired_node_ids,
            stale_node_ids=stale_node_ids,
            missing_node_ids=missing_node_ids,
            checksum_mismatch_node_ids=checksum_mismatch_node_ids,
            orphan_node_ids=orphan_node_ids,
            unavailable_node_ids=unavailable_node_ids,
        )

    @staticmethod
    def _find_healthy_source(
        *,
        obj: ObjectMetadata,
        recovered_node_id: UUID,
        inventories: dict[UUID, NodeInventory],
        desired_node_ids: set[UUID],
    ) -> StorageNode | None:
        """Find a verified healthy replica that can seed the recovered node."""

        for node_id in desired_node_ids:
            if node_id == recovered_node_id:
                continue

            inventory = inventories.get(node_id)

            if inventory is None or not inventory.available:
                continue

            physical = inventory.objects.get(
                obj.object_id
            )

            if physical is None:
                continue

            if (
                physical.size == obj.size
                and physical.checksum == obj.checksum
            ):
                return inventory.node

        return None

    def _reconcile_object(
        self,
        *,
        object_id: UUID,
        inventories: dict[UUID, NodeInventory],
        remove_obsolete: bool = True,
    ) -> ReconciliationResult:
        """Verify desired replicas, repair missing/stale ones, and optionally remove obsolete replicas."""

        with self.session_factory() as db:
            object_repository = ObjectRepository(db)
            replica_repository = ObjectReplicaRepository(db)
            obj = object_repository.get_by_id(object_id)
            active_nodes = [
                inventory.node
                for inventory in inventories.values()
                if inventory.available
            ]

            try:
                desired_nodes = select_replicas(
                    obj.object_id,
                    active_nodes,
                    self.replication_factor,
                )
            except PlacementError:
                return ReconciliationResult(
                    object_id=obj.object_id,
                    object_key=obj.object_key,
                    desired_node_ids=(),
                    healthy_node_ids=(),
                    unavailable_node_ids=tuple(
                        inventory.node.node_id
                        for inventory in inventories.values()
                        if not inventory.available
                    ),
                )

            desired_node_ids = tuple(
                node.node_id
                for node in desired_nodes
            )

            replicas = replica_repository.list_for_object(
                obj.object_id
            )

            replica_by_node = {
                replica.node_id: replica
                for replica in replicas
            }

            physical_by_node = {
                node_id: inventory.objects[obj.object_id]
                for node_id, inventory in inventories.items()
                if inventory.available
                and obj.object_id in inventory.objects
            }

            checksum_mismatches = tuple(
                node_id
                for node_id, physical in physical_by_node.items()
                if physical.size != obj.size
                or physical.checksum != obj.checksum
            )

            healthy_physical = {
                node_id
                for node_id, physical in physical_by_node.items()
                if physical.size == obj.size
                and physical.checksum == obj.checksum
            }

            source_node_id = next(
                (
                    node_id
                    for node_id in healthy_physical
                    if node_id in inventories
                ),
                None,
            )

            repaired: list[UUID] = []
            missing: list[UUID] = []
            stale: list[UUID] = []

            for node in desired_nodes:
                node_id = node.node_id
                physical = physical_by_node.get(node_id)
                is_valid = node_id in healthy_physical

                if not is_valid:
                    if physical is None:
                        missing.append(node_id)
                    else:
                        stale.append(node_id)

                    if source_node_id is not None:
                        if self._copy_object(
                            obj,
                            inventories[source_node_id].node,
                            node,
                        ):
                            healthy_physical.add(node_id)
                            repaired.append(node_id)
                            physical_by_node[node_id] = PhysicalObject(
                                object_id=obj.object_id,
                                size=obj.size,
                                checksum=obj.checksum,
                            )

                if node_id in healthy_physical:
                    replica = replica_by_node.get(node_id)

                    if replica is None:
                        replica_repository.create(
                            object_id=obj.object_id,
                            node_id=node_id,
                            state=ACTIVE,
                        )
                    elif replica.state != ACTIVE:
                        replica_repository.update_state(
                            replica.replica_id,
                            ACTIVE,
                        )

            desired_healthy = [
                node_id
                for node_id in desired_node_ids
                if node_id in healthy_physical
            ]

            removed: list[UUID] = []

            # Removal is deliberately gated on a complete verified desired set.
            if (
                remove_obsolete
                and len(desired_healthy) == len(desired_node_ids)
            ):
                for replica in replica_repository.list_for_object(
                    obj.object_id
                ):
                    if replica.node_id in desired_node_ids:
                        continue

                    inventory = inventories.get(replica.node_id)

                    if inventory is None or not inventory.available:
                        continue

                    physical = inventory.objects.get(
                        obj.object_id
                    )

                    if physical is None:
                        replica_repository.delete(
                            replica.replica_id
                        )
                        removed.append(
                            replica.node_id
                        )
                        continue

                    client = StorageNodeClient(
                        inventory.node.url
                    )

                    try:
                        client.delete_object(
                            obj.object_id
                        )

                        replica_repository.delete(
                            replica.replica_id
                        )

                        removed.append(
                            replica.node_id
                        )

                    except StorageNodeError:
                        pass

                    finally:
                        client.close()

            healthy_node_ids = tuple(
                node_id
                for node_id in desired_node_ids
                if node_id in healthy_physical
            )

            orphan_nodes = tuple(
                node_id
                for node_id, inventory in inventories.items()
                if inventory.available
                and obj.object_id in inventory.objects
                and node_id
                not in {
                    replica.node_id
                    for replica in replica_repository.list_for_object(
                        obj.object_id
                    )
                }
                and node_id not in desired_node_ids
            )

            return ReconciliationResult(
                object_id=obj.object_id,
                object_key=obj.object_key,
                desired_node_ids=desired_node_ids,
                healthy_node_ids=healthy_node_ids,
                repaired_node_ids=tuple(repaired),
                removed_node_ids=tuple(removed),
                stale_node_ids=tuple(stale),
                missing_node_ids=tuple(missing),
                checksum_mismatch_node_ids=checksum_mismatches,
                orphan_node_ids=orphan_nodes,
                unavailable_node_ids=tuple(
                    node_id
                    for node_id, inventory in inventories.items()
                    if not inventory.available
                ),
            )

    @staticmethod
    def _copy_object(
        obj: ObjectMetadata,
        source: StorageNode,
        destination: StorageNode,
    ) -> bool:
        """Copy an object between storage nodes after checksum verification."""

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

        try:
            with source_client.stream_object(
                obj.object_id
            ) as response:
                digest = sha256()
                size = 0

                for chunk in response.iter_bytes(
                    1024 * 1024
                ):
                    if not chunk:
                        continue

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
                and result.get("size")
                == obj.size
                and result.get("checksum")
                == obj.checksum
            )

        except (
            StorageNodeError,
            OSError,
        ):
            return False

        finally:
            buffered.close()
            source_client.close()
            destination_client.close()