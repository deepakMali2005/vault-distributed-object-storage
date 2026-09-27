"""Placement rebalancing for the VAULT coordinator."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable
from uuid import UUID

from sqlalchemy.orm import Session

from coordinator.reconciliation import ReconciliationResult, ReconciliationService
from coordinator.repository import ObjectReplicaRepository, ObjectRepository, StorageNodeRepository
from coordinator.storage_client import StorageNodeClient, StorageNodeError


@dataclass(frozen=True)
class RebalanceResult:
    """Outcome of bringing one object toward its desired placement."""

    object_id: UUID
    object_key: str
    desired_node_ids: tuple[UUID, ...]
    healthy_node_ids: tuple[UUID, ...]
    migrated_node_ids: tuple[UUID, ...] = ()
    removed_node_ids: tuple[UUID, ...] = ()
    failed_node_ids: tuple[UUID, ...] = ()
    unavailable_node_ids: tuple[UUID, ...] = ()


class RebalancingService:
    """Move replicas toward current placement after membership or RF changes."""

    def __init__(
        self,
        session_factory: Callable[[], Session],
        *,
        replication_factor: int,
    ) -> None:
        if replication_factor < 1:
            raise ValueError("Replication factor must be at least 1")

        self.session_factory = session_factory
        self.replication_factor = replication_factor

    def rebalance_all(self) -> list[RebalanceResult]:
        """Reconcile desired placement, then drain decommissioned nodes."""

        reconciliation_results = ReconciliationService(
            self.session_factory,
            replication_factor=self.replication_factor,
        ).reconcile_all()

        results = {
            result.object_id: self._from_reconciliation(result)
            for result in reconciliation_results
        }

        self._drain_decommissioned_nodes(
            results,
        )

        return [
            results[object_id]
            for object_id in sorted(
                results,
                key=str,
            )
        ]

    def rebalance_object(
        self,
        object_id: UUID,
    ) -> RebalanceResult:
        """Rebalance one object and drain obsolete decommissioned replicas."""

        reconciliation_result = ReconciliationService(
            self.session_factory,
            replication_factor=self.replication_factor,
        ).reconcile_object(object_id)

        result = self._from_reconciliation(
            reconciliation_result,
        )
        results = {object_id: result}

        self._drain_decommissioned_nodes(
            results,
        )

        return results[object_id]

    @staticmethod
    def _from_reconciliation(
        result: ReconciliationResult,
    ) -> RebalanceResult:
        desired = set(result.desired_node_ids)
        healthy = set(result.healthy_node_ids)

        return RebalanceResult(
            object_id=result.object_id,
            object_key=result.object_key,
            desired_node_ids=result.desired_node_ids,
            healthy_node_ids=result.healthy_node_ids,
            migrated_node_ids=result.repaired_node_ids,
            removed_node_ids=result.removed_node_ids,
            failed_node_ids=tuple(
                node_id
                for node_id in result.desired_node_ids
                if node_id not in healthy
            ),
            unavailable_node_ids=result.unavailable_node_ids,
        )

    def _drain_decommissioned_nodes(
        self,
        results: dict[UUID, RebalanceResult],
    ) -> None:
        """Remove obsolete data from intentionally decommissioned nodes.

        Physical data and replica metadata are removed only after every
        desired replica reported by reconciliation is healthy. If the
        decommissioned node or any required destination is unavailable, the
        old replica remains for a later retry.
        """

        with self.session_factory() as db:
            object_repository = ObjectRepository(db)
            replica_repository = ObjectReplicaRepository(db)
            nodes = StorageNodeRepository(db).list_decommissioned()

            object_ids = [
                obj.object_id
                for obj in object_repository.list_all()
            ]

            replicas_by_node = {
                node.node_id: [
                    replica
                    for object_id in object_ids
                    for replica in replica_repository.list_for_object(object_id)
                    if replica.node_id == node.node_id
                ]
                for node in nodes
            }

        for node in nodes:
            client = StorageNodeClient(node.url)

            try:
                try:
                    physical_objects = client.list_objects()
                except StorageNodeError:
                    continue

                physical_ids = set()
                for item in physical_objects:
                    try:
                        physical_ids.add(UUID(str(item["object_id"])))
                    except (KeyError, TypeError, ValueError):
                        continue

                replica_by_object = {
                    replica.object_id: replica
                    for replica in replicas_by_node[node.node_id]
                }

                candidate_ids = (
                    physical_ids
                    | set(replica_by_object)
                )

                for object_id in candidate_ids:
                    result = results.get(object_id)

                    if result is None:
                        # The object may not have been included in a previous
                        # reconciliation result because it was not metadata
                        # backed. Do not delete unknown data here.
                        continue

                    if (
                        not result.desired_node_ids
                        or set(result.healthy_node_ids)
                        != set(result.desired_node_ids)
                    ):
                        continue

                    try:
                        if object_id in physical_ids:
                            client.delete_object(object_id)
                    except StorageNodeError as exc:
                        if exc.status_code != 404:
                            continue

                    replica = replica_by_object.get(object_id)

                    if replica is not None:
                        with self.session_factory() as db:
                            ObjectReplicaRepository(db).delete(
                                replica.replica_id,
                            )

                    current = results[object_id]
                    results[object_id] = RebalanceResult(
                        object_id=current.object_id,
                        object_key=current.object_key,
                        desired_node_ids=current.desired_node_ids,
                        healthy_node_ids=current.healthy_node_ids,
                        migrated_node_ids=current.migrated_node_ids,
                        removed_node_ids=tuple(
                            list(current.removed_node_ids)
                            + [node.node_id]
                        ),
                        failed_node_ids=current.failed_node_ids,
                        unavailable_node_ids=current.unavailable_node_ids,
                    )

            finally:
                client.close()
