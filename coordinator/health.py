"""Storage-node health monitoring for the VAULT coordinator."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from uuid import UUID

import httpx
from sqlalchemy.orm import Session

from coordinator.repository import ObjectReplicaRepository, StorageNodeRepository

ACTIVE = "ACTIVE"
FAILED = "FAILED"


class StorageNodeHealthChecker:
    """Checks registered storage nodes and updates their coordinator state."""

    def __init__(
        self,
        session_factory: Callable[[], Session],
        *,
        timeout_seconds: float = 2.0,
    ) -> None:
        self.session_factory = session_factory
        self.timeout_seconds = timeout_seconds
        self.newly_failed_node_ids: tuple[UUID, ...] = ()
        self.recovered_node_ids: tuple[UUID, ...] = ()

    def check_node(self, url: str) -> bool:
        """Return whether a storage node responds successfully to /health."""

        try:
            with httpx.Client(timeout=self.timeout_seconds) as client:
                response = client.get(
                    f"{url.rstrip('/')}/health"
                )

            if response.status_code != 200:
                return False

            try:
                payload = response.json()
            except ValueError:
                return False

            return payload.get("status") == "ok"

        except httpx.HTTPError:
            return False

    def check_all_nodes(self) -> dict[str, bool]:
        """Check every registered node and persist ACTIVE/FAILED state."""

        with self.session_factory() as db:
            repository = StorageNodeRepository(db)
            nodes = repository.list_all()
            results: dict[str, bool] = {}
            newly_failed_node_ids: list[UUID] = []
            recovered_node_ids: list[UUID] = []

            replica_repository = ObjectReplicaRepository(db)

            for node in nodes:
                previous_status = node.status
                healthy = self.check_node(node.url)
                results[str(node.node_id)] = healthy

                status = ACTIVE if healthy else FAILED

                repository.update_status(
                    node.node_id,
                    status,
                )

                if previous_status == ACTIVE and not healthy:
                    newly_failed_node_ids.append(node.node_id)

                if previous_status == FAILED and healthy:
                    recovered_node_ids.append(node.node_id)

                if not healthy:
                    replica_repository.mark_failed_for_node(
                        node.node_id,
                    )

            self.newly_failed_node_ids = tuple(newly_failed_node_ids)
            self.recovered_node_ids = tuple(recovered_node_ids)

            return results


async def run_health_monitor(
    checker: StorageNodeHealthChecker,
    *,
    interval_seconds: float,
    after_check: Callable[[], None] | None = None,
) -> None:
    """Continuously check storage nodes until the application is shut down."""

    while True:
        await asyncio.to_thread(checker.check_all_nodes)

        if after_check is not None:
            await asyncio.to_thread(after_check)

        await asyncio.sleep(interval_seconds)