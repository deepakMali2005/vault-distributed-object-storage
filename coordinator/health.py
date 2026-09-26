"""Storage-node health monitoring for the VAULT coordinator."""

from __future__ import annotations

import asyncio
from collections.abc import Callable

import httpx
from sqlalchemy.orm import Session

from coordinator.repository import StorageNodeRepository

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

            for node in nodes:
                healthy = self.check_node(node.url)
                results[str(node.node_id)] = healthy

                repository.update_status(
                    node.node_id,
                    ACTIVE if healthy else FAILED,
                )

            return results


async def run_health_monitor(
    checker: StorageNodeHealthChecker,
    *,
    interval_seconds: float,
) -> None:
    """Continuously check storage nodes until the application is shut down."""

    while True:
        await asyncio.to_thread(
            checker.check_all_nodes
        )

        await asyncio.sleep(interval_seconds)