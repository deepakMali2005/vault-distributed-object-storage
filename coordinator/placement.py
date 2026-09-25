"""Deterministic object placement for the VAULT coordinator."""

from hashlib import sha256
from uuid import UUID

from coordinator.models import StorageNode


class PlacementError(Exception):
    """Raised when an object cannot be placed with the requested policy."""


def select_replicas(
    object_id: UUID,
    nodes: list[StorageNode],
    replication_factor: int,
) -> list[StorageNode]:
    """Select distinct ACTIVE storage nodes deterministically.

    The object UUID determines the starting position in a stable ordering of
    active nodes. Replicas are then selected by walking that ordering once.

    This distributes different objects across the cluster without introducing
    a full consistent-hashing ring at this stage of the project.
    """

    if replication_factor < 1:
        raise PlacementError("Replication factor must be at least 1")

    active_nodes = sorted(
        (node for node in nodes if node.status == "ACTIVE"),
        key=lambda node: (node.name, str(node.node_id)),
    )

    if not active_nodes:
        raise PlacementError("No active storage nodes are available")

    if replication_factor > len(active_nodes):
        raise PlacementError(
            "Replication factor cannot exceed the number of active storage nodes"
        )

    digest = sha256(object_id.bytes).digest()

    start_index = (
        int.from_bytes(
            digest[:8],
            byteorder="big",
        )
        % len(active_nodes)
    )

    return [
        active_nodes[
            (start_index + offset) % len(active_nodes)
        ]
        for offset in range(replication_factor)
    ]