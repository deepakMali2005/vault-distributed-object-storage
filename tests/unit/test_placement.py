from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from coordinator.db import Base
from coordinator.models import StorageNode
from coordinator.placement import PlacementError, select_replicas


def make_nodes(
    db: Session,
    count: int = 3,
) -> list[StorageNode]:
    nodes = []

    for index in range(1, count + 1):
        node = StorageNode(
            name=f"storage-node-{index}",
            url=f"http://node-{index}:8100",
            status="ACTIVE",
        )

        db.add(node)
        nodes.append(node)

    db.commit()

    return nodes


def test_placement_is_deterministic():
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)

    object_id = UUID("12345678-1234-5678-1234-567812345678")

    with Session(engine) as db:
        nodes = make_nodes(db)

        first = select_replicas(
            object_id,
            nodes,
            2,
        )

        second = select_replicas(
            object_id,
            nodes,
            2,
        )

        assert [node.node_id for node in first] == [
            node.node_id for node in second
        ]


def test_placement_selects_requested_number_of_distinct_active_nodes():
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)

    with Session(engine) as db:
        nodes = make_nodes(db)

        nodes[1].status = "INACTIVE"
        db.commit()

        replicas = select_replicas(
            uuid4(),
            nodes,
            2,
        )

        assert len(replicas) == 2
        assert len({node.node_id for node in replicas}) == 2
        assert all(node.status == "ACTIVE" for node in replicas)


def test_placement_fails_when_replication_factor_exceeds_active_nodes():
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)

    with Session(engine) as db:
        nodes = make_nodes(db)

        nodes[2].status = "INACTIVE"
        db.commit()

        with pytest.raises(
            PlacementError,
            match="cannot exceed",
        ):
            select_replicas(
                uuid4(),
                nodes,
                3,
            )


def test_placement_fails_when_no_active_nodes_exist():
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)

    with Session(engine) as db:
        nodes = make_nodes(db)

        for node in nodes:
            node.status = "INACTIVE"

        db.commit()

        with pytest.raises(
            PlacementError,
            match="No active",
        ):
            select_replicas(
                uuid4(),
                nodes,
                1,
            )


def test_placement_rejects_invalid_replication_factor():
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)

    with Session(engine) as db:
        nodes = make_nodes(db)

        with pytest.raises(
            PlacementError,
            match="at least 1",
        ):
            select_replicas(
                uuid4(),
                nodes,
                0,
            )