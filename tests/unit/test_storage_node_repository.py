from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from coordinator.db import Base
from coordinator.repository import StorageNodeRepository


def test_upsert_creates_storage_node():
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)

    with Session(engine) as db:
        repository = StorageNodeRepository(db)

        node = repository.upsert(
            name="storage-node-1",
            url="http://node-1:8100",
        )

        assert node.node_id is not None
        assert node.name == "storage-node-1"
        assert node.url == "http://node-1:8100"
        assert node.status == "ACTIVE"


def test_upsert_updates_existing_node_without_creating_duplicate():
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)

    with Session(engine) as db:
        repository = StorageNodeRepository(db)

        first = repository.upsert(
            name="storage-node-1",
            url="http://node-1:8100",
        )
        second = repository.upsert(
            name="storage-node-1",
            url="http://node-1:8100",
            status="INACTIVE",
            capacity_bytes=100,
            used_bytes=25,
        )

        assert second.node_id == first.node_id
        assert second.status == "INACTIVE"
        assert second.capacity_bytes == 100
        assert second.used_bytes == 25
        assert len(repository.list_all()) == 1


def test_list_active_only_returns_active_nodes():
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)

    with Session(engine) as db:
        repository = StorageNodeRepository(db)

        repository.upsert(
            name="storage-node-1",
            url="http://node-1:8100",
            status="ACTIVE",
        )
        repository.upsert(
            name="storage-node-2",
            url="http://node-2:8100",
            status="INACTIVE",
        )

        active = repository.list_active()

        assert [node.name for node in active] == ["storage-node-1"]