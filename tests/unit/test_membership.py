from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from coordinator.db import Base
from coordinator.health import StorageNodeHealthChecker
from coordinator.models import StorageNode
from coordinator.repository import StorageNodeRepository


def create_session_factory():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)

    def factory():
        return Session(
            engine,
            expire_on_commit=False,
        )

    return engine, factory


def test_upsert_does_not_resurrect_decommissioned_node():
    engine, factory = create_session_factory()

    try:
        with factory() as db:
            repository = StorageNodeRepository(db)

            node = repository.upsert(
                name="storage-node-1",
                url="http://node-1",
                status="ACTIVE",
            )

            repository.update_status(
                node.node_id,
                "DECOMMISSIONED",
            )

            node = repository.upsert(
                name="storage-node-1",
                url="http://node-1",
                status="ACTIVE",
            )

            assert node.status == "DECOMMISSIONED"

    finally:
        engine.dispose()


def test_health_checker_does_not_reactivate_decommissioned_node(monkeypatch):
    engine, factory = create_session_factory()

    try:
        with factory() as db:
            node = StorageNode(
                name="storage-node-1",
                url="http://node-1",
                status="DECOMMISSIONED",
            )
            db.add(node)
            db.commit()
            node_id = node.node_id

        checker = StorageNodeHealthChecker(factory)

        monkeypatch.setattr(
            checker,
            "check_node",
            lambda _url: True,
        )

        checker.check_all_nodes()

        with factory() as db:
            node = db.get(StorageNode, node_id)

            assert node.status == "DECOMMISSIONED"

        assert checker.recovered_node_ids == ()

    finally:
        engine.dispose()