from uuid import uuid4

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from coordinator.db import Base
from coordinator.health import ACTIVE, FAILED, StorageNodeHealthChecker
from coordinator.models import ObjectMetadata, ObjectReplica, StorageNode
from coordinator.repository import StorageNodeRepository


def create_session_factory():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)

    def factory():
        return Session(engine)

    return engine, factory


def test_health_checker_marks_unreachable_node_failed(monkeypatch):
    engine, factory = create_session_factory()

    try:
        with factory() as db:
            repository = StorageNodeRepository(db)
            node = repository.upsert(
                name="storage-node-1",
                url="http://test-storage-1",
            )
            node_id = node.node_id

        checker = StorageNodeHealthChecker(factory)

        monkeypatch.setattr(
            checker,
            "check_node",
            lambda _url: False,
        )

        checker.check_all_nodes()

        with factory() as db:
            node = db.get(StorageNode, node_id)

            assert node is not None
            assert node.status == FAILED
    finally:
        engine.dispose()


def test_health_checker_marks_replicas_failed_for_unreachable_node(
    monkeypatch,
):
    engine, factory = create_session_factory()

    try:
        object_id = uuid4()

        with factory() as db:
            node = StorageNode(
                name="storage-node-1",
                url="http://test-storage-1",
                status=ACTIVE,
            )

            db.add(node)
            db.flush()

            db.add(
                ObjectMetadata(
                    object_id=object_id,
                    object_key="docs/example.txt",
                    size=5,
                    checksum="a" * 64,
                )
            )

            db.add(
                ObjectReplica(
                    object_id=object_id,
                    node_id=node.node_id,
                    state=ACTIVE,
                )
            )

            node_id = node.node_id

            db.commit()

        checker = StorageNodeHealthChecker(factory)

        monkeypatch.setattr(
            checker,
            "check_node",
            lambda _url: False,
        )

        checker.check_all_nodes()

        with factory() as db:
            node = db.get(
                StorageNode,
                node_id,
            )

            replica = (
                db.query(ObjectReplica)
                .filter(
                    ObjectReplica.object_id == object_id,
                    ObjectReplica.node_id == node_id,
                )
                .one()
            )

            assert node is not None
            assert node.status == FAILED
            assert replica.state == FAILED
    finally:
        engine.dispose()


def test_health_checker_recovers_failed_node(monkeypatch):
    engine, factory = create_session_factory()

    try:
        with factory() as db:
            repository = StorageNodeRepository(db)

            node = repository.upsert(
                name="storage-node-1",
                url="http://test-storage-1",
                status=FAILED,
            )

            node_id = node.node_id

        checker = StorageNodeHealthChecker(factory)

        monkeypatch.setattr(
            checker,
            "check_node",
            lambda _url: True,
        )

        checker.check_all_nodes()

        with factory() as db:
            node = db.get(
                StorageNode,
                node_id,
            )

            assert node is not None
            assert node.status == ACTIVE
    finally:
        engine.dispose()


def test_health_checker_treats_non_ok_health_response_as_failed(
    monkeypatch,
):
    engine, factory = create_session_factory()

    try:
        with factory() as db:
            repository = StorageNodeRepository(db)

            node = repository.upsert(
                name="storage-node-1",
                url="http://test-storage-1",
            )

        class FakeResponse:
            status_code = 200

            @staticmethod
            def json():
                return {"status": "degraded"}

        class FakeClient:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            @staticmethod
            def get(_url):
                return FakeResponse()

        monkeypatch.setattr(
            "coordinator.health.httpx.Client",
            lambda **_kwargs: FakeClient(),
        )

        checker = StorageNodeHealthChecker(factory)

        assert checker.check_node(node.url) is False
    finally:
        engine.dispose()