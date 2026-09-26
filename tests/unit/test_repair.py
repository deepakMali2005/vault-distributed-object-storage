from uuid import uuid4

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from coordinator.db import Base
from coordinator.models import ObjectMetadata, ObjectReplica, StorageNode
from coordinator.repair import ReplicaRepairService
from coordinator.storage_client import StorageNodeError


class FakeResponse:
    def __init__(self, payload: bytes):
        self.payload = payload
        self.headers = {
            "content-length": str(len(payload))
        }

    def iter_bytes(self, _chunk_size: int):
        yield self.payload

    def close(self):
        pass


class FakeClient:
    objects: dict[str, dict[object, bytes]] = {}

    def __init__(self, base_url: str):
        self.base_url = base_url
        self.objects.setdefault(
            base_url,
            {},
        )

    def close(self):
        pass

    def head_object(self, object_id):
        payload = self.objects[
            self.base_url
        ].get(object_id)

        if payload is None:
            raise StorageNodeError(
                "not found",
                status_code=404,
            )

        return FakeResponse(payload)

    def stream_object(self, object_id):
        payload = self.objects[
            self.base_url
        ].get(object_id)

        if payload is None:
            raise StorageNodeError(
                "not found",
                status_code=404,
            )

        class Context:
            def __enter__(self):
                return FakeResponse(payload)

            def __exit__(self, *_args):
                return False

        return Context()

    def put_object(
        self,
        object_id,
        file,
        filename=None,
        content_type=None,
    ):
        payload = file.read()

        self.objects[
            self.base_url
        ][object_id] = payload

        import hashlib

        return {
            "object_id": str(object_id),
            "size": len(payload),
            "checksum": hashlib.sha256(
                payload
            ).hexdigest(),
        }


def create_session_factory():
    engine = create_engine(
        "sqlite://",
        connect_args={
            "check_same_thread": False
        },
        poolclass=StaticPool,
    )

    Base.metadata.create_all(engine)

    def factory():
        return Session(engine)

    return engine, factory


def test_finds_object_under_replicated_when_node_failed():
    engine, factory = create_session_factory()

    try:
        object_id = uuid4()

        with factory() as db:
            db.add(
                ObjectMetadata(
                    object_id=object_id,
                    object_key="docs/example.txt",
                    size=5,
                    checksum="a" * 64,
                )
            )

            node_one = StorageNode(
                name="storage-node-1",
                url="http://node-1",
                status="ACTIVE",
            )

            node_two = StorageNode(
                name="storage-node-2",
                url="http://node-2",
                status="FAILED",
            )

            node_three = StorageNode(
                name="storage-node-3",
                url="http://node-3",
                status="ACTIVE",
            )

            db.add_all(
                [
                    node_one,
                    node_two,
                    node_three,
                ]
            )

            db.flush()

            db.add_all(
                [
                    ObjectReplica(
                        object_id=object_id,
                        node_id=node_one.node_id,
                        state="ACTIVE",
                    ),
                    ObjectReplica(
                        object_id=object_id,
                        node_id=node_two.node_id,
                        state="ACTIVE",
                    ),
                    ObjectReplica(
                        object_id=object_id,
                        node_id=node_three.node_id,
                        state="ACTIVE",
                    ),
                ]
            )

            db.commit()

        service = ReplicaRepairService(
            factory,
            replication_factor=3,
        )

        under_replicated = (
            service.find_under_replicated()
        )

        assert len(under_replicated) == 1
        assert (
            under_replicated[0].object_id
            == object_id
        )
        assert (
            under_replicated[0].healthy_replica_count
            == 2
        )
        assert (
            under_replicated[0].missing_replicas
            == 1
        )

    finally:
        engine.dispose()


def test_repairs_object_to_unused_healthy_node(
    monkeypatch,
):
    engine, factory = create_session_factory()

    try:
        object_id = uuid4()
        payload = b"hello"

        import hashlib

        checksum = hashlib.sha256(
            payload
        ).hexdigest()

        with factory() as db:
            db.add(
                ObjectMetadata(
                    object_id=object_id,
                    object_key="docs/example.txt",
                    size=len(payload),
                    checksum=checksum,
                )
            )

            node_one = StorageNode(
                name="storage-node-1",
                url="http://node-1",
                status="ACTIVE",
            )

            node_two = StorageNode(
                name="storage-node-2",
                url="http://node-2",
                status="FAILED",
            )

            node_three = StorageNode(
                name="storage-node-3",
                url="http://node-3",
                status="ACTIVE",
            )

            node_four = StorageNode(
                name="storage-node-4",
                url="http://node-4",
                status="ACTIVE",
            )

            db.add_all(
                [
                    node_one,
                    node_two,
                    node_three,
                    node_four,
                ]
            )

            db.flush()

            db.add_all(
                [
                    ObjectReplica(
                        object_id=object_id,
                        node_id=node_one.node_id,
                        state="ACTIVE",
                    ),
                    ObjectReplica(
                        object_id=object_id,
                        node_id=node_two.node_id,
                        state="ACTIVE",
                    ),
                    ObjectReplica(
                        object_id=object_id,
                        node_id=node_three.node_id,
                        state="ACTIVE",
                    ),
                ]
            )

            db.commit()

            node_one_url = node_one.url
            node_four_url = node_four.url

        FakeClient.objects = {
            node_one_url: {
                object_id: payload
            },
            node_four_url: {},
        }

        monkeypatch.setattr(
            "coordinator.repair.StorageNodeClient",
            FakeClient,
        )

        service = ReplicaRepairService(
            factory,
            replication_factor=3,
        )

        result = service.repair_object(
            object_id
        )

        assert (
            result.repaired_replica_count
            == 1
        )

        assert (
            result.remaining_missing_replicas
            == 0
        )

        assert (
            result.healthy_replica_count
            == 3
        )

        assert (
            FakeClient.objects[
                node_four_url
            ][object_id]
            == payload
        )

        with factory() as db:
            replicas = (
                db.query(ObjectReplica)
                .filter(
                    ObjectReplica.object_id
                    == object_id
                )
                .all()
            )

            active_node_ids = {
                replica.node_id
                for replica in replicas
                if replica.state == "ACTIVE"
            }

            assert len(active_node_ids) == 3

    finally:
        engine.dispose()


def test_repair_stays_under_replicated_without_spare_node(
    monkeypatch,
):
    engine, factory = create_session_factory()

    try:
        object_id = uuid4()
        payload = b"hello"

        import hashlib

        with factory() as db:
            db.add(
                ObjectMetadata(
                    object_id=object_id,
                    object_key="docs/example.txt",
                    size=len(payload),
                    checksum=hashlib.sha256(
                        payload
                    ).hexdigest(),
                )
            )

            node_one = StorageNode(
                name="storage-node-1",
                url="http://node-1",
                status="ACTIVE",
            )

            node_two = StorageNode(
                name="storage-node-2",
                url="http://node-2",
                status="FAILED",
            )

            node_three = StorageNode(
                name="storage-node-3",
                url="http://node-3",
                status="ACTIVE",
            )

            db.add_all(
                [
                    node_one,
                    node_two,
                    node_three,
                ]
            )

            db.flush()

            db.add_all(
                [
                    ObjectReplica(
                        object_id=object_id,
                        node_id=node_one.node_id,
                        state="ACTIVE",
                    ),
                    ObjectReplica(
                        object_id=object_id,
                        node_id=node_two.node_id,
                        state="ACTIVE",
                    ),
                    ObjectReplica(
                        object_id=object_id,
                        node_id=node_three.node_id,
                        state="ACTIVE",
                    ),
                ]
            )

            db.commit()

        monkeypatch.setattr(
            "coordinator.repair.StorageNodeClient",
            FakeClient,
        )

        FakeClient.objects = {
            "http://node-1": {
                object_id: payload
            },
            "http://node-3": {
                object_id: payload
            },
        }

        service = ReplicaRepairService(
            factory,
            replication_factor=3,
        )

        result = service.repair_object(
            object_id
        )

        assert (
            result.repaired_replica_count
            == 0
        )

        assert (
            result.remaining_missing_replicas
            == 1
        )

        assert (
            result.healthy_replica_count
            == 2
        )

    finally:
        engine.dispose()


def test_repair_is_idempotent(
    monkeypatch,
):
    engine, factory = create_session_factory()

    try:
        object_id = uuid4()
        payload = b"hello"

        import hashlib

        checksum = hashlib.sha256(
            payload
        ).hexdigest()

        with factory() as db:
            db.add(
                ObjectMetadata(
                    object_id=object_id,
                    object_key="docs/example.txt",
                    size=len(payload),
                    checksum=checksum,
                )
            )

            nodes = [
                StorageNode(
                    name="storage-node-1",
                    url="http://node-1",
                    status="ACTIVE",
                ),
                StorageNode(
                    name="storage-node-2",
                    url="http://node-2",
                    status="FAILED",
                ),
                StorageNode(
                    name="storage-node-3",
                    url="http://node-3",
                    status="ACTIVE",
                ),
                StorageNode(
                    name="storage-node-4",
                    url="http://node-4",
                    status="ACTIVE",
                ),
            ]

            db.add_all(nodes)
            db.flush()

            db.add_all(
                [
                    ObjectReplica(
                        object_id=object_id,
                        node_id=nodes[0].node_id,
                        state="ACTIVE",
                    ),
                    ObjectReplica(
                        object_id=object_id,
                        node_id=nodes[1].node_id,
                        state="ACTIVE",
                    ),
                    ObjectReplica(
                        object_id=object_id,
                        node_id=nodes[2].node_id,
                        state="ACTIVE",
                    ),
                ]
            )

            db.commit()

        FakeClient.objects = {
            "http://node-1": {
                object_id: payload
            },
            "http://node-4": {},
        }

        monkeypatch.setattr(
            "coordinator.repair.StorageNodeClient",
            FakeClient,
        )

        service = ReplicaRepairService(
            factory,
            replication_factor=3,
        )

        first = service.repair_object(
            object_id
        )

        second = service.repair_object(
            object_id
        )

        assert (
            first.repaired_replica_count
            == 1
        )

        assert (
            second.repaired_replica_count
            == 0
        )

        assert (
            second.remaining_missing_replicas
            == 0
        )

    finally:
        engine.dispose()


def test_repair_rejects_source_checksum_mismatch(
    monkeypatch,
):
    engine, factory = create_session_factory()

    try:
        object_id = uuid4()
        payload = b"hello"

        import hashlib

        with factory() as db:
            db.add(
                ObjectMetadata(
                    object_id=object_id,
                    object_key="docs/example.txt",
                    size=len(payload),
                    checksum=hashlib.sha256(
                        b"different"
                    ).hexdigest(),
                )
            )

            node_one = StorageNode(
                name="storage-node-1",
                url="http://node-1",
                status="ACTIVE",
            )

            node_two = StorageNode(
                name="storage-node-2",
                url="http://node-2",
                status="ACTIVE",
            )

            node_three = StorageNode(
                name="storage-node-3",
                url="http://node-3",
                status="ACTIVE",
            )

            node_four = StorageNode(
                name="storage-node-4",
                url="http://node-4",
                status="ACTIVE",
            )

            db.add_all(
                [
                    node_one,
                    node_two,
                    node_three,
                    node_four,
                ]
            )

            db.flush()

            db.add_all(
                [
                    ObjectReplica(
                        object_id=object_id,
                        node_id=node_one.node_id,
                        state="ACTIVE",
                    ),
                    ObjectReplica(
                        object_id=object_id,
                        node_id=node_two.node_id,
                        state="ACTIVE",
                    ),
                ]
            )

            db.commit()

        FakeClient.objects = {
            "http://node-1": {
                object_id: payload
            },
            "http://node-2": {
                object_id: payload
            },
        }

        monkeypatch.setattr(
            "coordinator.repair.StorageNodeClient",
            FakeClient,
        )

        service = ReplicaRepairService(
            factory,
            replication_factor=3,
        )

        result = service.repair_object(
            object_id
        )

        assert (
            result.repaired_replica_count
            == 0
        )

        assert (
            result.remaining_missing_replicas
            == 1
        )

    finally:
        engine.dispose()