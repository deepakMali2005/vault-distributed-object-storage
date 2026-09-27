from hashlib import sha256
from uuid import UUID, uuid4

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from coordinator.db import Base
from coordinator.models import ObjectMetadata, ObjectReplica, StorageNode
from coordinator.placement import select_replicas
from coordinator.rebalancing import RebalancingService
from coordinator.storage_client import StorageNodeError


class FakeResponse:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.headers = {"content-length": str(len(payload))}

    def iter_bytes(self, _chunk_size: int):
        yield self.payload

    def close(self) -> None:
        pass


class FakeClient:
    objects: dict[str, dict[UUID, bytes]] = {}
    failed_urls: set[str] = set()
    put_failed_urls: set[str] = set()
    bad_checksum_urls: set[str] = set()

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url
        self.objects.setdefault(base_url, {})

    def close(self) -> None:
        pass

    def list_objects(self):
        if self.base_url in self.failed_urls:
            raise StorageNodeError("node unavailable")

        return [
            {
                "object_id": str(object_id),
                "size": len(payload),
                "checksum": sha256(payload).hexdigest(),
            }
            for object_id, payload in self.objects[self.base_url].items()
        ]

    def stream_object(self, object_id):
        if self.base_url in self.failed_urls:
            raise StorageNodeError("node unavailable")

        payload = self.objects[self.base_url].get(object_id)

        if payload is None:
            raise StorageNodeError("object not found", status_code=404)

        class Context:
            def __enter__(self):
                return FakeResponse(payload)

            def __exit__(self, *_args):
                return False

        return Context()

    def put_object(self, object_id, file, filename=None, content_type=None):
        if self.base_url in self.failed_urls:
            raise StorageNodeError("node unavailable")

        if self.base_url in self.put_failed_urls:
            raise StorageNodeError("destination write failed")

        payload = file.read()
        self.objects[self.base_url][object_id] = payload
        checksum = sha256(payload).hexdigest()

        if self.base_url in self.bad_checksum_urls:
            checksum = "0" * 64

        return {
            "object_id": str(object_id),
            "size": len(payload),
            "checksum": checksum,
        }

    def delete_object(self, object_id):
        if self.base_url in self.failed_urls:
            raise StorageNodeError("node unavailable")

        if object_id not in self.objects[self.base_url]:
            raise StorageNodeError("object not found", status_code=404)

        del self.objects[self.base_url][object_id]


def create_session_factory():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)

    def factory():
        return Session(engine, expire_on_commit=False)

    return engine, factory


def reset_client():
    FakeClient.objects = {}
    FakeClient.failed_urls = set()
    FakeClient.put_failed_urls = set()
    FakeClient.bad_checksum_urls = set()


def create_nodes(factory, count=3):
    nodes = []

    with factory() as db:
        for index in range(1, count + 1):
            node = StorageNode(
                name=f"storage-node-{index}",
                url=f"http://node-{index}",
                status="ACTIVE",
            )
            nodes.append(node)

        db.add_all(nodes)
        db.commit()

    return nodes


def create_object(factory, object_id, payload, selected):
    with factory() as db:
        db.add(
            ObjectMetadata(
                object_id=object_id,
                object_key=f"docs/{object_id}.txt",
                size=len(payload),
                checksum=sha256(payload).hexdigest(),
            )
        )
        db.add_all(
            [
                ObjectReplica(
                    object_id=object_id,
                    node_id=node.node_id,
                    state="ACTIVE",
                )
                for node in selected
            ]
        )
        db.commit()

    FakeClient.objects = {
        node.url: {}
        for node in {node for node in selected}
    }

    for node in selected:
        FakeClient.objects[node.url][object_id] = payload


def find_changed_placement(before_nodes, after_nodes, rf):
    for _ in range(1000):
        object_id = uuid4()
        before = {
            node.node_id
            for node in select_replicas(object_id, before_nodes, rf)
        }
        after = {
            node.node_id
            for node in select_replicas(object_id, after_nodes, rf)
        }

        if before != after:
            return object_id

    raise AssertionError("Could not find a changed placement")


def patch_clients(monkeypatch):
    monkeypatch.setattr(
        "coordinator.reconciliation.StorageNodeClient",
        FakeClient,
    )
    monkeypatch.setattr(
        "coordinator.rebalancing.StorageNodeClient",
        FakeClient,
    )


def test_already_correct_object_is_a_no_op(monkeypatch):
    engine, factory = create_session_factory()
    payload = b"already correct"
    object_id = uuid4()

    try:
        nodes = create_nodes(factory, 3)
        selected = select_replicas(object_id, nodes, 3)
        reset_client()
        create_object(factory, object_id, payload, selected)
        patch_clients(monkeypatch)

        result = RebalancingService(
            factory,
            replication_factor=3,
        ).rebalance_object(object_id)

        assert result.migrated_node_ids == ()
        assert result.removed_node_ids == ()
        assert set(result.healthy_node_ids) == {
            node.node_id for node in selected
        }
    finally:
        engine.dispose()


def test_new_node_causes_verified_replica_migration(monkeypatch):
    engine, factory = create_session_factory()
    payload = b"rebalance me"

    try:
        nodes = create_nodes(factory, 3)
        object_id = find_changed_placement(nodes[:2], nodes, 2)
        before = select_replicas(object_id, nodes[:2], 2)
        after = select_replicas(object_id, nodes, 2)

        with factory() as db:
            db.get(StorageNode, nodes[2].node_id).status = "FAILED"
            db.commit()

            db.add(
                ObjectMetadata(
                    object_id=object_id,
                    object_key="docs/new-node.txt",
                    size=len(payload),
                    checksum=sha256(payload).hexdigest(),
                )
            )
            db.add_all(
                [
                    ObjectReplica(
                        object_id=object_id,
                        node_id=node.node_id,
                        state="ACTIVE",
                    )
                    for node in before
                ]
            )
            db.commit()

        nodes[2].status = "ACTIVE"

        with factory() as db:
            db.get(StorageNode, nodes[2].node_id).status = "ACTIVE"
            db.commit()

        reset_client()
        FakeClient.objects = {node.url: {} for node in nodes}
        for node in before:
            FakeClient.objects[node.url][object_id] = payload

        patch_clients(monkeypatch)

        result = RebalancingService(
            factory,
            replication_factor=2,
        ).rebalance_object(object_id)

        expected = {node.node_id for node in after}
        assert set(result.healthy_node_ids) == expected
        assert result.migrated_node_ids
        assert set(result.removed_node_ids) == (
            {node.node_id for node in before} - expected
        )

        for node in after:
            assert FakeClient.objects[node.url][object_id] == payload
    finally:
        engine.dispose()


def test_destination_write_failure_keeps_old_replica(monkeypatch):
    engine, factory = create_session_factory()
    payload = b"destination failure"

    try:
        nodes = create_nodes(factory, 3)
        object_id = find_changed_placement(nodes[:2], nodes, 2)
        before = select_replicas(object_id, nodes[:2], 2)
        after = select_replicas(object_id, nodes, 2)
        destination = next(node for node in after if node not in before)

        reset_client()
        create_object(factory, object_id, payload, before)
        FakeClient.put_failed_urls = {destination.url}
        patch_clients(monkeypatch)

        result = RebalancingService(
            factory,
            replication_factor=2,
        ).rebalance_object(object_id)

        assert result.migrated_node_ids == ()
        assert result.removed_node_ids == ()

        with factory() as db:
            replicas = db.query(ObjectReplica).filter_by(
                object_id=object_id,
            ).all()

            assert {
                replica.node_id for replica in replicas
            } == {node.node_id for node in before}
    finally:
        engine.dispose()


def test_checksum_mismatch_does_not_activate_destination(monkeypatch):
    engine, factory = create_session_factory()
    payload = b"checksum mismatch"

    try:
        nodes = create_nodes(factory, 3)
        object_id = find_changed_placement(nodes[:2], nodes, 2)
        before = select_replicas(object_id, nodes[:2], 2)
        after = select_replicas(object_id, nodes, 2)
        destination = next(node for node in after if node not in before)

        reset_client()
        create_object(factory, object_id, payload, before)
        FakeClient.bad_checksum_urls = {destination.url}
        patch_clients(monkeypatch)

        result = RebalancingService(
            factory,
            replication_factor=2,
        ).rebalance_object(object_id)

        assert destination.node_id in result.failed_node_ids
        assert destination.node_id not in result.healthy_node_ids

        with factory() as db:
            replica = db.query(ObjectReplica).filter_by(
                object_id=object_id,
                node_id=destination.node_id,
            ).one_or_none()

            assert replica is None
    finally:
        engine.dispose()


def test_source_unavailable_leaves_existing_replicas_intact(monkeypatch):
    engine, factory = create_session_factory()
    payload = b"source unavailable"

    try:
        nodes = create_nodes(factory, 3)
        object_id = find_changed_placement(nodes[:2], nodes, 2)
        before = select_replicas(object_id, nodes[:2], 2)

        reset_client()
        create_object(factory, object_id, payload, before)
        FakeClient.failed_urls = {node.url for node in before}
        patch_clients(monkeypatch)

        result = RebalancingService(
            factory,
            replication_factor=2,
        ).rebalance_object(object_id)

        assert result.migrated_node_ids == ()
        assert result.removed_node_ids == ()

        with factory() as db:
            replicas = db.query(ObjectReplica).filter_by(
                object_id=object_id,
            ).all()

            assert {
                replica.node_id for replica in replicas
            } == {node.node_id for node in before}
    finally:
        engine.dispose()


def test_repeated_rebalance_is_idempotent(monkeypatch):
    engine, factory = create_session_factory()
    payload = b"idempotent"
    object_id = uuid4()

    try:
        nodes = create_nodes(factory, 3)
        selected = select_replicas(object_id, nodes, 2)
        reset_client()
        create_object(factory, object_id, payload, selected)
        patch_clients(monkeypatch)

        service = RebalancingService(
            factory,
            replication_factor=2,
        )

        first = service.rebalance_object(object_id)
        second = service.rebalance_object(object_id)

        assert first.migrated_node_ids == ()
        assert first.removed_node_ids == ()
        assert second.migrated_node_ids == ()
        assert second.removed_node_ids == ()
    finally:
        engine.dispose()


def test_decommissioned_node_drains_after_desired_set_is_healthy(
    monkeypatch,
):
    engine, factory = create_session_factory()
    payload = b"decommission"

    try:
        nodes = create_nodes(factory, 4)
        object_id = uuid4()
        selected = select_replicas(object_id, nodes[:3], 3)
        decommissioned = selected[0]

        with factory() as db:
            db.get(
                StorageNode,
                decommissioned.node_id,
            ).status = "DECOMMISSIONED"
            db.add(
                ObjectMetadata(
                    object_id=object_id,
                    object_key="docs/decommission.txt",
                    size=len(payload),
                    checksum=sha256(payload).hexdigest(),
                )
            )
            db.add_all(
                [
                    ObjectReplica(
                        object_id=object_id,
                        node_id=node.node_id,
                        state="ACTIVE",
                    )
                    for node in selected
                ]
            )
            db.commit()

        reset_client()
        FakeClient.objects = {node.url: {} for node in nodes}
        for node in selected:
            FakeClient.objects[node.url][object_id] = payload

        patch_clients(monkeypatch)

        result = RebalancingService(
            factory,
            replication_factor=3,
        ).rebalance_object(object_id)

        assert decommissioned.node_id in result.removed_node_ids
        assert object_id not in FakeClient.objects[decommissioned.url]

        with factory() as db:
            replicas = db.query(ObjectReplica).filter_by(
                object_id=object_id,
            ).all()

            assert decommissioned.node_id not in {
                replica.node_id for replica in replicas
            }
    finally:
        engine.dispose()


def test_replication_factor_increase_creates_replica(monkeypatch):
    engine, factory = create_session_factory()
    payload = b"rf increase"
    object_id = uuid4()

    try:
        nodes = create_nodes(factory, 3)
        selected = select_replicas(object_id, nodes, 2)
        reset_client()
        create_object(factory, object_id, payload, selected)
        patch_clients(monkeypatch)

        result = RebalancingService(
            factory,
            replication_factor=3,
        ).rebalance_object(object_id)

        assert len(result.healthy_node_ids) == 3
        assert len(result.migrated_node_ids) == 1
    finally:
        engine.dispose()


def test_replication_factor_decrease_removes_excess_replica(monkeypatch):
    engine, factory = create_session_factory()
    payload = b"rf decrease"
    object_id = uuid4()

    try:
        nodes = create_nodes(factory, 3)
        selected = select_replicas(object_id, nodes, 3)
        reset_client()
        create_object(factory, object_id, payload, selected)
        patch_clients(monkeypatch)

        result = RebalancingService(
            factory,
            replication_factor=2,
        ).rebalance_object(object_id)

        assert len(result.healthy_node_ids) == 2
        assert len(result.removed_node_ids) == 1

        with factory() as db:
            replicas = db.query(ObjectReplica).filter_by(
                object_id=object_id,
            ).all()

            assert len(replicas) == 2
    finally:
        engine.dispose()
