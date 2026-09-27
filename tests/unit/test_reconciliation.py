import hashlib
from uuid import uuid4

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

from coordinator.db import Base
from coordinator.models import ObjectMetadata, ObjectReplica, StorageNode
from coordinator.placement import select_replicas
from coordinator.reconciliation import ReconciliationService
from coordinator.storage_client import StorageNodeError


class FakeResponse:
    def __init__(self, payload: bytes):
        self.payload = payload

    def iter_bytes(self, _chunk_size: int):
        yield self.payload

    def close(self):
        pass


class FakeClient:
    objects: dict[str, dict[object, bytes]] = {}
    failed_urls: set[str] = set()
    put_failed_urls: set[str] = set()
    put_call_count: dict[str, int] = {}

    def __init__(self, base_url: str):
        self.base_url = base_url
        self.objects.setdefault(base_url, {})
        self.put_call_count.setdefault(base_url, 0)

    def close(self):
        pass

    def list_objects(self):
        if self.base_url in self.failed_urls:
            raise StorageNodeError("node unavailable")

        return [
            {
                "object_id": str(object_id),
                "size": len(payload),
                "checksum": hashlib.sha256(payload).hexdigest(),
            }
            for object_id, payload in self.objects[self.base_url].items()
        ]

    def stream_object(self, object_id):
        if self.base_url in self.failed_urls:
            raise StorageNodeError("node unavailable")

        payload = self.objects[self.base_url].get(object_id)

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
        self.put_call_count[self.base_url] += 1

        if self.base_url in self.failed_urls:
            raise StorageNodeError("node unavailable")

        if self.base_url in self.put_failed_urls:
            raise StorageNodeError("put failed")

        payload = file.read()
        self.objects[self.base_url][object_id] = payload

        return {
            "object_id": str(object_id),
            "size": len(payload),
            "checksum": hashlib.sha256(payload).hexdigest(),
        }

    def delete_object(self, object_id):
        if self.base_url in self.failed_urls:
            raise StorageNodeError("node unavailable")

        if object_id not in self.objects[self.base_url]:
            raise StorageNodeError(
                "not found",
                status_code=404,
            )

        del self.objects[self.base_url][object_id]


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


def create_cluster(factory, object_id, payload):
    nodes = []

    with factory() as db:
        db.add(
            ObjectMetadata(
                object_id=object_id,
                object_key="docs/example.txt",
                size=len(payload),
                checksum=hashlib.sha256(payload).hexdigest(),
            )
        )

        for index in range(1, 5):
            node = StorageNode(
                name=f"storage-node-{index}",
                url=f"http://node-{index}",
                status="ACTIVE",
            )
            nodes.append(node)

        db.add_all(nodes)
        db.flush()
        db.commit()

    return nodes


def reset_fake_client():
    FakeClient.objects = {}
    FakeClient.failed_urls = set()
    FakeClient.put_failed_urls = set()
    FakeClient.put_call_count = {}


def test_reconciliation_syncs_missing_desired_replica(monkeypatch):
    engine, factory = create_session_factory()
    payload = b"hello vault"
    object_id = uuid4()

    try:
        nodes = create_cluster(factory, object_id, payload)
        selected = select_replicas(object_id, nodes, 3)
        source = selected[0]
        destination = selected[1]

        reset_fake_client()

        FakeClient.objects = {
            node.url: {}
            for node in nodes
        }
        FakeClient.objects[source.url][object_id] = payload

        with factory() as db:
            db.add(
                ObjectReplica(
                    object_id=object_id,
                    node_id=source.node_id,
                    state="ACTIVE",
                )
            )
            db.commit()

        monkeypatch.setattr(
            "coordinator.reconciliation.StorageNodeClient",
            FakeClient,
        )

        result = ReconciliationService(
            factory,
            replication_factor=3,
        ).reconcile_object(object_id)

        assert set(result.repaired_node_ids) >= {
            destination.node_id
        }
        assert len(result.healthy_node_ids) == 3
        assert all(
            object_id in FakeClient.objects[node.url]
            for node in selected
        )

    finally:
        engine.dispose()


def test_reconciliation_activates_valid_stale_replica(monkeypatch):
    engine, factory = create_session_factory()
    payload = b"stale metadata"
    object_id = uuid4()

    try:
        nodes = create_cluster(factory, object_id, payload)
        selected = select_replicas(object_id, nodes, 3)
        target = selected[0]

        reset_fake_client()

        FakeClient.objects = {
            node.url: {}
            for node in nodes
        }
        FakeClient.objects[target.url][object_id] = payload

        with factory() as db:
            db.add(
                ObjectReplica(
                    object_id=object_id,
                    node_id=target.node_id,
                    state="FAILED",
                )
            )
            db.commit()

        monkeypatch.setattr(
            "coordinator.reconciliation.StorageNodeClient",
            FakeClient,
        )

        ReconciliationService(
            factory,
            replication_factor=3,
        ).reconcile_object(object_id)

        with factory() as db:
            replica = db.query(ObjectReplica).filter_by(
                object_id=object_id,
                node_id=target.node_id,
            ).one()

            assert replica.state == "ACTIVE"

    finally:
        engine.dispose()


def test_reconciliation_removes_obsolete_replica_only_after_desired_set_is_verified(
    monkeypatch,
):
    engine, factory = create_session_factory()
    payload = b"rebalance me"
    object_id = uuid4()

    try:
        nodes = create_cluster(factory, object_id, payload)
        selected = select_replicas(object_id, nodes, 3)
        obsolete = next(
            node
            for node in nodes
            if node not in selected
        )

        reset_fake_client()

        FakeClient.objects = {
            node.url: {
                object_id: payload
            }
            for node in nodes
        }

        with factory() as db:
            db.add_all(
                [
                    ObjectReplica(
                        object_id=object_id,
                        node_id=node.node_id,
                        state="ACTIVE",
                    )
                    for node in nodes
                ]
            )
            db.commit()

        monkeypatch.setattr(
            "coordinator.reconciliation.StorageNodeClient",
            FakeClient,
        )

        result = ReconciliationService(
            factory,
            replication_factor=3,
        ).reconcile_object(object_id)

        assert obsolete.node_id in result.removed_node_ids
        assert object_id not in FakeClient.objects[obsolete.url]

        with factory() as db:
            replicas = db.query(ObjectReplica).filter_by(
                object_id=object_id,
                state="ACTIVE",
            ).all()

            assert {
                replica.node_id
                for replica in replicas
            } == {
                node.node_id
                for node in selected
            }

    finally:
        engine.dispose()


def test_reconciliation_reports_orphan_physical_object(monkeypatch):
    engine, factory = create_session_factory()
    object_id = uuid4()
    orphan_id = uuid4()
    payload = b"orphan"

    try:
        nodes = create_cluster(
            factory,
            object_id,
            payload,
        )

        reset_fake_client()

        FakeClient.objects = {
            node.url: {}
            for node in nodes
        }
        FakeClient.objects[
            nodes[0].url
        ][orphan_id] = payload

        monkeypatch.setattr(
            "coordinator.reconciliation.StorageNodeClient",
            FakeClient,
        )

        service = ReconciliationService(
            factory,
            replication_factor=3,
        )

        orphans = service.find_orphan_objects()

        assert len(orphans) == 1
        assert orphans[0].object_id == orphan_id
        assert orphans[0].node_id == nodes[0].node_id

    finally:
        engine.dispose()


def test_recovered_node_syncs_missing_desired_replica(
    monkeypatch,
):
    """A recovered desired node receives a verified copy and becomes ACTIVE."""

    engine, factory = create_session_factory()

    payload = b"recovered node"
    object_id = uuid4()

    try:
        nodes = create_cluster(
            factory,
            object_id,
            payload,
        )

        selected = select_replicas(
            object_id,
            nodes,
            3,
        )

        target = selected[0]
        source = selected[1]

        reset_fake_client()

        FakeClient.objects = {
            node.url: {}
            for node in nodes
        }

        FakeClient.objects[
            source.url
        ][object_id] = payload

        with factory() as db:
            target = db.get(
                StorageNode,
                target.node_id,
            )
            target.status = "FAILED"

            db.add(
                ObjectReplica(
                    object_id=object_id,
                    node_id=target.node_id,
                    state="FAILED",
                )
            )

            db.commit()

        with factory() as db:
            target = db.get(
                StorageNode,
                target.node_id,
            )
            target.status = "ACTIVE"
            db.commit()

        monkeypatch.setattr(
            "coordinator.reconciliation.StorageNodeClient",
            FakeClient,
        )

        results = ReconciliationService(
            factory,
            replication_factor=3,
        ).synchronize_recovered_node(
            target.node_id
        )

        assert len(results) == 1
        assert target.node_id in results[0].repaired_node_ids
        assert object_id in FakeClient.objects[target.url]

        with factory() as db:
            replica = (
                db.query(ObjectReplica)
                .filter_by(
                    object_id=object_id,
                    node_id=target.node_id,
                )
                .one()
            )

            assert replica.state == "ACTIVE"

    finally:
        engine.dispose()


def test_recovered_node_activates_valid_existing_replica(
    monkeypatch,
):
    """A recovered desired node with valid old data reactivates its replica metadata."""

    engine, factory = create_session_factory()

    payload = b"valid recovered data"
    object_id = uuid4()

    try:
        nodes = create_cluster(
            factory,
            object_id,
            payload,
        )

        selected = select_replicas(
            object_id,
            nodes,
            3,
        )

        target = selected[0]

        reset_fake_client()

        FakeClient.objects = {
            node.url: {}
            for node in nodes
        }

        FakeClient.objects[
            target.url
        ][object_id] = payload

        with factory() as db:
            target = db.get(
                StorageNode,
                target.node_id,
            )
            target.status = "ACTIVE"

            db.add(
                ObjectReplica(
                    object_id=object_id,
                    node_id=target.node_id,
                    state="FAILED",
                )
            )

            db.commit()

        monkeypatch.setattr(
            "coordinator.reconciliation.StorageNodeClient",
            FakeClient,
        )

        results = ReconciliationService(
            factory,
            replication_factor=3,
        ).synchronize_recovered_node(
            target.node_id
        )

        assert len(results) == 1
        assert results[0].repaired_node_ids == ()
        assert target.node_id in results[0].healthy_node_ids

        with factory() as db:
            replica = (
                db.query(ObjectReplica)
                .filter_by(
                    object_id=object_id,
                    node_id=target.node_id,
                )
                .one()
            )

            assert replica.state == "ACTIVE"

    finally:
        engine.dispose()


def test_recovered_node_does_not_resurrect_non_desired_replica(
    monkeypatch,
):
    """A recovered node outside current placement keeps old FAILED metadata."""

    engine, factory = create_session_factory()

    payload = b"stale non desired data"
    object_id = uuid4()

    try:
        nodes = create_cluster(
            factory,
            object_id,
            payload,
        )

        selected = select_replicas(
            object_id,
            nodes,
            3,
        )

        target = next(
            node
            for node in nodes
            if node not in selected
        )

        reset_fake_client()

        FakeClient.objects = {
            node.url: {}
            for node in nodes
        }

        FakeClient.objects[
            target.url
        ][object_id] = payload

        with factory() as db:
            target = db.get(
                StorageNode,
                target.node_id,
            )
            target.status = "ACTIVE"

            db.add(
                ObjectReplica(
                    object_id=object_id,
                    node_id=target.node_id,
                    state="FAILED",
                )
            )

            db.commit()

        monkeypatch.setattr(
            "coordinator.reconciliation.StorageNodeClient",
            FakeClient,
        )

        results = ReconciliationService(
            factory,
            replication_factor=3,
        ).synchronize_recovered_node(
            target.node_id
        )

        assert results == []
        assert object_id in FakeClient.objects[target.url]

        with factory() as db:
            replica = (
                db.query(ObjectReplica)
                .filter_by(
                    object_id=object_id,
                    node_id=target.node_id,
                )
                .one()
            )

            assert replica.state == "FAILED"

    finally:
        engine.dispose()


def test_recovered_node_replaces_corrupted_existing_data(
    monkeypatch,
):
    """A recovered desired node replaces corrupted physical data from a healthy source."""

    engine, factory = create_session_factory()

    payload = b"correct recovered payload"
    corrupted_payload = b"corrupted recovered payload"
    object_id = uuid4()

    try:
        nodes = create_cluster(
            factory,
            object_id,
            payload,
        )

        selected = select_replicas(
            object_id,
            nodes,
            3,
        )

        target = selected[0]
        source = selected[1]

        reset_fake_client()

        FakeClient.objects = {
            node.url: {}
            for node in nodes
        }

        FakeClient.objects[source.url][object_id] = payload
        FakeClient.objects[target.url][object_id] = corrupted_payload

        with factory() as db:
            db.add(
                ObjectReplica(
                    object_id=object_id,
                    node_id=target.node_id,
                    state="FAILED",
                )
            )
            db.add(
                ObjectReplica(
                    object_id=object_id,
                    node_id=source.node_id,
                    state="ACTIVE",
                )
            )
            db.commit()

        monkeypatch.setattr(
            "coordinator.reconciliation.StorageNodeClient",
            FakeClient,
        )

        results = ReconciliationService(
            factory,
            replication_factor=3,
        ).synchronize_recovered_node(
            target.node_id
        )

        assert len(results) == 1
        assert target.node_id in results[0].repaired_node_ids
        assert (
            FakeClient.objects[target.url][object_id]
            == payload
        )

        with factory() as db:
            replica = (
                db.query(ObjectReplica)
                .filter_by(
                    object_id=object_id,
                    node_id=target.node_id,
                )
                .one()
            )

            assert replica.state == "ACTIVE"

    finally:
        engine.dispose()


def test_recovered_node_does_not_activate_without_healthy_source(
    monkeypatch,
):
    """A recovered desired node stays FAILED when no verified source is available."""

    engine, factory = create_session_factory()

    payload = b"source unavailable"
    object_id = uuid4()

    try:
        nodes = create_cluster(
            factory,
            object_id,
            payload,
        )

        selected = select_replicas(
            object_id,
            nodes,
            3,
        )

        target = selected[0]
        source = selected[1]

        reset_fake_client()

        FakeClient.objects = {
            node.url: {}
            for node in nodes
        }

        FakeClient.failed_urls = {
            source.url,
        }

        with factory() as db:
            db.add(
                ObjectReplica(
                    object_id=object_id,
                    node_id=target.node_id,
                    state="FAILED",
                )
            )
            db.add(
                ObjectReplica(
                    object_id=object_id,
                    node_id=source.node_id,
                    state="ACTIVE",
                )
            )
            db.commit()

        monkeypatch.setattr(
            "coordinator.reconciliation.StorageNodeClient",
            FakeClient,
        )

        results = ReconciliationService(
            factory,
            replication_factor=3,
        ).synchronize_recovered_node(
            target.node_id
        )

        assert len(results) == 1
        assert source.node_id in results[0].unavailable_node_ids
        assert target.node_id not in results[0].healthy_node_ids
        assert target.node_id not in results[0].repaired_node_ids

        with factory() as db:
            replica = (
                db.query(ObjectReplica)
                .filter_by(
                    object_id=object_id,
                    node_id=target.node_id,
                )
                .one()
            )

            assert replica.state == "FAILED"

    finally:
        engine.dispose()


def test_recovered_node_sync_is_idempotent(
    monkeypatch,
):
    """Running recovered-node synchronization twice does not copy the object twice."""

    engine, factory = create_session_factory()

    payload = b"idempotent recovery"
    object_id = uuid4()

    try:
        nodes = create_cluster(
            factory,
            object_id,
            payload,
        )

        selected = select_replicas(
            object_id,
            nodes,
            3,
        )

        target = selected[0]
        source = selected[1]

        reset_fake_client()

        FakeClient.objects = {
            node.url: {}
            for node in nodes
        }

        FakeClient.objects[source.url][object_id] = payload

        with factory() as db:
            db.add(
                ObjectReplica(
                    object_id=object_id,
                    node_id=target.node_id,
                    state="FAILED",
                )
            )
            db.add(
                ObjectReplica(
                    object_id=object_id,
                    node_id=source.node_id,
                    state="ACTIVE",
                )
            )
            db.commit()

        monkeypatch.setattr(
            "coordinator.reconciliation.StorageNodeClient",
            FakeClient,
        )

        service = ReconciliationService(
            factory,
            replication_factor=3,
        )

        first_results = service.synchronize_recovered_node(
            target.node_id
        )

        first_put_count = FakeClient.put_call_count[
            target.url
        ]

        second_results = service.synchronize_recovered_node(
            target.node_id
        )

        second_put_count = FakeClient.put_call_count[
            target.url
        ]

        assert len(first_results) == 1
        assert first_results[0].repaired_node_ids == (
            target.node_id,
        )

        assert len(second_results) == 1
        assert second_results[0].repaired_node_ids == ()

        assert first_put_count == 1
        assert second_put_count == 1

        with factory() as db:
            replicas = (
                db.query(ObjectReplica)
                .filter_by(
                    object_id=object_id,
                    node_id=target.node_id,
                )
                .all()
            )

            assert len(replicas) == 1
            assert replicas[0].state == "ACTIVE"

    finally:
        engine.dispose()


def test_recovered_node_does_not_activate_when_destination_put_fails(
    monkeypatch,
):
    """A failed destination write must not activate recovered replica metadata."""

    engine, factory = create_session_factory()

    payload = b"destination failure"
    object_id = uuid4()

    try:
        nodes = create_cluster(
            factory,
            object_id,
            payload,
        )

        selected = select_replicas(
            object_id,
            nodes,
            3,
        )

        target = selected[0]
        source = selected[1]

        reset_fake_client()

        FakeClient.objects = {
            node.url: {}
            for node in nodes
        }

        FakeClient.objects[source.url][object_id] = payload
        FakeClient.put_failed_urls = {
            target.url,
        }

        with factory() as db:
            db.add(
                ObjectReplica(
                    object_id=object_id,
                    node_id=target.node_id,
                    state="FAILED",
                )
            )
            db.add(
                ObjectReplica(
                    object_id=object_id,
                    node_id=source.node_id,
                    state="ACTIVE",
                )
            )
            db.commit()

        monkeypatch.setattr(
            "coordinator.reconciliation.StorageNodeClient",
            FakeClient,
        )

        results = ReconciliationService(
            factory,
            replication_factor=3,
        ).synchronize_recovered_node(
            target.node_id
        )

        assert len(results) == 1
        assert target.node_id not in results[0].healthy_node_ids
        assert target.node_id not in results[0].repaired_node_ids

        with factory() as db:
            replica = (
                db.query(ObjectReplica)
                .filter_by(
                    object_id=object_id,
                    node_id=target.node_id,
                )
                .one()
            )

            assert replica.state == "FAILED"

        assert (
            object_id
            not in FakeClient.objects[target.url]
        )

    finally:
        engine.dispose()


def test_recovered_node_sync_is_noop_when_node_is_not_active(
    monkeypatch,
):
    """A node that has not recovered must not be synchronized."""

    engine, factory = create_session_factory()

    payload = b"node still failed"
    object_id = uuid4()

    try:
        nodes = create_cluster(
            factory,
            object_id,
            payload,
        )

        selected = select_replicas(
            object_id,
            nodes,
            3,
        )

        target = selected[0]

        reset_fake_client()

        FakeClient.objects = {
            node.url: {}
            for node in nodes
        }

        with factory() as db:
            target = db.get(
                StorageNode,
                target.node_id,
            )
            target.status = "FAILED"

            db.add(
                ObjectReplica(
                    object_id=object_id,
                    node_id=target.node_id,
                    state="FAILED",
                )
            )

            db.commit()

        monkeypatch.setattr(
            "coordinator.reconciliation.StorageNodeClient",
            FakeClient,
        )

        results = ReconciliationService(
            factory,
            replication_factor=3,
        ).synchronize_recovered_node(
            target.node_id
        )

        assert results == []

        assert FakeClient.put_call_count.get(
            target.url,
            0,
        ) == 0

        with factory() as db:
            replica = (
                db.query(ObjectReplica)
                .filter_by(
                    object_id=object_id,
                    node_id=target.node_id,
                )
                .one()
            )

            assert replica.state == "FAILED"

    finally:
        engine.dispose()