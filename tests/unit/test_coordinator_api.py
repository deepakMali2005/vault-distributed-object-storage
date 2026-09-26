from contextlib import contextmanager
from uuid import UUID

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from coordinator import main
from coordinator.db import Base
from coordinator.models import ObjectReplica, StorageNode
from coordinator.storage_client import StorageNodeError


class FakeStorageResponse:
    """Minimal response used by coordinator API tests."""

    def __init__(self, payload: bytes = b"", status_code: int = 200) -> None:
        self.payload = payload
        self.status_code = status_code
        self.headers = {
            "content-length": str(len(payload)),
        }

    def iter_bytes(self, _chunk_size: int):
        yield self.payload

    def close(self) -> None:
        pass


class FakeStorageNodeClient:
    """In-memory stand-in for storage nodes during coordinator API tests."""

    objects_by_node: dict[str, dict[UUID, bytes]] = {}
    failed_urls: set[str] = set()
    get_failed_urls: set[str] = set()

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.objects_by_node.setdefault(
            self.base_url,
            {},
        )

    def close(self) -> None:
        pass

    def _raise_if_failed(self, operation: str) -> None:
        if self.base_url in self.failed_urls:
            raise StorageNodeError(
                f"simulated storage-node {operation} failure"
            )

    def put_object(
        self,
        object_id,
        file,
        filename=None,
        content_type=None,
    ):
        self._raise_if_failed("PUT")

        payload = file.read()

        self.objects_by_node[
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

    def head_object(self, object_id):
        self._raise_if_failed("HEAD")

        objects = self.objects_by_node[
            self.base_url
        ]

        if object_id not in objects:
            raise StorageNodeError(
                "Object not found on storage node",
                status_code=404,
            )

        return FakeStorageResponse(
            objects[object_id],
        )

    @contextmanager
    def stream_object(self, object_id):
        if self.base_url in self.get_failed_urls:
            raise StorageNodeError(
                "simulated storage-node GET failure"
            )

        self._raise_if_failed("GET")

        objects = self.objects_by_node[
            self.base_url
        ]

        if object_id not in objects:
            raise StorageNodeError(
                "Object not found on storage node",
                status_code=404,
            )

        yield FakeStorageResponse(
            objects[object_id]
        )

    def delete_object(self, object_id):
        self._raise_if_failed("DELETE")

        objects = self.objects_by_node[
            self.base_url
        ]

        if object_id not in objects:
            raise StorageNodeError(
                "Object not found on storage node",
                status_code=404,
            )

        del objects[object_id]


def create_test_client(monkeypatch):
    """Create a FastAPI test client backed by a shared SQLite database."""

    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    Base.metadata.create_all(engine)

    session_factory = sessionmaker(
        bind=engine,
        class_=Session,
    )

    with session_factory() as db:
        db.add_all(
            [
                StorageNode(
                    name="storage-node-1",
                    url="http://test-storage-1",
                    status="ACTIVE",
                ),
                StorageNode(
                    name="storage-node-2",
                    url="http://test-storage-2",
                    status="ACTIVE",
                ),
                StorageNode(
                    name="storage-node-3",
                    url="http://test-storage-3",
                    status="ACTIVE",
                ),
            ]
        )

        db.commit()

    def override_get_db():
        with session_factory() as db:
            yield db

    monkeypatch.setattr(
        main,
        "init_db",
        lambda: None,
    )

    monkeypatch.setattr(
        main,
        "StorageNodeClient",
        FakeStorageNodeClient,
    )

    monkeypatch.setattr(
        main.settings,
        "replication_factor",
        3,
    )

    main.app.dependency_overrides[
        main.get_db
    ] = override_get_db

    FakeStorageNodeClient.objects_by_node = {}
    FakeStorageNodeClient.failed_urls = set()
    FakeStorageNodeClient.get_failed_urls = set()

    return TestClient(main.app), engine


def test_object_lifecycle(monkeypatch):
    client, engine = create_test_client(monkeypatch)
    payload = b"Hello VAULT\n"

    try:
        put_response = client.put(
            "/objects/demo/hello.txt",
            files={
                "file": (
                    "object.txt",
                    payload,
                    "text/plain",
                )
            },
        )

        assert put_response.status_code == 200

        body = put_response.json()

        assert body["object_key"] == "demo/hello.txt"
        assert body["size"] == len(payload)
        assert UUID(body["object_id"])

        object_id = UUID(body["object_id"])

        for objects in (
            FakeStorageNodeClient
            .objects_by_node
            .values()
        ):
            assert objects[object_id] == payload

        replica_response = client.get(
            f"/internal/objects/{object_id}/replicas"
        )

        assert replica_response.status_code == 200

        replicas = replica_response.json()["replicas"]

        assert len(replicas) == 3
        assert {
            replica["state"]
            for replica in replicas
        } == {"ACTIVE"}

        head_response = client.head(
            "/objects/demo/hello.txt"
        )

        assert head_response.status_code == 200
        assert (
            head_response.headers["content-length"]
            == str(len(payload))
        )
        assert (
            head_response.headers["etag"]
            == f'"{body["checksum"]}"'
        )
        assert head_response.content == b""

        get_response = client.get(
            "/objects/demo/hello.txt"
        )

        assert get_response.status_code == 200
        assert get_response.content == payload

        list_response = client.get("/objects")

        assert list_response.status_code == 200

        assert [
            obj["object_key"]
            for obj in list_response.json()["objects"]
        ] == ["demo/hello.txt"]

        delete_response = client.delete(
            "/objects/demo/hello.txt"
        )

        assert delete_response.status_code == 204

        assert all(
            not objects
            for objects in FakeStorageNodeClient.objects_by_node.values()
        )

        missing_response = client.get(
            "/objects/demo/hello.txt"
        )

        assert missing_response.status_code == 404

    finally:
        main.app.dependency_overrides.clear()
        engine.dispose()


def test_partial_replication_returns_error_and_records_replica_states(
    monkeypatch,
):
    client, engine = create_test_client(monkeypatch)

    payload = b"partial replication"

    FakeStorageNodeClient.failed_urls = {
        "http://test-storage-2"
    }

    try:
        response = client.put(
            "/objects/demo/partial.txt",
            files={
                "file": (
                    "object.txt",
                    payload,
                )
            },
        )

        assert response.status_code == 502

        assert response.json()["detail"] == (
            "Object replication failed: "
            "2/3 replicas persisted"
        )

        object_list = client.get("/objects")

        assert object_list.status_code == 200

        object_id = UUID(
            object_list.json()["objects"][0]["object_id"]
        )

        replica_response = client.get(
            f"/internal/objects/{object_id}/replicas"
        )

        assert replica_response.status_code == 200

        states = [
            replica["state"]
            for replica in replica_response.json()["replicas"]
        ]

        assert states.count("ACTIVE") == 2
        assert states.count("FAILED") == 1

        assert object_id in (
            FakeStorageNodeClient
            .objects_by_node[
                "http://test-storage-1"
            ]
        )

        assert object_id not in (
            FakeStorageNodeClient
            .objects_by_node[
                "http://test-storage-2"
            ]
        )

        assert object_id in (
            FakeStorageNodeClient
            .objects_by_node[
                "http://test-storage-3"
            ]
        )

    finally:
        main.app.dependency_overrides.clear()
        engine.dispose()


def test_get_falls_back_when_selected_replica_fails_during_get(monkeypatch):
    client, engine = create_test_client(monkeypatch)
    payload = b"replica fallback"

    try:
        put_response = client.put(
            "/objects/demo/fallback.txt",
            files={"file": ("object.txt", payload)},
        )
        assert put_response.status_code == 200

        FakeStorageNodeClient.get_failed_urls = {
            "http://test-storage-1"
        }

        response = client.get("/objects/demo/fallback.txt")

        assert response.status_code == 200
        assert response.content == payload

    finally:
        main.app.dependency_overrides.clear()
        engine.dispose()


def test_get_returns_service_unavailable_when_all_replicas_fail(monkeypatch):
    client, engine = create_test_client(monkeypatch)
    payload = b"unavailable replicas"

    try:
        put_response = client.put(
            "/objects/demo/unavailable.txt",
            files={"file": ("object.txt", payload)},
        )
        assert put_response.status_code == 200

        FakeStorageNodeClient.failed_urls = {
            "http://test-storage-1",
            "http://test-storage-2",
            "http://test-storage-3",
        }

        response = client.get("/objects/demo/unavailable.txt")

        assert response.status_code == 503
        assert response.json()["detail"] == (
            "No active replica is available for this object"
        )

    finally:
        main.app.dependency_overrides.clear()
        engine.dispose()


def test_head_falls_back_to_another_active_replica(monkeypatch):
    client, engine = create_test_client(monkeypatch)
    payload = b"head fallback"

    try:
        put_response = client.put(
            "/objects/demo/head.txt",
            files={"file": ("object.txt", payload)},
        )
        assert put_response.status_code == 200
        checksum = put_response.json()["checksum"]

        FakeStorageNodeClient.failed_urls = {
            "http://test-storage-1"
        }

        response = client.head("/objects/demo/head.txt")

        assert response.status_code == 200
        assert response.headers["content-length"] == str(len(payload))
        assert response.headers["etag"] == f'"{checksum}"'
        assert response.content == b""

    finally:
        main.app.dependency_overrides.clear()
        engine.dispose()


def test_delete_propagates_to_all_replicas(monkeypatch):
    client, engine = create_test_client(monkeypatch)
    payload = b"delete replicas"

    try:
        put_response = client.put(
            "/objects/demo/delete.txt",
            files={"file": ("object.txt", payload)},
        )
        assert put_response.status_code == 200
        object_id = UUID(put_response.json()["object_id"])

        response = client.delete("/objects/demo/delete.txt")

        assert response.status_code == 204
        assert all(
            object_id not in objects
            for objects in FakeStorageNodeClient.objects_by_node.values()
        )

        replica_response = client.get(
            f"/internal/objects/{object_id}/replicas"
        )
        assert replica_response.json()["replicas"] == []

        assert client.get("/objects/demo/delete.txt").status_code == 404

    finally:
        main.app.dependency_overrides.clear()
        engine.dispose()


def test_partial_delete_preserves_failed_replica_metadata(monkeypatch):
    client, engine = create_test_client(monkeypatch)
    payload = b"partial delete"

    try:
        put_response = client.put(
            "/objects/demo/partial-delete.txt",
            files={"file": ("object.txt", payload)},
        )
        assert put_response.status_code == 200
        object_id = UUID(put_response.json()["object_id"])

        FakeStorageNodeClient.failed_urls = {
            "http://test-storage-1"
        }

        response = client.delete("/objects/demo/partial-delete.txt")

        assert response.status_code == 502
        assert response.json()["detail"] == (
            "Object deletion incomplete: 1 replica(s) could not be deleted"
        )

        assert object_id in FakeStorageNodeClient.objects_by_node[
            "http://test-storage-1"
        ]
        assert object_id not in FakeStorageNodeClient.objects_by_node[
            "http://test-storage-2"
        ]
        assert object_id not in FakeStorageNodeClient.objects_by_node[
            "http://test-storage-3"
        ]

        replica_response = client.get(
            f"/internal/objects/{object_id}/replicas"
        )
        replicas = replica_response.json()["replicas"]

        assert len(replicas) == 1
        assert replicas[0]["state"] == "ACTIVE"

        FakeStorageNodeClient.failed_urls = set()

        retry_response = client.delete("/objects/demo/partial-delete.txt")

        assert retry_response.status_code == 204
        assert client.get("/objects/demo/partial-delete.txt").status_code == 404

    finally:
        main.app.dependency_overrides.clear()
        engine.dispose()


def test_delete_attempts_all_replica_records_not_only_active_records(monkeypatch):
    client, engine = create_test_client(monkeypatch)
    payload = b"delete every replica record"

    try:
        put_response = client.put(
            "/objects/demo/all-replicas.txt",
            files={"file": ("object.txt", payload)},
        )
        assert put_response.status_code == 200
        object_id = UUID(put_response.json()["object_id"])

        replica_response = client.get(
            f"/internal/objects/{object_id}/replicas"
        )
        replicas = replica_response.json()["replicas"]
        assert len(replicas) == 3

        # Simulate a stale FAILED metadata state while the physical copy still
        # exists. DELETE must still attempt this replica instead of silently
        # leaving its bytes behind.
        failed_replica_id = replicas[0]["replica_id"]
        with Session(engine) as db:
            replica = db.get(
                ObjectReplica,
                UUID(failed_replica_id),
            )
            assert replica is not None
            replica.state = "FAILED"
            db.commit()

        response = client.delete("/objects/demo/all-replicas.txt")

        assert response.status_code == 204
        assert all(
            object_id not in objects
            for objects in FakeStorageNodeClient.objects_by_node.values()
        )
        assert client.get("/objects/demo/all-replicas.txt").status_code == 404

        replica_response = client.get(
            f"/internal/objects/{object_id}/replicas"
        )
        assert replica_response.json()["replicas"] == []

    finally:
        main.app.dependency_overrides.clear()
        engine.dispose()


def test_duplicate_object_key_returns_conflict(monkeypatch):
    client, engine = create_test_client(monkeypatch)
    payload = b"duplicate"

    try:
        first = client.put(
            "/objects/demo/duplicate.txt",
            files={
                "file": (
                    "object.txt",
                    payload,
                )
            },
        )

        second = client.put(
            "/objects/demo/duplicate.txt",
            files={
                "file": (
                    "object.txt",
                    payload,
                )
            },
        )

        assert first.status_code == 200
        assert second.status_code == 409

    finally:
        main.app.dependency_overrides.clear()
        engine.dispose()


def test_missing_object_returns_404(monkeypatch):
    client, engine = create_test_client(monkeypatch)

    try:
        assert (
            client.get(
                "/objects/missing.txt"
            ).status_code
            == 404
        )

        assert (
            client.head(
                "/objects/missing.txt"
            ).status_code
            == 404
        )

        assert (
            client.delete(
                "/objects/missing.txt"
            ).status_code
            == 404
        )

    finally:
        main.app.dependency_overrides.clear()
        engine.dispose()
