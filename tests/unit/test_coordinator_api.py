from contextlib import contextmanager
from uuid import UUID

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from coordinator import main
from coordinator.db import Base
from coordinator.storage_client import StorageNodeError


class FakeStorageResponse:
    """Minimal streaming response used by coordinator API tests."""

    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def iter_bytes(self, _chunk_size: int):
        yield self.payload


class FakeStorageNodeClient:
    """In-memory stand-in for the storage node during API tests."""

    objects: dict[UUID, bytes] = {}

    def __init__(self, _base_url: str) -> None:
        pass

    def close(self) -> None:
        pass

    def put_object(
        self,
        object_id,
        file,
        filename=None,
        content_type=None,
    ):
        payload = file.read()
        self.objects[object_id] = payload

        import hashlib

        return {
            "object_id": str(object_id),
            "size": len(payload),
            "checksum": hashlib.sha256(payload).hexdigest(),
        }

    @contextmanager
    def stream_object(self, object_id):
        if object_id not in self.objects:
            raise StorageNodeError("Object not found on storage node")

        yield FakeStorageResponse(self.objects[object_id])

    def delete_object(self, object_id):
        if object_id not in self.objects:
            raise StorageNodeError("Object not found on storage node")

        del self.objects[object_id]


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

    def override_get_db():
        with session_factory() as db:
            yield db

    monkeypatch.setattr(main, "init_db", lambda: None)
    monkeypatch.setattr(main, "StorageNodeClient", FakeStorageNodeClient)
    monkeypatch.setattr(
        main.settings,
        "storage_node_url",
        "http://test-storage",
    )

    main.app.dependency_overrides[main.get_db] = override_get_db
    FakeStorageNodeClient.objects = {}

    return TestClient(main.app), engine


def test_object_lifecycle(monkeypatch):
    client, engine = create_test_client(monkeypatch)
    payload = b"Hello VAULT\n"

    try:
        put_response = client.put(
            "/objects/demo/hello.txt",
            files={"file": ("object.txt", payload, "text/plain")},
        )

        assert put_response.status_code == 200

        body = put_response.json()

        assert body["object_key"] == "demo/hello.txt"
        assert body["size"] == len(payload)
        assert UUID(body["object_id"])

        head_response = client.head(
            "/objects/demo/hello.txt"
        )

        assert head_response.status_code == 200
        assert head_response.headers["content-length"] == str(len(payload))
        assert head_response.headers["etag"] == f'"{body["checksum"]}"'
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

        missing_response = client.get(
            "/objects/demo/hello.txt"
        )

        assert missing_response.status_code == 404

    finally:
        main.app.dependency_overrides.clear()
        engine.dispose()


def test_duplicate_object_key_returns_conflict(monkeypatch):
    client, engine = create_test_client(monkeypatch)
    payload = b"duplicate"

    try:
        first = client.put(
            "/objects/demo/duplicate.txt",
            files={"file": ("object.txt", payload)},
        )

        second = client.put(
            "/objects/demo/duplicate.txt",
            files={"file": ("object.txt", payload)},
        )

        assert first.status_code == 200
        assert second.status_code == 409
        assert second.json()["detail"] == "Object key already exists"
        assert len(FakeStorageNodeClient.objects) == 1

    finally:
        main.app.dependency_overrides.clear()
        engine.dispose()


def test_missing_object_returns_404(monkeypatch):
    client, engine = create_test_client(monkeypatch)

    try:
        assert client.get("/objects/missing.txt").status_code == 404
        assert client.head("/objects/missing.txt").status_code == 404
        assert client.delete("/objects/missing.txt").status_code == 404

    finally:
        main.app.dependency_overrides.clear()
        engine.dispose()