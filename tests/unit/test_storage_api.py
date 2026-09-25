from uuid import uuid4

from fastapi.testclient import TestClient

from storage_node import main
from storage_node.storage import ObjectStorage


def test_object_lifecycle(tmp_path, monkeypatch):
    monkeypatch.setattr(main, "storage", ObjectStorage(tmp_path))
    client = TestClient(main.app)
    object_id = uuid4()
    payload = b"Hello VAULT\n"

    put_response = client.put(
        f"/objects/{object_id}",
        files={"file": ("object.txt", payload, "text/plain")},
    )

    assert put_response.status_code == 200
    assert put_response.json()["object_id"] == str(object_id)
    assert put_response.json()["size"] == len(payload)

    head_response = client.head(f"/objects/{object_id}")
    assert head_response.status_code == 200
    assert head_response.headers["content-length"] == str(len(payload))
    assert head_response.content == b""

    get_response = client.get(f"/objects/{object_id}")
    assert get_response.status_code == 200
    assert get_response.content == payload

    delete_response = client.delete(f"/objects/{object_id}")
    assert delete_response.status_code == 204

    missing_response = client.get(f"/objects/{object_id}")
    assert missing_response.status_code == 404


def test_missing_object_returns_404(tmp_path, monkeypatch):
    monkeypatch.setattr(main, "storage", ObjectStorage(tmp_path))
    client = TestClient(main.app)
    object_id = uuid4()

    assert client.head(f"/objects/{object_id}").status_code == 404
    assert client.get(f"/objects/{object_id}").status_code == 404
    assert client.delete(f"/objects/{object_id}").status_code == 404