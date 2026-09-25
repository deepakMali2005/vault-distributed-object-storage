import hashlib
from io import BytesIO
from uuid import uuid4

import pytest

from storage_node.storage import ObjectNotFoundError, ObjectStorage


def test_store_returns_size_and_sha256(tmp_path):
    storage = ObjectStorage(tmp_path)
    object_id = uuid4()
    payload = b"Hello VAULT\n"

    size, checksum = storage.store(object_id, BytesIO(payload))

    assert size == len(payload)
    assert checksum == hashlib.sha256(payload).hexdigest()
    assert storage.path_for(object_id).read_bytes() == payload


def test_object_exists_and_can_be_deleted(tmp_path):
    storage = ObjectStorage(tmp_path)
    object_id = uuid4()

    storage.store(object_id, BytesIO(b"data"))

    assert storage.exists(object_id)
    assert storage.size(object_id) == 4

    storage.delete(object_id)

    assert not storage.exists(object_id)


def test_missing_object_raises(tmp_path):
    storage = ObjectStorage(tmp_path)
    object_id = uuid4()

    with pytest.raises(ObjectNotFoundError):
        storage.open(object_id)