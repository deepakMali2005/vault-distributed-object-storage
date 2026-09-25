from uuid import uuid4

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from coordinator.db import Base
from coordinator.repository import ObjectNotFoundError, ObjectRepository


def test_create_and_get_object_metadata():
    engine = create_engine("sqlite://")

    Base.metadata.create_all(engine)

    object_id = uuid4()

    with Session(engine) as db:
        repository = ObjectRepository(db)

        metadata = repository.create(
            object_id=object_id,
            object_key="documents/test.txt",
            size=12,
            checksum="abc123",
        )

        assert metadata.object_id == object_id
        assert metadata.object_key == "documents/test.txt"
        assert metadata.size == 12
        assert metadata.checksum == "abc123"

        loaded = repository.get_by_key("documents/test.txt")

        assert loaded.object_id == object_id


def test_missing_object_raises():
    engine = create_engine("sqlite://")

    Base.metadata.create_all(engine)

    with Session(engine) as db:
        repository = ObjectRepository(db)

        with pytest.raises(ObjectNotFoundError):
            repository.get_by_key("missing.txt")


def test_list_objects():
    engine = create_engine("sqlite://")

    Base.metadata.create_all(engine)

    with Session(engine) as db:
        repository = ObjectRepository(db)

        repository.create(
            object_id=uuid4(),
            object_key="a.txt",
            size=1,
            checksum="a",
        )

        repository.create(
            object_id=uuid4(),
            object_key="b.txt",
            size=2,
            checksum="b",
        )

        objects = repository.list_all()

        assert len(objects) == 2
        assert {obj.object_key for obj in objects} == {
            "a.txt",
            "b.txt",
        }


def test_delete_object():
    engine = create_engine("sqlite://")

    Base.metadata.create_all(engine)

    object_id = uuid4()

    with Session(engine) as db:
        repository = ObjectRepository(db)

        repository.create(
            object_id=object_id,
            object_key="delete.txt",
            size=1,
            checksum="abc",
        )

        repository.delete(object_id)

        with pytest.raises(ObjectNotFoundError):
            repository.get_by_id(object_id)