from uuid import uuid4

from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from coordinator.db import Base
from coordinator.models import ObjectMetadata, StorageNode
from coordinator.repository import ObjectReplicaRepository


def test_create_and_list_replicas_for_object():
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)

    object_id = uuid4()

    with Session(engine) as db:
        db.add(
            ObjectMetadata(
                object_id=object_id,
                object_key="photos/example.jpg",
                size=100,
                checksum="a" * 64,
            )
        )

        node_one = StorageNode(
            name="storage-node-1",
            url="http://node-1:8100",
        )

        node_two = StorageNode(
            name="storage-node-2",
            url="http://node-2:8100",
        )

        db.add_all(
            [
                node_one,
                node_two,
            ]
        )

        db.commit()

        repository = ObjectReplicaRepository(db)

        first = repository.create(
            object_id=object_id,
            node_id=node_one.node_id,
        )

        second = repository.create(
            object_id=object_id,
            node_id=node_two.node_id,
            state="ACTIVE",
        )

        replicas = repository.list_for_object(object_id)
        active = repository.list_active_for_object(object_id)

        replica_ids = {
            replica.replica_id
            for replica in replicas
        }

        replica_node_ids = {
            replica.node_id
            for replica in replicas
        }

        assert len(replicas) == 2
        assert replica_ids == {
            first.replica_id,
            second.replica_id,
        }
        assert replica_node_ids == {
            node_one.node_id,
            node_two.node_id,
        }

        assert {replica.state for replica in replicas} == {
            "PENDING",
            "ACTIVE",
        }

        assert len(active) == 1
        assert active[0].replica_id == second.replica_id
        assert active[0].node_id == node_two.node_id
        assert active[0].state == "ACTIVE"