"""FastAPI application for the VAULT coordinator."""

from collections.abc import Iterator
from contextlib import asynccontextmanager
from hashlib import sha256
from tempfile import SpooledTemporaryFile
from uuid import UUID, uuid4

from fastapi import Depends, FastAPI, File, HTTPException, UploadFile, status
from fastapi.responses import Response, StreamingResponse
from sqlalchemy.orm import Session

from coordinator.config import settings
from coordinator.db import get_db, init_db
from coordinator.placement import PlacementError, select_replicas
from coordinator.repository import (
    ObjectNotFoundError,
    ObjectReplicaRepository,
    ObjectRepository,
    StorageNodeRepository,
)
from coordinator.schemas import (
    ObjectListResponse,
    ObjectMetadataResponse,
    ObjectReplicaListResponse,
    ObjectReplicaResponse,
    StorageNodeListResponse,
    StorageNodeResponse,
)
from coordinator.storage_client import StorageNodeClient, StorageNodeError


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """Initialize coordinator infrastructure when the service starts."""

    init_db()

    db = next(get_db())
    try:
        repository = StorageNodeRepository(db)

        for index, url in enumerate(settings.configured_storage_nodes(), start=1):
            repository.upsert(
                name=f"storage-node-{index}",
                url=url,
                status="ACTIVE",
            )
    finally:
        db.close()

    yield


app = FastAPI(
    title="VAULT Coordinator",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


def _buffer_upload(file: UploadFile) -> tuple[SpooledTemporaryFile, int, str]:
    """Buffer an upload into a rewindable temporary stream and calculate its checksum."""

    buffered = SpooledTemporaryFile(
        max_size=8 * 1024 * 1024,
        mode="w+b",
    )
    digest = sha256()
    size = 0

    try:
        while True:
            chunk = file.file.read(1024 * 1024)

            if not chunk:
                break

            buffered.write(chunk)
            digest.update(chunk)
            size += len(chunk)

        buffered.seek(0)

        return buffered, size, digest.hexdigest()

    except Exception:
        buffered.close()
        raise


def _write_replicas(
    *,
    object_id: UUID,
    buffered: SpooledTemporaryFile,
    selected_nodes,
    replica_records,
    expected_size: int,
    expected_checksum: str,
    replica_repository: ObjectReplicaRepository,
) -> int:
    """Write an object to every planned replica and persist each result state."""

    successful_writes = 0

    for node, replica in zip(
        selected_nodes,
        replica_records,
        strict=True,
    ):
        client = StorageNodeClient(node.url)

        try:
            buffered.seek(0)

            result = client.put_object(
                object_id,
                buffered,
            )

            if (
                result.get("size") != expected_size
                or result.get("checksum") != expected_checksum
            ):
                raise StorageNodeError(
                    "Storage node returned object metadata that does not match "
                    "the uploaded object"
                )

            replica_repository.update_state(
                replica.replica_id,
                "ACTIVE",
            )

            successful_writes += 1

        except StorageNodeError:
            replica_repository.update_state(
                replica.replica_id,
                "FAILED",
            )

        finally:
            client.close()

    return successful_writes


@app.put(
    "/objects/{object_key:path}",
    response_model=ObjectMetadataResponse,
)
def put_object(
    object_key: str,
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
) -> ObjectMetadataResponse:
    if not object_key:
        raise HTTPException(
            status_code=400,
            detail="Object key cannot be empty",
        )

    object_repository = ObjectRepository(db)
    replica_repository = ObjectReplicaRepository(db)
    node_repository = StorageNodeRepository(db)

    try:
        object_repository.get_by_key(object_key)
    except ObjectNotFoundError:
        pass
    else:
        raise HTTPException(
            status_code=409,
            detail="Object key already exists",
        )

    object_id = uuid4()

    active_nodes = node_repository.list_active()

    try:
        selected_nodes = select_replicas(
            object_id,
            active_nodes,
            settings.replication_factor,
        )
    except PlacementError as exc:
        raise HTTPException(
            status_code=503,
            detail=str(exc),
        ) from exc

    try:
        buffered, size, checksum = _buffer_upload(file)
    finally:
        file.file.close()

    try:
        metadata = object_repository.create(
            object_id=object_id,
            object_key=object_key,
            size=size,
            checksum=checksum,
        )

        replica_records = replica_repository.create_many(
            object_id=object_id,
            node_ids=[
                node.node_id
                for node in selected_nodes
            ],
        )

        successful_writes = _write_replicas(
            object_id=object_id,
            buffered=buffered,
            selected_nodes=selected_nodes,
            replica_records=replica_records,
            expected_size=size,
            expected_checksum=checksum,
            replica_repository=replica_repository,
        )

    finally:
        buffered.close()

    if successful_writes != len(selected_nodes):
        raise HTTPException(
            status_code=502,
            detail=(
                "Object replication failed: "
                f"{successful_writes}/{len(selected_nodes)} replicas persisted"
            ),
        )

    return ObjectMetadataResponse.model_validate(metadata)


@app.get("/objects/{object_key:path}")
def get_object(
    object_key: str,
    db: Session = Depends(get_db),
) -> StreamingResponse:
    object_repository = ObjectRepository(db)

    try:
        metadata = object_repository.get_by_key(object_key)
    except ObjectNotFoundError as exc:
        raise HTTPException(
            status_code=404,
            detail="Object not found",
        ) from exc

    client = StorageNodeClient(settings.storage_node_url)

    def body() -> Iterator[bytes]:
        try:
            with client.stream_object(metadata.object_id) as response:
                yield from response.iter_bytes(1024 * 1024)
        finally:
            client.close()

    return StreamingResponse(
        body(),
        media_type="application/octet-stream",
        headers={
            "Content-Length": str(metadata.size),
            "ETag": f'"{metadata.checksum}"',
        },
    )


@app.head("/objects/{object_key:path}")
def head_object(
    object_key: str,
    db: Session = Depends(get_db),
) -> Response:
    object_repository = ObjectRepository(db)

    try:
        metadata = object_repository.get_by_key(object_key)
    except ObjectNotFoundError as exc:
        raise HTTPException(
            status_code=404,
            detail="Object not found",
        ) from exc

    return Response(
        status_code=200,
        headers={
            "Content-Length": str(metadata.size),
            "ETag": f'"{metadata.checksum}"',
        },
    )


@app.delete(
    "/objects/{object_key:path}",
    status_code=status.HTTP_204_NO_CONTENT,
)
def delete_object(
    object_key: str,
    db: Session = Depends(get_db),
) -> None:
    object_repository = ObjectRepository(db)

    try:
        metadata = object_repository.get_by_key(object_key)
    except ObjectNotFoundError as exc:
        raise HTTPException(
            status_code=404,
            detail="Object not found",
        ) from exc

    client = StorageNodeClient(settings.storage_node_url)

    try:
        client.delete_object(metadata.object_id)
    except StorageNodeError as exc:
        raise HTTPException(
            status_code=502,
            detail=str(exc),
        ) from exc
    finally:
        client.close()

    object_repository.delete(metadata.object_id)


@app.get(
    "/objects",
    response_model=ObjectListResponse,
)
def list_objects(
    db: Session = Depends(get_db),
) -> ObjectListResponse:
    object_repository = ObjectRepository(db)

    return ObjectListResponse(
        objects=[
            ObjectMetadataResponse.model_validate(obj)
            for obj in object_repository.list_all()
        ]
    )


@app.get(
    "/internal/storage-nodes",
    response_model=StorageNodeListResponse,
)
def list_storage_nodes(
    db: Session = Depends(get_db),
) -> StorageNodeListResponse:
    """Return storage nodes known to the coordinator."""

    repository = StorageNodeRepository(db)

    return StorageNodeListResponse(
        nodes=[
            StorageNodeResponse.model_validate(node)
            for node in repository.list_all()
        ]
    )


@app.get(
    "/internal/objects/{object_id}/replicas",
    response_model=ObjectReplicaListResponse,
)
def list_object_replicas(
    object_id: UUID,
    db: Session = Depends(get_db),
) -> ObjectReplicaListResponse:
    """Return replica state for an object."""

    repository = ObjectReplicaRepository(db)

    return ObjectReplicaListResponse(
        replicas=[
            ObjectReplicaResponse.model_validate(replica)
            for replica in repository.list_for_object(object_id)
        ]
    )