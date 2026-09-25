"""FastAPI application for the VAULT coordinator."""

from collections.abc import Iterator
from contextlib import asynccontextmanager
from uuid import uuid4

from fastapi import Depends, FastAPI, File, HTTPException, UploadFile, status
from fastapi.responses import Response, StreamingResponse
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from coordinator.config import settings
from coordinator.db import get_db, init_db
from coordinator.repository import ObjectNotFoundError, ObjectRepository
from coordinator.schemas import ObjectListResponse, ObjectMetadataResponse
from coordinator.storage_client import StorageNodeClient, StorageNodeError


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """Initialize coordinator infrastructure when the service starts."""

    init_db()
    yield


app = FastAPI(
    title="VAULT Coordinator",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


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

    repository = ObjectRepository(db)

    try:
        repository.get_by_key(object_key)
    except ObjectNotFoundError:
        pass
    else:
        raise HTTPException(
            status_code=409,
            detail="Object key already exists",
        )

    object_id = uuid4()
    client = StorageNodeClient(settings.storage_node_url)

    try:
        result = client.put_object(
            object_id,
            file.file,
            file.filename,
            file.content_type,
        )
    except StorageNodeError as exc:
        raise HTTPException(
            status_code=502,
            detail=str(exc),
        ) from exc
    finally:
        client.close()
        file.file.close()

    try:
        metadata = repository.create(
            object_id=object_id,
            object_key=object_key,
            size=result["size"],
            checksum=result["checksum"],
        )
    except IntegrityError as exc:
        db.rollback()
        cleanup = StorageNodeClient(settings.storage_node_url)

        try:
            cleanup.delete_object(object_id)
        except StorageNodeError:
            pass
        finally:
            cleanup.close()

        raise HTTPException(
            status_code=409,
            detail="Object key already exists",
        ) from exc
    except Exception:
        db.rollback()
        cleanup = StorageNodeClient(settings.storage_node_url)

        try:
            cleanup.delete_object(object_id)
        except StorageNodeError:
            pass
        finally:
            cleanup.close()

        raise

    return ObjectMetadataResponse.model_validate(metadata)


@app.get("/objects/{object_key:path}")
def get_object(
    object_key: str,
    db: Session = Depends(get_db),
) -> StreamingResponse:
    repository = ObjectRepository(db)

    try:
        metadata = repository.get_by_key(object_key)
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
    repository = ObjectRepository(db)

    try:
        metadata = repository.get_by_key(object_key)
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
    repository = ObjectRepository(db)

    try:
        metadata = repository.get_by_key(object_key)
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

    repository.delete(metadata.object_id)


@app.get(
    "/objects",
    response_model=ObjectListResponse,
)
def list_objects(
    db: Session = Depends(get_db),
) -> ObjectListResponse:
    repository = ObjectRepository(db)

    return ObjectListResponse(
        objects=[
            ObjectMetadataResponse.model_validate(obj)
            for obj in repository.list_all()
        ]
    )