"""FastAPI application for a VAULT storage node."""

from uuid import UUID

from fastapi import FastAPI, HTTPException, UploadFile, status
from fastapi.responses import FileResponse, Response

from storage_node.config import settings
from storage_node.schemas import StoredObject
from storage_node.storage import ObjectNotFoundError, ObjectStorage


app = FastAPI(
    title="VAULT Storage Node",
    version="0.1.0",
)

storage = ObjectStorage(settings.data_dir)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.put("/objects/{object_id}", response_model=StoredObject)
def put_object(object_id: UUID, file: UploadFile) -> StoredObject:
    """Persist an object using its internal UUID as its physical identity."""

    try:
        size, checksum = storage.store(object_id, file.file)
    finally:
        file.file.close()

    return StoredObject(
        object_id=object_id,
        size=size,
        checksum=checksum,
    )


@app.get("/objects/{object_id}")
def get_object(object_id: UUID) -> FileResponse:
    """Return the raw object bytes."""

    path = storage.path_for(object_id)

    if not path.is_file():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Object not found",
        )

    return FileResponse(
        path,
        media_type="application/octet-stream",
        filename=str(object_id),
    )


@app.head("/objects/{object_id}")
def head_object(object_id: UUID) -> Response:
    """Check whether an object exists without returning its bytes."""

    try:
        size = storage.size(object_id)
    except ObjectNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Object not found",
        ) from exc

    return Response(
        status_code=status.HTTP_200_OK,
        headers={"Content-Length": str(size)},
    )


@app.delete(
    "/objects/{object_id}",
    status_code=status.HTTP_204_NO_CONTENT,
)
def delete_object(object_id: UUID) -> None:
    """Delete an object from the node."""

    try:
        storage.delete(object_id)
    except ObjectNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Object not found",
        ) from exc