"""FastAPI application for the VAULT coordinator."""

from collections.abc import Iterator
import asyncio
from contextlib import ExitStack, asynccontextmanager
from hashlib import sha256
from tempfile import SpooledTemporaryFile
from uuid import UUID, uuid4

from fastapi import Depends, FastAPI, File, HTTPException, UploadFile, status
from fastapi.responses import Response, StreamingResponse
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from coordinator.config import settings
from coordinator.db import SessionLocal, get_db, init_db
from coordinator.health import StorageNodeHealthChecker, run_health_monitor
from coordinator.placement import PlacementError, select_replicas
from coordinator.repair import ReplicaRepairService
from coordinator.reconciliation import ReconciliationService
from coordinator.repository import (
    ObjectNotFoundError,
    ObjectReplicaRepository,
    ObjectRepository,
    StorageNodeNotFoundError,
    StorageNodeRepository,
)
from coordinator.schemas import (
    ObjectListResponse,
    ObjectMetadataResponse,
    ObjectReplicaListResponse,
    ObjectReplicaResponse,
    RepairResultListResponse,
    RepairResultResponse,
    PhysicalOrphanResponse,
    ReconciliationResultListResponse,
    ReconciliationResultResponse,
    StorageNodeListResponse,
    StorageNodeResponse,
    UnderReplicatedObjectListResponse,
    UnderReplicatedObjectResponse,
)
from coordinator.storage_client import StorageNodeClient, StorageNodeError


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """Initialize coordinator infrastructure and monitor node health."""

    init_db()

    db = next(get_db())

    try:
        repository = StorageNodeRepository(db)

        for index, url in enumerate(
            settings.configured_storage_nodes(),
            start=1,
        ):
            repository.upsert(
                name=f"storage-node-{index}",
                url=url,
                status="ACTIVE",
            )

    finally:
        db.close()

    health_checker = StorageNodeHealthChecker(
        SessionLocal,
        timeout_seconds=settings.health_check_timeout_seconds,
    )

    def repair_after_health_check() -> None:
        """Repair new failures and synchronize nodes that have recovered."""

        if health_checker.newly_failed_node_ids:
            ReplicaRepairService(
                SessionLocal,
                replication_factor=settings.replication_factor,
            ).repair_all()

        recovered_node_ids = health_checker.recovered_node_ids

        if not recovered_node_ids:
            return

        reconciliation_service = ReconciliationService(
            SessionLocal,
            replication_factor=settings.replication_factor,
        )

        for node_id in recovered_node_ids:
            reconciliation_service.synchronize_recovered_node(
                node_id
            )

    health_task = asyncio.create_task(
        run_health_monitor(
            health_checker,
            interval_seconds=settings.health_check_interval_seconds,
            after_check=repair_after_health_check,
        )
    )

    try:
        yield

    finally:
        health_task.cancel()

        try:
            await health_task
        except asyncio.CancelledError:
            pass


app = FastAPI(
    title="VAULT Coordinator",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


def _buffer_upload(
    file: UploadFile,
) -> tuple[SpooledTemporaryFile, int, str]:
    """Buffer an upload into a rewindable temporary stream and calculate its checksum."""

    buffered = SpooledTemporaryFile(
        max_size=8 * 1024 * 1024,
        mode="w+b",
    )

    digest = sha256()
    size = 0

    try:
        while True:
            chunk = file.file.read(
                1024 * 1024
            )

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
        client = StorageNodeClient(
            node.url
        )

        try:
            buffered.seek(0)

            result = client.put_object(
                object_id,
                buffered,
            )

            if (
                result.get("object_id")
                != str(object_id)
                or result.get("size")
                != expected_size
                or result.get("checksum")
                != expected_checksum
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


def _get_active_replica_nodes(
    db: Session,
    object_id: UUID,
):
    """Return ACTIVE replica records whose storage nodes are also ACTIVE."""

    replica_repository = ObjectReplicaRepository(db)
    node_repository = StorageNodeRepository(db)

    candidates = []

    for replica in replica_repository.list_active_for_object(
        object_id
    ):
        try:
            node = node_repository.get_by_id(
                replica.node_id
            )

        except StorageNodeNotFoundError:
            continue

        if node.status != "ACTIVE":
            continue

        candidates.append(
            (replica, node)
        )

    return candidates


def _select_read_replica(
    db: Session,
    object_id: UUID,
    expected_size: int,
) -> StorageNodeClient:
    """Select the first readable ACTIVE replica using a lightweight HEAD check."""

    candidates = _get_active_replica_nodes(
        db,
        object_id,
    )

    if not candidates:
        raise HTTPException(
            status_code=503,
            detail="No active replica is available for this object",
        )

    for _replica, node in candidates:
        client = StorageNodeClient(
            node.url
        )

        try:
            response = client.head_object(
                object_id
            )

            content_length = response.headers.get(
                "content-length"
            )

            response.close()

            if (
                content_length is not None
                and int(content_length) != expected_size
            ):
                raise StorageNodeError(
                    "Storage node returned an unexpected object size"
                )

            return client

        except (
            StorageNodeError,
            ValueError,
        ):
            client.close()

    raise HTTPException(
        status_code=503,
        detail="No active replica is available for this object",
    )


def _prepare_read_stream(
    node_url: str,
    object_id: UUID,
    expected_size: int,
):
    """Open a replica stream and read its first chunk before sending HTTP headers.

    The coordinator must establish that at least one replica can actually serve
    the object before returning a StreamingResponse. Otherwise an exception
    raised later by the streaming generator occurs after Starlette has already
    committed the HTTP status line, producing a RuntimeError instead of a clean
    503.
    """

    stack = ExitStack()
    client = StorageNodeClient(
        node_url
    )

    stack.callback(
        client.close
    )

    try:
        response = stack.enter_context(
            client.stream_object(
                object_id
            )
        )

        content_length = response.headers.get(
            "content-length"
        )

        if (
            content_length is not None
            and int(content_length) != expected_size
        ):
            raise StorageNodeError(
                "Storage node returned an unexpected object size"
            )

        iterator = response.iter_bytes(
            1024 * 1024
        )

        first_chunk = next(
            iterator,
            None,
        )

        if (
            expected_size > 0
            and first_chunk is None
        ):
            raise StorageNodeError(
                "Storage node returned an empty object for a non-empty object"
            )

        return (
            stack,
            iterator,
            first_chunk,
        )

    except (
        StorageNodeError,
        ValueError,
        StopIteration,
    ):
        stack.close()
        raise


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
        object_repository.get_by_key(
            object_key
        )

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
        buffered, size, checksum = _buffer_upload(
            file
        )

    finally:
        file.file.close()

    try:
        try:
            metadata = object_repository.create(
                object_id=object_id,
                object_key=object_key,
                size=size,
                checksum=checksum,
            )

        except IntegrityError as exc:
            db.rollback()

            raise HTTPException(
                status_code=409,
                detail="Object key already exists",
            ) from exc

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

    return ObjectMetadataResponse.model_validate(
        metadata
    )


@app.get(
    "/objects/{object_key:path}"
)
def get_object(
    object_key: str,
    db: Session = Depends(get_db),
) -> StreamingResponse:
    object_repository = ObjectRepository(db)

    try:
        metadata = object_repository.get_by_key(
            object_key
        )

    except ObjectNotFoundError as exc:
        raise HTTPException(
            status_code=404,
            detail="Object not found",
        ) from exc

    candidates = _get_active_replica_nodes(
        db,
        metadata.object_id,
    )

    if not candidates:
        raise HTTPException(
            status_code=503,
            detail="No active replica is available for this object",
        )

    selected_stream = None

    for _replica, node in candidates:
        try:
            selected_stream = _prepare_read_stream(
                node.url,
                metadata.object_id,
                metadata.size,
            )
            break

        except (
            StorageNodeError,
            ValueError,
            StopIteration,
        ):
            continue

    if selected_stream is None:
        raise HTTPException(
            status_code=503,
            detail="No active replica is available for this object",
        )

    stack, iterator, first_chunk = selected_stream

    def body() -> Iterator[bytes]:
        try:
            if first_chunk is not None:
                yield first_chunk

            yield from iterator

        finally:
            stack.close()

    return StreamingResponse(
        body(),
        media_type="application/octet-stream",
        headers={
            "Content-Length": str(
                metadata.size
            ),
            "ETag": f'"{metadata.checksum}"',
        },
    )


@app.head(
    "/objects/{object_key:path}"
)
def head_object(
    object_key: str,
    db: Session = Depends(get_db),
) -> Response:
    object_repository = ObjectRepository(db)

    try:
        metadata = object_repository.get_by_key(
            object_key
        )

    except ObjectNotFoundError as exc:
        raise HTTPException(
            status_code=404,
            detail="Object not found",
        ) from exc

    client = _select_read_replica(
        db,
        metadata.object_id,
        metadata.size,
    )

    client.close()

    return Response(
        status_code=200,
        headers={
            "Content-Length": str(
                metadata.size
            ),
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
    replica_repository = ObjectReplicaRepository(db)

    try:
        metadata = object_repository.get_by_key(
            object_key
        )

    except ObjectNotFoundError as exc:
        raise HTTPException(
            status_code=404,
            detail="Object not found",
        ) from exc

    replica_records = replica_repository.list_for_object(
        metadata.object_id
    )

    node_repository = StorageNodeRepository(db)

    failures = 0

    for replica in replica_records:
        try:
            node = node_repository.get_by_id(
                replica.node_id
            )

        except StorageNodeNotFoundError:
            failures += 1
            continue

        if node.status != "ACTIVE":
            failures += 1
            continue

        client = StorageNodeClient(
            node.url
        )

        try:
            client.delete_object(
                metadata.object_id
            )

            replica_repository.delete(
                replica.replica_id
            )

        except StorageNodeError as exc:
            if exc.status_code == 404:
                replica_repository.delete(
                    replica.replica_id
                )
            else:
                failures += 1

        finally:
            client.close()

    if failures:
        raise HTTPException(
            status_code=502,
            detail=(
                "Object deletion incomplete: "
                f"{failures} replica(s) could not be deleted"
            ),
        )

    object_repository.delete(
        metadata.object_id
    )


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
            ObjectMetadataResponse.model_validate(
                obj
            )
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
            StorageNodeResponse.model_validate(
                node
            )
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
            ObjectReplicaResponse.model_validate(
                replica
            )
            for replica in repository.list_for_object(
                object_id
            )
        ]
    )


@app.get(
    "/internal/under-replicated",
    response_model=UnderReplicatedObjectListResponse,
)
def list_under_replicated_objects() -> UnderReplicatedObjectListResponse:
    """Return objects whose effective healthy replica count is below the target."""

    service = ReplicaRepairService(
        SessionLocal,
        replication_factor=settings.replication_factor,
    )

    return UnderReplicatedObjectListResponse(
        objects=[
            UnderReplicatedObjectResponse(
                object_id=item.object_id,
                object_key=item.object_key,
                replication_factor=item.replication_factor,
                healthy_replica_count=item.healthy_replica_count,
                missing_replicas=item.missing_replicas,
            )
            for item in service.find_under_replicated()
        ]
    )


@app.post(
    "/internal/repair",
    response_model=RepairResultListResponse,
)
def repair_under_replicated_objects() -> RepairResultListResponse:
    """Repair all currently under-replicated objects."""

    service = ReplicaRepairService(
        SessionLocal,
        replication_factor=settings.replication_factor,
    )

    return RepairResultListResponse(
        results=[
            RepairResultResponse(
                object_id=result.object_id,
                object_key=result.object_key,
                healthy_replica_count=result.healthy_replica_count,
                target_replica_count=result.target_replica_count,
                repaired_replica_count=result.repaired_replica_count,
                remaining_missing_replicas=result.remaining_missing_replicas,
            )
            for result in service.repair_all()
        ]
    )


@app.post(
    "/internal/reconcile",
    response_model=ReconciliationResultListResponse,
)
def reconcile_storage() -> ReconciliationResultListResponse:
    """Reconcile physical storage with current coordinator placement."""

    service = ReconciliationService(
        SessionLocal,
        replication_factor=settings.replication_factor,
    )

    results = service.reconcile_all()

    return ReconciliationResultListResponse(
        results=[
            ReconciliationResultResponse(
                object_id=result.object_id,
                object_key=result.object_key,
                desired_node_ids=list(result.desired_node_ids),
                healthy_node_ids=list(result.healthy_node_ids),
                repaired_node_ids=list(result.repaired_node_ids),
                removed_node_ids=list(result.removed_node_ids),
                stale_node_ids=list(result.stale_node_ids),
                missing_node_ids=list(result.missing_node_ids),
                checksum_mismatch_node_ids=list(
                    result.checksum_mismatch_node_ids
                ),
                orphan_node_ids=list(result.orphan_node_ids),
                unavailable_node_ids=list(result.unavailable_node_ids),
            )
            for result in results
        ],
        orphan_objects=[
            PhysicalOrphanResponse(
                object_id=orphan.object_id,
                node_id=orphan.node_id,
            )
            for orphan in service.find_orphan_objects()
        ],
    )


@app.post(
    "/internal/storage-nodes/health-check",
    response_model=StorageNodeListResponse,
)
def check_storage_nodes_health() -> StorageNodeListResponse:
    """Run an immediate health check for all registered storage nodes."""

    checker = StorageNodeHealthChecker(
        SessionLocal,
        timeout_seconds=settings.health_check_timeout_seconds,
    )

    checker.check_all_nodes()

    with SessionLocal() as db:
        repository = StorageNodeRepository(db)

        return StorageNodeListResponse(
            nodes=[
                StorageNodeResponse.model_validate(
                    node
                )
                for node in repository.list_all()
            ]
        )