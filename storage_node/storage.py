"""Persistent local storage for VAULT objects."""

from __future__ import annotations

import hashlib
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID


class ObjectNotFoundError(Exception):
    """Raised when an object does not exist on the storage node."""


@dataclass(frozen=True)
class StoredObjectInfo:
    """Physical metadata for an object stored on this node."""

    object_id: UUID
    size: int
    checksum: str


class ObjectStorage:
    """Stores objects on the node's local persistent filesystem."""

    def __init__(self, data_dir: str | Path) -> None:
        """Create the storage directory if it does not already exist."""

        self.root = Path(data_dir).resolve()
        self.root.mkdir(
            parents=True,
            exist_ok=True,
        )

    def path_for(
        self,
        object_id: UUID,
    ) -> Path:
        """Return the filesystem path for an internal object ID."""

        return self.root / f"{object_id}.bin"

    def exists(
        self,
        object_id: UUID,
    ) -> bool:
        """Return whether an object exists."""

        return self.path_for(object_id).is_file()

    def store(
        self,
        object_id: UUID,
        source,
    ) -> tuple[int, str]:
        """Persist an object atomically while calculating its SHA-256 checksum."""

        destination = self.path_for(object_id)

        fd, temporary_name = tempfile.mkstemp(
            prefix=f".{object_id}.",
            suffix=".tmp",
            dir=self.root,
        )

        size = 0
        digest = hashlib.sha256()

        try:
            with os.fdopen(
                fd,
                "wb",
            ) as temporary_file:
                while True:
                    chunk = source.read(
                        1024 * 1024
                    )

                    if not chunk:
                        break

                    temporary_file.write(chunk)
                    digest.update(chunk)
                    size += len(chunk)

                temporary_file.flush()
                os.fsync(
                    temporary_file.fileno()
                )

            os.replace(
                temporary_name,
                destination,
            )

        except Exception:
            try:
                os.unlink(
                    temporary_name
                )
            except FileNotFoundError:
                pass

            raise

        return size, digest.hexdigest()

    def open(
        self,
        object_id: UUID,
    ):
        """Open an object for reading."""

        path = self.path_for(object_id)

        if not path.is_file():
            raise ObjectNotFoundError(object_id)

        return path.open("rb")

    def size(
        self,
        object_id: UUID,
    ) -> int:
        """Return the physical size of an object."""

        path = self.path_for(object_id)

        if not path.is_file():
            raise ObjectNotFoundError(object_id)

        return path.stat().st_size

    def checksum(
        self,
        object_id: UUID,
    ) -> str:
        """Calculate the SHA-256 checksum of a stored object."""

        with self.open(object_id) as object_file:
            digest = hashlib.sha256()

            while True:
                chunk = object_file.read(
                    1024 * 1024
                )

                if not chunk:
                    break

                digest.update(chunk)

        return digest.hexdigest()

    def inspect(
        self,
        object_id: UUID,
    ) -> StoredObjectInfo:
        """Return verified physical metadata for one stored object."""

        return StoredObjectInfo(
            object_id=object_id,
            size=self.size(object_id),
            checksum=self.checksum(object_id),
        )

    def list_objects(
        self,
    ) -> list[StoredObjectInfo]:
        """Enumerate valid physical object files on this node."""

        objects: list[StoredObjectInfo] = []

        for path in sorted(
            self.root.glob("*.bin")
        ):
            try:
                object_id = UUID(
                    path.stem
                )
            except ValueError:
                continue

            objects.append(
                self.inspect(
                    object_id
                )
            )

        return objects

    def delete(
        self,
        object_id: UUID,
    ) -> None:
        """Delete an object from the node."""

        path = self.path_for(object_id)

        try:
            path.unlink()
        except FileNotFoundError as exc:
            raise ObjectNotFoundError(
                object_id
            ) from exc