"""Persistent local storage for VAULT objects."""

from __future__ import annotations

import hashlib
import os
import tempfile
from pathlib import Path
from uuid import UUID


class ObjectNotFoundError(Exception):
    """Raised when an object does not exist on the storage node."""


class ObjectStorage:
    """Stores objects on the node's local persistent filesystem."""

    def __init__(self, data_dir: str | Path) -> None:
        self.root = Path(data_dir).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def path_for(self, object_id: UUID) -> Path:
        """Return the filesystem path for an internal object ID."""

        return self.root / f"{object_id}.bin"

    def exists(self, object_id: UUID) -> bool:
        return self.path_for(object_id).is_file()

    def store(self, object_id: UUID, source) -> tuple[int, str]:
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
            with os.fdopen(fd, "wb") as temporary_file:
                while True:
                    chunk = source.read(1024 * 1024)

                    if not chunk:
                        break

                    temporary_file.write(chunk)
                    digest.update(chunk)
                    size += len(chunk)

                temporary_file.flush()
                os.fsync(temporary_file.fileno())

            os.replace(temporary_name, destination)

        except Exception:
            try:
                os.unlink(temporary_name)
            except FileNotFoundError:
                pass

            raise

        return size, digest.hexdigest()

    def open(self, object_id: UUID):
        """Open an object for reading."""

        path = self.path_for(object_id)

        if not path.is_file():
            raise ObjectNotFoundError(object_id)

        return path.open("rb")

    def size(self, object_id: UUID) -> int:
        path = self.path_for(object_id)

        if not path.is_file():
            raise ObjectNotFoundError(object_id)

        return path.stat().st_size

    def delete(self, object_id: UUID) -> None:
        path = self.path_for(object_id)

        try:
            path.unlink()
        except FileNotFoundError as exc:
            raise ObjectNotFoundError(object_id) from exc