"""HTTP client for communicating with a VAULT storage node."""

from contextlib import contextmanager
from typing import Iterator
from uuid import UUID

import httpx


class StorageNodeError(Exception):
    """Raised when a storage-node operation fails."""


class StorageNodeClient:
    """Client used by the coordinator to communicate with a storage node."""

    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.client = httpx.Client(timeout=60.0)

    def close(self) -> None:
        self.client.close()

    def put_object(
        self,
        object_id: UUID,
        file,
        filename: str | None = None,
        content_type: str | None = None,
    ) -> dict:
        files = {
            "file": (
                filename or str(object_id),
                file,
                content_type or "application/octet-stream",
            )
        }

        try:
            response = self.client.put(
                f"{self.base_url}/objects/{object_id}",
                files=files,
            )

            if response.is_error:
                raise StorageNodeError(
                    "Storage node PUT failed with status "
                    f"{response.status_code}"
                )

            return response.json()

        except httpx.HTTPError as exc:
            raise StorageNodeError(
                f"Storage node PUT failed: {exc}"
            ) from exc

    @contextmanager
    def stream_object(
        self,
        object_id: UUID,
    ) -> Iterator[httpx.Response]:
        try:
            with self.client.stream(
                "GET",
                f"{self.base_url}/objects/{object_id}",
            ) as response:
                if response.status_code == 404:
                    raise StorageNodeError(
                        "Object not found on storage node"
                    )

                if response.is_error:
                    raise StorageNodeError(
                        "Storage node GET failed with status "
                        f"{response.status_code}"
                    )

                yield response

        except httpx.HTTPError as exc:
            raise StorageNodeError(
                f"Storage node GET failed: {exc}"
            ) from exc

    def head_object(self, object_id: UUID) -> httpx.Response:
        try:
            response = self.client.head(
                f"{self.base_url}/objects/{object_id}",
            )

        except httpx.HTTPError as exc:
            raise StorageNodeError(
                f"Storage node HEAD failed: {exc}"
            ) from exc

        if response.status_code == 404:
            raise StorageNodeError(
                "Object not found on storage node"
            )

        if response.is_error:
            raise StorageNodeError(
                "Storage node HEAD failed with status "
                f"{response.status_code}"
            )

        return response

    def delete_object(self, object_id: UUID) -> None:
        try:
            response = self.client.delete(
                f"{self.base_url}/objects/{object_id}",
            )

        except httpx.HTTPError as exc:
            raise StorageNodeError(
                f"Storage node DELETE failed: {exc}"
            ) from exc

        if response.status_code == 404:
            raise StorageNodeError(
                "Object not found on storage node"
            )

        if response.is_error:
            raise StorageNodeError(
                "Storage node DELETE failed with status "
                f"{response.status_code}"
            )