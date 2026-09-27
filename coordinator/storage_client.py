"""HTTP client for communicating with a VAULT storage node."""

from contextlib import contextmanager
from typing import Iterator
from uuid import UUID

import httpx


class StorageNodeError(Exception):
    """Raised when a storage-node operation fails."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
    ) -> None:
        """Store the failure message and optional HTTP status code."""

        super().__init__(message)
        self.status_code = status_code


class StorageNodeClient:
    """Client used by the coordinator to communicate with a storage node."""

    def __init__(self, base_url: str) -> None:
        """Create an HTTP client for one storage node."""

        self.base_url = base_url.rstrip("/")
        self.client = httpx.Client(timeout=60.0)

    def close(self) -> None:
        """Close the underlying HTTP connection pool."""

        self.client.close()

    def put_object(
        self,
        object_id: UUID,
        file,
        filename: str | None = None,
        content_type: str | None = None,
    ) -> dict:
        """Upload an object to the storage node."""

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
                    f"{response.status_code}",
                    status_code=response.status_code,
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
        """Stream an object's bytes from the storage node."""

        try:
            with self.client.stream(
                "GET",
                f"{self.base_url}/objects/{object_id}",
            ) as response:
                if response.status_code == 404:
                    raise StorageNodeError(
                        "Object not found on storage node",
                        status_code=404,
                    )

                if response.is_error:
                    raise StorageNodeError(
                        "Storage node GET failed with status "
                        f"{response.status_code}",
                        status_code=response.status_code,
                    )

                yield response

        except httpx.HTTPError as exc:
            raise StorageNodeError(
                f"Storage node GET failed: {exc}"
            ) from exc

    def head_object(
        self,
        object_id: UUID,
    ) -> httpx.Response:
        """Request object metadata without downloading the object."""

        try:
            response = self.client.head(
                f"{self.base_url}/objects/{object_id}",
            )

        except httpx.HTTPError as exc:
            raise StorageNodeError(
                f"Storage node HEAD failed: {exc}"
            ) from exc

        if response.status_code == 404:
            response.close()

            raise StorageNodeError(
                "Object not found on storage node",
                status_code=404,
            )

        if response.is_error:
            status_code = response.status_code
            response.close()

            raise StorageNodeError(
                "Storage node HEAD failed with status "
                f"{status_code}",
                status_code=status_code,
            )

        return response

    def list_objects(self) -> list[dict]:
        """Return the physical object inventory reported by a storage node."""

        try:
            response = self.client.get(
                f"{self.base_url}/objects",
            )

            if response.is_error:
                raise StorageNodeError(
                    "Storage node inventory failed with status "
                    f"{response.status_code}",
                    status_code=response.status_code,
                )

            payload = response.json()

        except httpx.HTTPError as exc:
            raise StorageNodeError(
                f"Storage node inventory failed: {exc}"
            ) from exc

        finally:
            if "response" in locals():
                response.close()

        objects = payload.get("objects")

        if not isinstance(objects, list):
            raise StorageNodeError(
                "Storage node inventory response is invalid"
            )

        return objects

    def delete_object(
        self,
        object_id: UUID,
    ) -> None:
        """Delete an object from the storage node."""

        try:
            response = self.client.delete(
                f"{self.base_url}/objects/{object_id}",
            )

        except httpx.HTTPError as exc:
            raise StorageNodeError(
                f"Storage node DELETE failed: {exc}"
            ) from exc

        if response.status_code == 404:
            response.close()

            raise StorageNodeError(
                "Object not found on storage node",
                status_code=404,
            )

        if response.is_error:
            status_code = response.status_code
            response.close()

            raise StorageNodeError(
                "Storage node DELETE failed with status "
                f"{status_code}",
                status_code=status_code,
            )

        response.close()