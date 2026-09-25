"""Metadata repository for the VAULT coordinator."""

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from coordinator.models import ObjectMetadata


class ObjectNotFoundError(Exception):
    """Raised when object metadata does not exist."""


class ObjectRepository:
    """Provides persistence operations for object metadata."""

    def __init__(self, db: Session) -> None:
        self.db = db

    def create(
        self,
        *,
        object_id: UUID,
        object_key: str,
        size: int,
        checksum: str,
    ) -> ObjectMetadata:
        metadata = ObjectMetadata(
            object_id=object_id,
            object_key=object_key,
            size=size,
            checksum=checksum,
        )

        self.db.add(metadata)
        self.db.commit()
        self.db.refresh(metadata)

        return metadata

    def get_by_key(self, object_key: str) -> ObjectMetadata:
        statement = select(ObjectMetadata).where(
            ObjectMetadata.object_key == object_key
        )

        metadata = self.db.scalar(statement)

        if metadata is None:
            raise ObjectNotFoundError(object_key)

        return metadata

    def get_by_id(self, object_id: UUID) -> ObjectMetadata:
        metadata = self.db.get(ObjectMetadata, object_id)

        if metadata is None:
            raise ObjectNotFoundError(object_id)

        return metadata

    def list_all(self) -> list[ObjectMetadata]:
        statement = select(ObjectMetadata).order_by(
            ObjectMetadata.created_at,
            ObjectMetadata.object_id,
        )

        return list(self.db.scalars(statement).all())

    def delete(self, object_id: UUID) -> None:
        metadata = self.get_by_id(object_id)

        self.db.delete(metadata)
        self.db.commit()