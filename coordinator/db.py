"""Database configuration for the VAULT coordinator."""

from collections.abc import Generator

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from coordinator.config import settings


class Base(DeclarativeBase):
    """Base class for SQLAlchemy models."""


engine = create_engine(
    settings.database_url,
    pool_pre_ping=True,
)

SessionLocal = sessionmaker(
    bind=engine,
    class_=Session,
    expire_on_commit=False,
)


def get_db() -> Generator[Session, None, None]:
    """Provide a database session for a request."""

    db = SessionLocal()

    try:
        yield db
    finally:
        db.close()


def init_db() -> None:
    """Create database tables that do not already exist."""

    from coordinator import models  # noqa: F401

    Base.metadata.create_all(bind=engine)