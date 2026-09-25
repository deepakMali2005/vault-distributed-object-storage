"""Configuration for the VAULT coordinator."""

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class CoordinatorSettings(BaseSettings):
    """Environment-backed configuration for the coordinator."""

    host: str = Field(
        default="0.0.0.0",
        validation_alias=AliasChoices("COORDINATOR_HOST", "HOST"),
    )
    port: int = Field(
        default=8000,
        validation_alias=AliasChoices("COORDINATOR_PORT", "PORT"),
    )
    database_url: str = Field(
        default="postgresql+psycopg://vault:vault@localhost:5433/vault",
        validation_alias=AliasChoices(
            "DATABASE_URL",
            "COORDINATOR_DATABASE_URL",
        ),
    )
    storage_node_url: str = Field(
        default="http://localhost:8100",
        validation_alias=AliasChoices(
            "STORAGE_NODE_URL",
            "COORDINATOR_STORAGE_NODE_URL",
        ),
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
    )


settings = CoordinatorSettings()