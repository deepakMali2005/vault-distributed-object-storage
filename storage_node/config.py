"""Configuration for the VAULT storage node."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class StorageNodeSettings(BaseSettings):
    """Environment-backed configuration for a storage node."""

    host: str = "0.0.0.0"
    port: int = 8100
    data_dir: str = "./data/storage-node"

    model_config = SettingsConfigDict(
        env_prefix="STORAGE_NODE_",
        env_file=".env",
        extra="ignore",
    )


settings = StorageNodeSettings()