from pathlib import Path

from pydantic import BaseModel, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class PostgresSettings(BaseModel):
    host: str = "localhost"
    port: int = 5432
    database: str = "evidence_cartographer"
    user: str = "evidence_cartographer"
    password: SecretStr = SecretStr("evidence_cartographer")


class ObjectStoreSettings(BaseModel):
    endpoint: str = "localhost:9000"
    access_key: str = "minioadmin"
    secret_key: SecretStr = SecretStr("minioadmin")
    secure: bool = False
    bronze_bucket: str = "bronze"
    silver_bucket: str = "silver"
    gold_bucket: str = "gold"
    quarantine_bucket: str = "quarantine"
    image_cache_bucket: str = "image-cache"


class DuckDBSettings(BaseModel):
    path: Path = Path("data/evidence_cartographer.duckdb")


class RefreshSettings(BaseModel):
    weekly_full_cron: str = "0 2 * * 0"
    daily_incremental_cron: str = "0 3 * * *"
    timezone: str = "UTC"


class SourceEndpoints(BaseModel):
    met_api_base_url: str = "https://collectionapi.metmuseum.org/public/collection/v1"
    aic_api_base_url: str = "https://api.artic.edu/api/v1"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="EC_",
        env_nested_delimiter="__",
        env_file=".env",
        extra="ignore",
    )

    environment: str = "development"
    log_level: str = "INFO"
    postgres: PostgresSettings = PostgresSettings()
    object_store: ObjectStoreSettings = ObjectStoreSettings()
    duckdb: DuckDBSettings = DuckDBSettings()
    refresh: RefreshSettings = RefreshSettings()
    sources: SourceEndpoints = SourceEndpoints()
