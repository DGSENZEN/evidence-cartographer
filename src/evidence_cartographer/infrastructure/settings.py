from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel, Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class PostgresSettings(BaseModel):
    host: str = "localhost"
    port: int = 5432
    database: str = "evidence_cartographer"
    user: str = "evidence_cartographer"
    password: SecretStr = Field(min_length=1)


class ObjectStoreSettings(BaseModel):
    endpoint: str = "localhost:9000"
    access_key: str = Field(min_length=1)
    secret_key: SecretStr = Field(min_length=1)
    secure: bool = False
    bronze_bucket: str = "bronze"
    silver_bucket: str = "silver"
    gold_bucket: str = "gold"
    quarantine_bucket: str = "quarantine"
    image_cache_bucket: str = "image-cache"


class DuckDBSettings(BaseModel):
    path: Path = Path("data/evidence_cartographer.duckdb")


class LakeSettings(BaseModel):
    bronze_prefix: str = "raw"
    manifest_prefix: str = "manifests"
    quarantine_prefix: str = "quarantine"
    silver_prefix: str = "normalized"
    gold_prefix: str = "gold"


class PrefectSettings(BaseModel):
    api_url: str | None = None
    work_pool_name: str | None = None

    @field_validator("api_url", "work_pool_name", mode="before")
    @classmethod
    def normalize_empty_value(cls, value: object) -> object:
        return None if value == "" else value


class ContractVersionSettings(BaseModel):
    met_version: str = "1.0.0"
    aic_version: str = "1.0.0"


class ImageCacheSelection(StrEnum):
    PRIMARY_ONLY = "primary_only"
    ALL = "all"


class ImageCacheSettings(BaseModel):
    enabled: bool = False
    prefix: str = "images"
    selection: ImageCacheSelection = ImageCacheSelection.PRIMARY_ONLY


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
    postgres: PostgresSettings
    object_store: ObjectStoreSettings
    duckdb: DuckDBSettings = DuckDBSettings()
    lake: LakeSettings = LakeSettings()
    prefect: PrefectSettings = PrefectSettings()
    contracts: ContractVersionSettings = ContractVersionSettings()
    image_cache: ImageCacheSettings = ImageCacheSettings()
    refresh: RefreshSettings = RefreshSettings()
    sources: SourceEndpoints = SourceEndpoints()
