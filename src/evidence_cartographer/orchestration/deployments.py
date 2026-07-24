from pydantic import BaseModel, ConfigDict, Field

from evidence_cartographer.domain.enums import IngestionMode, SourceName


class PrefectDeploymentConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    source: SourceName
    mode: IngestionMode
    cron: str = Field(min_length=1)
    timezone: str = Field(min_length=1)
    work_pool_name: str | None = None
