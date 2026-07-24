from pydantic import BaseModel, ConfigDict

from evidence_cartographer.domain.enums import SourceName


class SourceDescriptor(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    source: SourceName
    display_name: str
    bulk_format: str
    supports_incremental_api: bool
    contract_version: str
