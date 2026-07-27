import re
from collections.abc import Iterable
from pathlib import Path
from typing import Protocol
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
)

from evidence_cartographer.application.contracts import ContractResult
from evidence_cartographer.domain.enums import ContractOutcome, SourceName
from evidence_cartographer.domain.models import SourceRecord

SAFE_EXTENSION = re.compile(r"^[a-z0-9]+(?:[._-][a-z0-9]+)*$")


class BronzeModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class BronzeArtifact(BronzeModel):
    source: SourceName
    ingestion_run_id: UUID
    retrieved_at: AwareDatetime
    source_url: str
    media_type: str = Field(min_length=1)
    extension: str
    contract_version: str = Field(min_length=1)
    local_path: Path
    expected_sha256: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )

    @field_validator("extension", mode="before")
    @classmethod
    def normalize_extension(cls, value: object) -> object:
        if not isinstance(value, str):
            return value
        normalized = value.removeprefix(".").lower()
        if len(normalized) > 32 or SAFE_EXTENSION.fullmatch(normalized) is None:
            raise ValueError("extension must be a safe file suffix")
        return normalized


class BronzeRecordEvidence(BronzeModel):
    provenance: SourceRecord
    result: ContractResult


class StoredObject(BronzeModel):
    uri: str
    size_bytes: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class BronzeCompletionManifest(BronzeModel):
    source: SourceName
    ingestion_run_id: UUID
    retrieved_at: AwareDatetime
    source_url: str
    contract_version: str
    artifact: StoredObject
    evidence_manifest: StoredObject
    total_records: int = Field(ge=0)
    outcome_counts: dict[ContractOutcome, int]


class BronzeBundleReceipt(BronzeModel):
    source: SourceName
    ingestion_run_id: UUID
    artifact: StoredObject
    evidence_manifest: StoredObject
    completion_manifest_uri: str


class BronzeArtifactStore(Protocol):
    def store_bundle(
        self,
        artifact: BronzeArtifact,
        evidence: Iterable[BronzeRecordEvidence],
    ) -> BronzeBundleReceipt: ...
