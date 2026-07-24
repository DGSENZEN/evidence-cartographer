from datetime import date
from typing import Self
from uuid import UUID, uuid4

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from .enums import (
    ContractOutcome,
    IngestionMode,
    RetrievalStatus,
    RunStatus,
    SourceName,
)


class DomainModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class AcquisitionContext(DomainModel):
    mode: IngestionMode


class Entity(DomainModel):
    id: UUID = Field(default_factory=uuid4)
    created_at: AwareDatetime


class Museum(Entity):
    name: str
    source: SourceName


class Department(Entity):
    museum_id: UUID
    name: str


class Classification(Entity):
    museum_id: UUID
    name: str


class Culture(Entity):
    name: str


class Place(Entity):
    name: str
    latitude: float | None = None
    longitude: float | None = None


class Person(Entity):
    display_name: str
    normalized_name: str | None = None
    birth_date: date | None = None
    death_date: date | None = None
    authority_ids: dict[str, str] = Field(default_factory=dict)


class Object(Entity):
    museum_id: UUID
    source_record_id: UUID
    accession_number: str | None = None
    title: str | None = None
    department_id: UUID | None = None
    classification_id: UUID | None = None
    culture_id: UUID | None = None
    place_id: UUID | None = None


class Image(Entity):
    museum_id: UUID
    source_url: str
    retrieval_url: str | None = None
    iiif_id: str | None = None
    width: int | None = Field(default=None, gt=0)
    height: int | None = Field(default=None, gt=0)
    rights_text: str | None = None
    license_uri: str | None = None
    checksum: str | None = None
    retrieval_status: RetrievalStatus = RetrievalStatus.NOT_REQUESTED
    cached_uri: str | None = None

    @model_validator(mode="after")
    def validate_cache_state(self) -> Self:
        if self.retrieval_status is RetrievalStatus.CACHED and self.cached_uri is None:
            raise ValueError("cached images require cached_uri")
        if (
            self.cached_uri is not None
            and self.retrieval_status is not RetrievalStatus.CACHED
        ):
            raise ValueError("cached_uri requires a cached retrieval status")
        return self


class ObjectPersonRole(Entity):
    object_id: UUID
    person_id: UUID
    role: str
    attribution_text: str | None = None


class ObjectImage(Entity):
    object_id: UUID
    image_id: UUID
    is_primary: bool = False
    sequence: int | None = Field(default=None, ge=0)


class SourceRecord(Entity):
    museum_id: UUID
    source: SourceName
    source_record_id: str
    contract_version: str
    ingestion_run_id: UUID
    observed_at: AwareDatetime
    source_url: str
    retrieval_status: RetrievalStatus
    retrieved_at: AwareDatetime
    raw_uri: str
    acquisition_context: AcquisitionContext
    raw_checksum: str | None = None
    attribution_text: str | None = None
    outcome: ContractOutcome


class IngestionRun(Entity):
    source: SourceName
    mode: IngestionMode
    status: RunStatus
    started_at: AwareDatetime
    ended_at: AwareDatetime | None = None

    @model_validator(mode="after")
    def validate_lifecycle_state(self) -> Self:
        is_terminal = self.status in {RunStatus.SUCCEEDED, RunStatus.FAILED}
        if is_terminal and self.ended_at is None:
            raise ValueError("terminal ingestion runs require ended_at")
        if not is_terminal and self.ended_at is not None:
            raise ValueError("non-terminal ingestion runs cannot have ended_at")
        if self.ended_at is not None and self.ended_at < self.started_at:
            raise ValueError("ended_at cannot precede started_at")
        return self


class QualityCheck(DomainModel):
    rule_id: str
    passed: bool
    message: str | None = None


class DataQualityResult(Entity):
    source_record_id: UUID
    score: float = Field(ge=0.0, le=1.0)
    checks: tuple[QualityCheck, ...] = ()
    evaluated_at: AwareDatetime


class SCD2Period(DomainModel):
    valid_from: AwareDatetime
    valid_to: AwareDatetime | None = None
    is_current: bool

    @model_validator(mode="after")
    def validate_period(self) -> Self:
        if self.valid_to is not None and self.valid_to < self.valid_from:
            raise ValueError("valid_to cannot precede valid_from")
        if self.is_current and self.valid_to is not None:
            raise ValueError("current periods require valid_to=None")
        if not self.is_current and self.valid_to is None:
            raise ValueError("non-current periods require valid_to")
        return self
