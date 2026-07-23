from datetime import date
from uuid import UUID, uuid4

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from .enums import (
    ContractOutcome,
    IngestionMode,
    RetrievalStatus,
    RunStatus,
    SourceName,
)


class DomainModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


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
    raw_uri: str
    raw_checksum: str | None = None
    attribution_text: str | None = None
    outcome: ContractOutcome


class IngestionRun(Entity):
    source: SourceName
    mode: IngestionMode
    status: RunStatus
    started_at: AwareDatetime
    ended_at: AwareDatetime | None = None


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
