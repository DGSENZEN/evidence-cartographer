from collections.abc import Iterable, Mapping
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict

from evidence_cartographer.application.contracts import (
    ContractResult,
    GoldEligibilitySignals,
)
from evidence_cartographer.domain.enums import IngestionMode, SourceName
from evidence_cartographer.domain.models import Object, SourceRecord


class RawRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    source_record_id: str
    payload: Mapping[str, Any]


class SourceAdapter(Protocol):
    source: SourceName

    def acquire(self, mode: IngestionMode) -> Iterable[RawRecord]: ...


class ContractValidator(Protocol):
    def validate(self, record: RawRecord) -> ContractResult: ...


class BronzeWriter(Protocol):
    def append(self, record: RawRecord, provenance: SourceRecord) -> None: ...


class CanonicalMapper(Protocol):
    def map_object(self, record: RawRecord, provenance: SourceRecord) -> Object: ...


class GoldPublisher(Protocol):
    def publish(self, object_: Object, signals: GoldEligibilitySignals) -> None: ...
