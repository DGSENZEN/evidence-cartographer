from collections.abc import Iterable, Mapping
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict

from evidence_cartographer.application.contracts import (
    ContractResult,
    GoldEligibilitySignals,
)
from evidence_cartographer.application.resolution import (
    ResolutionCandidate,
    ResolutionReview,
)
from evidence_cartographer.application.retry import RetryDecider
from evidence_cartographer.domain.enums import IngestionMode, SourceName
from evidence_cartographer.domain.models import (
    DataQualityResult,
    Object,
    SCD2Period,
    SourceRecord,
)


class RawRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    source_record_id: str
    payload: Mapping[str, Any]


class SourceAdapter(Protocol):
    source: SourceName

    def acquire(
        self,
        mode: IngestionMode,
        retry_decider: RetryDecider,
    ) -> Iterable[RawRecord]: ...


class ContractValidator(Protocol):
    def validate(self, record: RawRecord) -> ContractResult: ...


class BronzeWriter(Protocol):
    def append(
        self,
        record: RawRecord,
        provenance: SourceRecord,
        result: ContractResult,
    ) -> None: ...


class CanonicalMapper(Protocol):
    def map_object(self, record: RawRecord, provenance: SourceRecord) -> Object: ...


class SilverWriter(Protocol):
    def append_version(
        self,
        object_: Object,
        period: SCD2Period,
        quality: DataQualityResult,
    ) -> None: ...


class DataQualityAssessor(Protocol):
    def assess(
        self,
        object_: Object,
        provenance: SourceRecord,
    ) -> DataQualityResult: ...


class ResolutionWriter(Protocol):
    def append_candidate(self, candidate: ResolutionCandidate) -> None: ...

    def append_review(self, review: ResolutionReview) -> None: ...


class GoldPublisher(Protocol):
    def publish(self, object_: Object, signals: GoldEligibilitySignals) -> None: ...
