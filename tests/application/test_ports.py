from collections.abc import Iterable
from inspect import Parameter, signature
from typing import get_type_hints

from evidence_cartographer.application.errors import EvidenceCartographerError
from evidence_cartographer.application.ports import (
    DataQualityAssessor,
    RawRecord,
    ResolutionWriter,
    SilverWriter,
    SourceAdapter,
)
from evidence_cartographer.application.resolution import (
    ResolutionCandidate,
    ResolutionReview,
)
from evidence_cartographer.application.retry import RetryDecider, RetryDecision
from evidence_cartographer.domain.enums import IngestionMode, SourceName
from evidence_cartographer.domain.models import (
    DataQualityResult,
    Object,
    SCD2Period,
    SourceRecord,
)


def test_source_adapter_requires_a_caller_supplied_retry_decider() -> None:
    annotations = get_type_hints(SourceAdapter.acquire)
    retry_parameter = signature(SourceAdapter.acquire).parameters["retry_decider"]

    assert annotations["mode"] is IngestionMode
    assert annotations["retry_decider"] is RetryDecider
    assert retry_parameter.default is Parameter.empty


def test_source_adapter_receives_the_exact_retry_decider() -> None:
    class NeverRetry:
        def decide(
            self,
            failure: EvidenceCartographerError,
            attempt: int,
        ) -> RetryDecision:
            return RetryDecision.STOP

    class RecordingSourceAdapter:
        source = SourceName.MET
        received_decider: RetryDecider | None = None

        def acquire(
            self,
            mode: IngestionMode,
            retry_decider: RetryDecider,
        ) -> Iterable[RawRecord]:
            self.received_decider = retry_decider
            return ()

    adapter: SourceAdapter = RecordingSourceAdapter()
    retry_decider: RetryDecider = NeverRetry()
    tuple(adapter.acquire(IngestionMode.FULL_SNAPSHOT, retry_decider))

    assert adapter.received_decider is retry_decider


def test_silver_writer_exposes_append_only_canonical_version_boundary() -> None:
    assert hasattr(SilverWriter, "append_version")
    annotations = get_type_hints(SilverWriter.append_version)
    assert annotations["object_"] is Object
    assert annotations["period"] is SCD2Period
    assert annotations["quality"] is DataQualityResult


def test_quality_assessor_returns_typed_quality_evidence() -> None:
    assert hasattr(DataQualityAssessor, "assess")
    annotations = get_type_hints(DataQualityAssessor.assess)
    assert annotations["object_"] is Object
    assert annotations["provenance"] is SourceRecord
    assert annotations["return"] is DataQualityResult


def test_resolution_writer_records_candidates_and_review_decisions() -> None:
    assert hasattr(ResolutionWriter, "append_candidate")
    assert hasattr(ResolutionWriter, "append_review")
    candidate_annotations = get_type_hints(ResolutionWriter.append_candidate)
    review_annotations = get_type_hints(ResolutionWriter.append_review)
    assert candidate_annotations["candidate"] is ResolutionCandidate
    assert review_annotations["review"] is ResolutionReview
    assert "decision" not in ResolutionCandidate.model_fields
    assert "reviewer_note" not in ResolutionCandidate.model_fields
    assert "decision" in ResolutionReview.model_fields
    assert "reviewer_id" in ResolutionReview.model_fields
