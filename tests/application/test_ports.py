from typing import get_type_hints

from evidence_cartographer.application.ports import (
    DataQualityAssessor,
    ResolutionWriter,
    SilverWriter,
)
from evidence_cartographer.application.resolution import (
    ResolutionCandidate,
    ResolutionReview,
)
from evidence_cartographer.domain.models import (
    DataQualityResult,
    Object,
    SCD2Period,
    SourceRecord,
)


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
