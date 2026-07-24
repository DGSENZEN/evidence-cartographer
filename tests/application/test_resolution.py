from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from evidence_cartographer.application.resolution import (
    MatchEvidence,
    ResolutionCandidate,
    ResolutionReview,
)
from evidence_cartographer.domain.enums import ResolutionDecision


def test_weak_candidate_defaults_to_human_review() -> None:
    candidate = ResolutionCandidate(
        left_person_id=uuid4(),
        right_person_id=uuid4(),
        evidence=(
            MatchEvidence(
                rule_id="exact_name_and_dates",
                field="normalized_name",
                left_value="claude monet",
                right_value="claude monet",
                is_strong_identifier=False,
            ),
        ),
        confidence=0.85,
    )
    assert candidate.decision is ResolutionDecision.UNREVIEWED
    assert not candidate.can_auto_link


def test_resolution_review_captures_a_human_decision() -> None:
    left_person_id = uuid4()
    right_person_id = uuid4()
    review = ResolutionReview(
        left_person_id=left_person_id,
        right_person_id=right_person_id,
        decision=ResolutionDecision.LINK,
        reviewed_at=datetime.now(UTC),
        reviewer_id="curator@example.test",
        note="Authority identifiers match.",
    )

    assert review.left_person_id == left_person_id
    assert review.right_person_id == right_person_id
    assert review.decision is ResolutionDecision.LINK


def test_resolution_review_rejects_unreviewed_decision() -> None:
    with pytest.raises(ValidationError):
        ResolutionReview(
            left_person_id=uuid4(),
            right_person_id=uuid4(),
            decision=ResolutionDecision.UNREVIEWED,
            reviewed_at=datetime.now(UTC),
            reviewer_id="curator@example.test",
        )
