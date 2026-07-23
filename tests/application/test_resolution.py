from uuid import uuid4

from evidence_cartographer.application.resolution import (
    MatchEvidence,
    ResolutionCandidate,
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
