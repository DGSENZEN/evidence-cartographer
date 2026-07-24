from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    computed_field,
    field_validator,
)

from evidence_cartographer.domain.enums import ResolutionDecision


class MatchEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    rule_id: str
    field: str
    left_value: str
    right_value: str
    is_strong_identifier: bool


class ResolutionCandidate(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    left_person_id: UUID
    right_person_id: UUID
    evidence: tuple[MatchEvidence, ...]
    confidence: float = Field(ge=0.0, le=1.0)
    decision: ResolutionDecision = ResolutionDecision.UNREVIEWED
    reviewer_note: str | None = None

    @computed_field  # type: ignore[prop-decorator]
    @property
    def can_auto_link(self) -> bool:
        return bool(self.evidence) and all(
            item.is_strong_identifier for item in self.evidence
        )


class ResolutionReview(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    left_person_id: UUID
    right_person_id: UUID
    decision: ResolutionDecision
    reviewed_at: AwareDatetime
    reviewer_id: str
    note: str | None = None

    @field_validator("decision")
    @classmethod
    def require_review_decision(
        cls,
        decision: ResolutionDecision,
    ) -> ResolutionDecision:
        if decision is ResolutionDecision.UNREVIEWED:
            raise ValueError("a completed review requires a decision")
        return decision
