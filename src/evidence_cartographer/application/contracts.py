from pydantic import BaseModel, ConfigDict, Field

from evidence_cartographer.domain.enums import ContractOutcome, SourceName


class ApplicationModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ContractManifest(ApplicationModel):
    source: SourceName
    version: str
    formats: tuple[str, ...]
    outcomes: tuple[ContractOutcome, ...]


class ValidationMessage(ApplicationModel):
    rule_id: str
    message: str
    field: str | None = None


class ContractResult(ApplicationModel):
    outcome: ContractOutcome
    messages: tuple[ValidationMessage, ...] = ()


class GoldEligibilitySignals(ApplicationModel):
    rights_are_permissive: bool | None
    has_usable_image: bool
    metadata_quality_score: float = Field(ge=0.0, le=1.0)
