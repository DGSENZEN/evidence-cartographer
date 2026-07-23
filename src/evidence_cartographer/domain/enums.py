from enum import StrEnum


class SourceName(StrEnum):
    MET = "met"
    AIC = "aic"


class ContractOutcome(StrEnum):
    ACCEPTED = "accepted"
    ACCEPTED_WITH_WARNINGS = "accepted_with_warnings"
    QUARANTINED = "quarantined"
    REJECTED = "rejected"


class IngestionMode(StrEnum):
    FULL_SNAPSHOT = "full_snapshot"
    INCREMENTAL = "incremental"


class RunStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class RetrievalStatus(StrEnum):
    NOT_REQUESTED = "not_requested"
    AVAILABLE = "available"
    CACHED = "cached"
    UNAVAILABLE = "unavailable"
    FAILED = "failed"


class ResolutionDecision(StrEnum):
    UNREVIEWED = "unreviewed"
    LINK = "link"
    REJECT = "reject"
