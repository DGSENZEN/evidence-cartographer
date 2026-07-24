from enum import StrEnum
from typing import Protocol

from evidence_cartographer.application.errors import EvidenceCartographerError


class RetryDecision(StrEnum):
    RETRY = "retry"
    STOP = "stop"


class RetryDecider(Protocol):
    def decide(
        self,
        failure: EvidenceCartographerError,
        attempt: int,
    ) -> RetryDecision: ...
