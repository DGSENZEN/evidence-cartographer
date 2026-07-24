from typing import get_type_hints

from evidence_cartographer.application.errors import (
    AcquisitionError,
    EvidenceCartographerError,
)
from evidence_cartographer.application.retry import RetryDecider, RetryDecision


def test_retry_decider_is_injected_without_a_default_policy() -> None:
    assert hasattr(RetryDecider, "decide")
    annotations = get_type_hints(RetryDecider.decide)
    assert annotations["failure"] is EvidenceCartographerError
    assert annotations["attempt"] is int
    assert annotations["return"] is RetryDecision
    assert {decision.value for decision in RetryDecision} == {"retry", "stop"}


def _ask_for_retry(
    decider: RetryDecider,
    failure: EvidenceCartographerError,
    attempt: int,
) -> RetryDecision:
    return decider.decide(failure, attempt)


def test_retry_boundary_accepts_a_caller_supplied_decider() -> None:
    class StopImmediately:
        def decide(
            self,
            failure: EvidenceCartographerError,
            attempt: int,
        ) -> RetryDecision:
            return RetryDecision.STOP

    assert (
        _ask_for_retry(StopImmediately(), AcquisitionError("offline"), 1)
        is RetryDecision.STOP
    )
