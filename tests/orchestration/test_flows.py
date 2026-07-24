import importlib
import sys
from inspect import Parameter, signature
from pathlib import Path

from pytest import MonkeyPatch

from evidence_cartographer.application.errors import EvidenceCartographerError
from evidence_cartographer.application.retry import RetryDecider, RetryDecision
from evidence_cartographer.domain.enums import IngestionMode, SourceName


def test_ingestion_flow_delegates_typed_inputs_unchanged(
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("PREFECT_HOME", str(tmp_path / "prefect"))
    module_name = "evidence_cartographer.orchestration.flows"
    sys.modules.pop(module_name, None)
    flows = importlib.import_module(module_name)
    retry_parameter = signature(flows.build_ingestion_flow).parameters["retry_decider"]
    assert retry_parameter.default is Parameter.empty

    class NeverRetry:
        def decide(
            self,
            failure: EvidenceCartographerError,
            attempt: int,
        ) -> RetryDecision:
            return RetryDecision.STOP

    retry_decider: RetryDecider = NeverRetry()
    received: list[tuple[SourceName, IngestionMode, RetryDecider]] = []

    ingestion_flow = flows.build_ingestion_flow(
        lambda source, mode, decider: received.append((source, mode, decider)),
        retry_decider,
    )
    ingestion_flow.fn(SourceName.MET, IngestionMode.FULL_SNAPSHOT)

    assert received == [(SourceName.MET, IngestionMode.FULL_SNAPSHOT, retry_decider)]
    assert received[0][2] is retry_decider
