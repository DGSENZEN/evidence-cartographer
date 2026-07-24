import importlib
import sys
from pathlib import Path

from pytest import MonkeyPatch

from evidence_cartographer.domain.enums import IngestionMode, SourceName


def test_ingestion_flow_delegates_typed_inputs_unchanged(
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("PREFECT_HOME", str(tmp_path / "prefect"))
    module_name = "evidence_cartographer.orchestration.flows"
    sys.modules.pop(module_name, None)
    flows = importlib.import_module(module_name)
    received: list[tuple[SourceName, IngestionMode]] = []

    ingestion_flow = flows.build_ingestion_flow(
        lambda source, mode: received.append((source, mode))
    )
    ingestion_flow.fn(SourceName.MET, IngestionMode.FULL_SNAPSHOT)

    assert received == [(SourceName.MET, IngestionMode.FULL_SNAPSHOT)]
