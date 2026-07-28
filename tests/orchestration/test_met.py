from pathlib import Path

from pytest import MonkeyPatch

from evidence_cartographer.application.retry import RetryDecision
from evidence_cartographer.infrastructure.settings import Settings
from evidence_cartographer.orchestration.met import (
    build_met_snapshot_flow,
    build_met_snapshot_service,
)


class NeverRetry:
    def decide(self, failure: Exception, attempt: int) -> RetryDecision:
        return RetryDecision.STOP


def test_flow_delegates_to_application_service(
    monkeypatch: MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("PREFECT_HOME", str(tmp_path / "prefect"))
    received: list[object] = []

    class StubService:
        def run(self, retry_decider: NeverRetry) -> object:
            received.append(retry_decider)
            return {"status": "synthetic"}

    retry = NeverRetry()
    flow = build_met_snapshot_flow(StubService(), retry)

    assert flow.fn() == {"status": "synthetic"}
    assert received == [retry]


def test_production_composition_uses_configured_real_boundaries() -> None:
    settings = Settings.model_validate(
        {
            "postgres": {"password": "test"},
            "object_store": {
                "endpoint": "localhost:9000",
                "access_key": "test",
                "secret_key": "test",
            },
            "sources": {
                "met_snapshot_url": "https://example.test/MetObjects.csv",
                "met_csv_batch_size": 123,
            },
        }
    )

    service = build_met_snapshot_service(settings)

    assert service.snapshot_url == "https://example.test/MetObjects.csv"
    assert service.batch_size == 123
