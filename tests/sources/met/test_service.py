import hashlib
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest

from evidence_cartographer.application.bronze import (
    BronzeArtifact,
    BronzeBundleReceipt,
    BronzeBundleTarget,
    BronzeRecordEvidence,
    StoredObject,
)
from evidence_cartographer.application.contracts import ValidationMessage
from evidence_cartographer.application.retry import RetryDecider, RetryDecision
from evidence_cartographer.domain.enums import ContractOutcome
from evidence_cartographer.sources.met.download import DownloadedMetSnapshot
from evidence_cartographer.sources.met.service import (
    MET_MUSEUM_ID,
    MetSnapshotIngestionService,
)

FIXTURE = Path("tests/sources/met/fixtures/met-small.csv")
RUN_ID = UUID("00000000-0000-0000-0000-000000000042")
NOW = datetime(2026, 7, 28, 12, 0, tzinfo=UTC)


class NeverRetry:
    def decide(self, failure: Exception, attempt: int) -> RetryDecision:
        return RetryDecision.STOP


class FixtureDownloader:
    def __init__(self) -> None:
        self.target: Path | None = None

    def download(
        self,
        destination_dir: Path,
        retry_decider: RetryDecider,
    ) -> DownloadedMetSnapshot:
        target = destination_dir / "MetObjects.csv"
        self.target = target
        payload = FIXTURE.read_bytes()
        target.write_bytes(payload)
        return DownloadedMetSnapshot(
            local_path=target,
            requested_url="https://github.example.test/MetObjects.csv",
            final_url="https://media.example.test/MetObjects.csv",
            media_type="text/csv",
            size_bytes=len(payload),
            sha256=hashlib.sha256(payload).hexdigest(),
            retrieved_at=NOW,
        )


class CountingStore:
    def __init__(self) -> None:
        self.messages: tuple[ValidationMessage, ...] = ()
        self.outcomes: dict[ContractOutcome, int] = dict.fromkeys(
            ContractOutcome,
            0,
        )

    def target_for(self, artifact: BronzeArtifact) -> BronzeBundleTarget:
        return BronzeBundleTarget(
            artifact_uri="s3://bronze/raw/met/source.csv",
            evidence_manifest_uri="s3://bronze/raw/met/records.manifest.jsonl",
            completion_manifest_uri="s3://bronze/raw/met/_SUCCESS.json",
        )

    def store_bundle(
        self,
        artifact: BronzeArtifact,
        evidence: Iterable[BronzeRecordEvidence],
        *,
        contract_messages: tuple[ValidationMessage, ...] = (),
    ) -> BronzeBundleReceipt:
        self.messages = contract_messages
        target = self.target_for(artifact)
        for item in evidence:
            self.outcomes[item.result.outcome] += 1
            assert item.provenance.raw_uri == target.artifact_uri
        stored = StoredObject(
            uri=target.artifact_uri,
            size_bytes=artifact.local_path.stat().st_size,
            sha256=artifact.expected_sha256 or "0" * 64,
        )
        return BronzeBundleReceipt(
            source=artifact.source,
            ingestion_run_id=artifact.ingestion_run_id,
            artifact=stored,
            evidence_manifest=stored,
            completion_manifest_uri=target.completion_manifest_uri,
        )


def test_service_runs_download_contract_and_bronze_commit() -> None:
    store = CountingStore()
    service = MetSnapshotIngestionService(
        downloader=FixtureDownloader(),
        bronze_store=store,
        museum_id=MET_MUSEUM_ID,
        contract_version="1.0.0",
        batch_size=2,
        clock=lambda: NOW,
        run_id_factory=lambda: RUN_ID,
    )

    result = service.run(NeverRetry())

    assert result.ingestion_run_id == RUN_ID
    assert result.total_records == 3
    assert result.outcome_counts == {
        ContractOutcome.ACCEPTED: 1,
        ContractOutcome.ACCEPTED_WITH_WARNINGS: 0,
        ContractOutcome.QUARANTINED: 1,
        ContractOutcome.REJECTED: 1,
    }
    assert sum(store.outcomes.values()) == 3
    assert result.requested_url == "https://github.example.test/MetObjects.csv"
    assert result.final_url == "https://media.example.test/MetObjects.csv"
    assert result.size_bytes == FIXTURE.stat().st_size
    assert result.sha256 == hashlib.sha256(FIXTURE.read_bytes()).hexdigest()


def test_service_cleans_download_after_store_failure() -> None:
    class FailingStore(CountingStore):
        def store_bundle(
            self,
            *args: object,
            **kwargs: object,
        ) -> BronzeBundleReceipt:
            raise RuntimeError("synthetic storage failure")

    downloader = FixtureDownloader()
    service = MetSnapshotIngestionService(
        downloader=downloader,
        bronze_store=FailingStore(),
        museum_id=MET_MUSEUM_ID,
        contract_version="1.0.0",
        batch_size=2,
        clock=lambda: NOW,
        run_id_factory=lambda: RUN_ID,
    )

    with pytest.raises(RuntimeError, match="synthetic storage failure"):
        service.run(NeverRetry())

    assert downloader.target is not None
    assert not downloader.target.exists()
