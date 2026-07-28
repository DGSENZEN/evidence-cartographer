import shutil
from collections.abc import Iterable
from pathlib import Path

from evidence_cartographer.application.bronze import (
    BronzeArtifact,
    BronzeBundleReceipt,
    BronzeBundleTarget,
    BronzeRecordEvidence,
    StoredObject,
)
from evidence_cartographer.application.contracts import ValidationMessage
from evidence_cartographer.application.retry import (
    RetryDecider,
    RetryDecision,
)
from evidence_cartographer.domain.enums import ContractOutcome
from evidence_cartographer.sources.met.download import DownloadedMetSnapshot
from evidence_cartographer.sources.met.service import (
    MET_MUSEUM_ID,
    MetSnapshotIngestionService,
)


class NeverRetry:
    def decide(self, failure: Exception, attempt: int) -> RetryDecision:
        return RetryDecision.STOP


class CachedSnapshotDownloader:
    def __init__(self, snapshot: DownloadedMetSnapshot) -> None:
        self._snapshot = snapshot

    @property
    def url(self) -> str:
        return self._snapshot.requested_url

    def download(
        self,
        destination_dir: Path,
        retry_decider: RetryDecider,
    ) -> DownloadedMetSnapshot:
        target = destination_dir / "MetObjects.csv"
        shutil.copyfile(self._snapshot.local_path, target)
        return self._snapshot.model_copy(update={"local_path": target})


class CountingBronzeStore:
    def __init__(self) -> None:
        self.outcome_counts = dict.fromkeys(ContractOutcome, 0)
        self.iteration_count = 0
        self.retained_evidence_count = 0

    def target_for(self, artifact: BronzeArtifact) -> BronzeBundleTarget:
        return BronzeBundleTarget(
            artifact_uri="s3://bronze-live/raw/met/source.csv",
            evidence_manifest_uri=("s3://bronze-live/raw/met/records.manifest.jsonl"),
            completion_manifest_uri="s3://bronze-live/raw/met/_SUCCESS.json",
        )

    def store_bundle(
        self,
        artifact: BronzeArtifact,
        evidence: Iterable[BronzeRecordEvidence],
        *,
        contract_messages: tuple[ValidationMessage, ...] = (),
    ) -> BronzeBundleReceipt:
        self.iteration_count += 1
        target = self.target_for(artifact)
        for item in evidence:
            self.outcome_counts[item.result.outcome] += 1
            assert item.provenance.raw_uri == target.artifact_uri

        artifact_object = StoredObject(
            uri=target.artifact_uri,
            size_bytes=artifact.local_path.stat().st_size,
            sha256=artifact.expected_sha256 or "0" * 64,
        )
        evidence_object = StoredObject(
            uri=target.evidence_manifest_uri,
            size_bytes=0,
            sha256="0" * 64,
        )
        return BronzeBundleReceipt(
            source=artifact.source,
            ingestion_run_id=artifact.ingestion_run_id,
            artifact=artifact_object,
            evidence_manifest=evidence_object,
            completion_manifest_uri=target.completion_manifest_uri,
        )


def test_complete_live_met_snapshot_produces_reconciled_evidence(
    live_met_snapshot: DownloadedMetSnapshot,
) -> None:
    store = CountingBronzeStore()
    service = MetSnapshotIngestionService(
        downloader=CachedSnapshotDownloader(live_met_snapshot),
        bronze_store=store,
        museum_id=MET_MUSEUM_ID,
        contract_version="1.0.0",
        batch_size=50_000,
    )

    result = service.run(NeverRetry())

    assert live_met_snapshot.size_bytes > 0
    assert len(live_met_snapshot.sha256) == 64
    assert result.total_records > 400_000
    assert sum(result.outcome_counts.values()) == result.total_records
    assert sum(store.outcome_counts.values()) == result.total_records
    assert (
        result.outcome_counts[ContractOutcome.ACCEPTED]
        + result.outcome_counts[ContractOutcome.ACCEPTED_WITH_WARNINGS]
        > 0
    )
    assert store.iteration_count == 1
    assert store.retained_evidence_count == 0
