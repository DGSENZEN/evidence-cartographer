from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Protocol
from uuid import UUID, uuid4

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from evidence_cartographer.application.bronze import (
    BronzeArtifact,
    BronzeArtifactStore,
    BronzeBundleReceipt,
    BronzeRecordEvidence,
)
from evidence_cartographer.application.contracts import ValidationMessage
from evidence_cartographer.application.retry import RetryDecider
from evidence_cartographer.domain.enums import ContractOutcome, SourceName
from evidence_cartographer.sources.met.contract import MetCsvPreflight
from evidence_cartographer.sources.met.csv import (
    MetCsvEvidenceReader,
    MetEvidenceContext,
)
from evidence_cartographer.sources.met.download import DownloadedMetSnapshot

MET_MUSEUM_ID = UUID("d3fe97c7-14b2-54a5-a58b-ebed9307ae93")


class MetSnapshotDownloadPort(Protocol):
    @property
    def url(self) -> str: ...

    def download(
        self,
        destination_dir: Path,
        retry_decider: RetryDecider,
    ) -> DownloadedMetSnapshot: ...


class MetSnapshotIngestionResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    ingestion_run_id: UUID
    retrieved_at: AwareDatetime
    receipt: BronzeBundleReceipt
    requested_url: str
    final_url: str
    media_type: str
    size_bytes: int = Field(gt=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    total_records: int = Field(ge=0)
    outcome_counts: dict[ContractOutcome, int]
    contract_messages: tuple[ValidationMessage, ...]


class MetSnapshotIngestionService:
    def __init__(
        self,
        *,
        downloader: MetSnapshotDownloadPort,
        bronze_store: BronzeArtifactStore,
        museum_id: UUID,
        contract_version: str,
        batch_size: int,
        clock: Callable[[], datetime] | None = None,
        run_id_factory: Callable[[], UUID] = uuid4,
    ) -> None:
        self._downloader = downloader
        self._bronze_store = bronze_store
        self._museum_id = museum_id
        self._contract_version = contract_version
        self._batch_size = batch_size
        self._clock = clock or _utc_now
        self._run_id_factory = run_id_factory
        self._preflight = MetCsvPreflight()
        self._reader = MetCsvEvidenceReader(batch_size=batch_size)

    @property
    def snapshot_url(self) -> str:
        return self._downloader.url

    @property
    def batch_size(self) -> int:
        return self._batch_size

    def run(self, retry_decider: RetryDecider) -> MetSnapshotIngestionResult:
        with TemporaryDirectory(prefix="ec-met-snapshot-") as temporary:
            ingestion_run_id = self._run_id_factory()
            downloaded = self._downloader.download(
                Path(temporary),
                retry_decider,
            )
            preflight = self._preflight.inspect(downloaded.local_path)
            artifact = BronzeArtifact(
                source=SourceName.MET,
                ingestion_run_id=ingestion_run_id,
                retrieved_at=downloaded.retrieved_at,
                source_url=downloaded.requested_url,
                media_type=downloaded.media_type,
                extension="csv",
                contract_version=self._contract_version,
                local_path=downloaded.local_path,
                expected_sha256=downloaded.sha256,
            )
            target = self._bronze_store.target_for(artifact)
            context = MetEvidenceContext(
                museum_id=self._museum_id,
                ingestion_run_id=ingestion_run_id,
                observed_at=self._clock(),
                retrieved_at=downloaded.retrieved_at,
                source_url=downloaded.requested_url,
                raw_uri=target.artifact_uri,
                raw_checksum=downloaded.sha256,
                contract_version=self._contract_version,
            )
            outcome_counts: dict[ContractOutcome, int] = dict.fromkeys(
                ContractOutcome,
                0,
            )

            def counted_evidence() -> Iterator[BronzeRecordEvidence]:
                for item in self._reader.iter_evidence(
                    downloaded.local_path,
                    preflight,
                    context,
                ):
                    outcome_counts[item.result.outcome] += 1
                    yield item

            receipt = self._bronze_store.store_bundle(
                artifact,
                counted_evidence(),
                contract_messages=preflight.contract_messages,
            )
            result = MetSnapshotIngestionResult(
                ingestion_run_id=ingestion_run_id,
                retrieved_at=downloaded.retrieved_at,
                receipt=receipt,
                requested_url=downloaded.requested_url,
                final_url=downloaded.final_url,
                media_type=downloaded.media_type,
                size_bytes=downloaded.size_bytes,
                sha256=downloaded.sha256,
                total_records=sum(outcome_counts.values()),
                outcome_counts=outcome_counts,
                contract_messages=preflight.contract_messages,
            )
        return result


def _utc_now() -> datetime:
    return datetime.now(UTC)
