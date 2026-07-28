from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Protocol

import urllib3
from minio import Minio

from evidence_cartographer.application.retry import RetryDecider
from evidence_cartographer.infrastructure.conditional_object_client import (
    MinioConditionalObjectClient,
)
from evidence_cartographer.infrastructure.http import Urllib3StreamingHttpClient
from evidence_cartographer.infrastructure.minio_bronze import (
    MinioBronzeArtifactStore,
)
from evidence_cartographer.infrastructure.settings import Settings
from evidence_cartographer.sources.met.download import MetSnapshotDownloader
from evidence_cartographer.sources.met.service import (
    MET_MUSEUM_ID,
    MetSnapshotIngestionResult,
    MetSnapshotIngestionService,
)

if TYPE_CHECKING:
    from prefect import Flow


class MetSnapshotRunner(Protocol):
    def run(
        self,
        retry_decider: RetryDecider,
    ) -> MetSnapshotIngestionResult: ...


def build_met_snapshot_service(settings: Settings) -> MetSnapshotIngestionService:
    pool = urllib3.PoolManager()
    http_client = Urllib3StreamingHttpClient(pool)
    downloader = MetSnapshotDownloader(
        http_client=http_client,
        url=settings.sources.met_snapshot_url,
        connect_timeout_seconds=settings.sources.http_connect_timeout_seconds,
        read_timeout_seconds=settings.sources.http_read_timeout_seconds,
        chunk_size_bytes=settings.sources.download_chunk_size_bytes,
        max_attempts=3,
        clock=lambda: datetime.now(UTC),
    )
    minio_client = Minio(
        settings.object_store.endpoint,
        access_key=settings.object_store.access_key,
        secret_key=settings.object_store.secret_key.get_secret_value(),
        secure=settings.object_store.secure,
    )
    conditional_client = MinioConditionalObjectClient(minio_client)
    bronze_store = MinioBronzeArtifactStore(
        conditional_client,
        settings.object_store.bronze_bucket,
        settings.lake.bronze_prefix,
    )
    return MetSnapshotIngestionService(
        downloader=downloader,
        bronze_store=bronze_store,
        museum_id=MET_MUSEUM_ID,
        contract_version=settings.contracts.met_version,
        batch_size=settings.sources.met_csv_batch_size,
    )


def build_met_snapshot_flow(
    service: MetSnapshotRunner,
    retry_decider: RetryDecider,
) -> Flow[..., MetSnapshotIngestionResult]:
    from prefect import flow

    @flow(name="met-full-snapshot-ingestion")
    def met_snapshot_flow() -> MetSnapshotIngestionResult:
        return service.run(retry_decider)

    return met_snapshot_flow
