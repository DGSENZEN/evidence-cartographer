import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path

import pytest
import urllib3
from pydantic import ValidationError

from evidence_cartographer.application.errors import EvidenceCartographerError
from evidence_cartographer.application.retry import RetryDecision
from evidence_cartographer.infrastructure.http import Urllib3StreamingHttpClient
from evidence_cartographer.sources.met.download import (
    DownloadedMetSnapshot,
    MetSnapshotConditional,
    MetSnapshotDownloader,
    MetSnapshotNotModified,
)

MET_SNAPSHOT_URL = (
    "https://github.com/metmuseum/openaccess/raw/refs/heads/master/MetObjects.csv"
)


class RetryLiveDownload:
    def decide(
        self,
        failure: EvidenceCartographerError,
        attempt: int,
    ) -> RetryDecision:
        return RetryDecision.RETRY


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


@pytest.fixture(scope="session")
def live_met_snapshot() -> DownloadedMetSnapshot:
    override = os.environ.get("EC_TEST_MET_SNAPSHOT_CACHE_DIR")
    cache_dir = Path(override) if override else Path(".pytest_cache/met")
    cache_dir.mkdir(parents=True, exist_ok=True)
    artifact_path = cache_dir / "MetObjects.csv"
    metadata_path = cache_dir / "MetObjects.metadata.json"

    cached_snapshot: DownloadedMetSnapshot | None = None
    if artifact_path.is_file() and metadata_path.is_file():
        try:
            candidate = DownloadedMetSnapshot.model_validate(
                {
                    **json.loads(metadata_path.read_text()),
                    "local_path": artifact_path,
                }
            )
            if (
                artifact_path.stat().st_size == candidate.size_bytes
                and _sha256(artifact_path) == candidate.sha256
            ):
                cached_snapshot = candidate
        except (
            json.JSONDecodeError,
            OSError,
            TypeError,
            ValidationError,
        ):
            cached_snapshot = None

    downloader = MetSnapshotDownloader(
        http_client=Urllib3StreamingHttpClient(urllib3.PoolManager()),
        url=MET_SNAPSHOT_URL,
        connect_timeout_seconds=10,
        read_timeout_seconds=300,
        chunk_size_bytes=1024 * 1024,
        max_attempts=3,
        clock=lambda: datetime.now(UTC),
    )

    if cached_snapshot is None:
        result = downloader.download(cache_dir, RetryLiveDownload())
    else:
        result = downloader.download_if_changed(
            cache_dir,
            RetryLiveDownload(),
            MetSnapshotConditional(
                etag=cached_snapshot.etag,
                last_modified=cached_snapshot.last_modified,
            ),
        )
        if isinstance(result, MetSnapshotNotModified):
            return cached_snapshot.model_copy(
                update={
                    "retrieved_at": result.retrieved_at,
                    "etag": result.etag or cached_snapshot.etag,
                    "last_modified": (
                        result.last_modified or cached_snapshot.last_modified
                    ),
                }
            )

    metadata = result.model_dump(mode="json", exclude={"local_path"})
    metadata_part = metadata_path.with_suffix(".json.part")
    metadata_part.write_text(json.dumps(metadata, sort_keys=True))
    metadata_part.replace(metadata_path)
    return result
