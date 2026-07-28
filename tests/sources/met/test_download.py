from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path
from typing import cast

import pytest

from evidence_cartographer.application.errors import (
    DownloadIntegrityError,
    DownloadSizeLimitError,
    HttpDownloadError,
)
from evidence_cartographer.application.retry import RetryDecision
from evidence_cartographer.sources.met import download as download_module
from evidence_cartographer.sources.met.download import (
    DownloadedMetSnapshot,
    MetSnapshotConditional,
    MetSnapshotDownloader,
    MetSnapshotNotModified,
)


class FakeResponse:
    def __init__(
        self,
        status: int,
        body: bytes = b"",
        headers: Mapping[str, str] | None = None,
        final_url: str = "https://media.example.test/MetObjects.csv",
    ) -> None:
        self.status = status
        self.headers = headers or {}
        self.final_url = final_url
        self.body = BytesIO(body)


class FakeHttpClient:
    def __init__(self, responses: list[FakeResponse | Exception]) -> None:
        self.responses = responses
        self.request_headers: list[Mapping[str, str] | None] = []

    @contextmanager
    def open(self, url: str, **kwargs: object) -> Iterator[FakeResponse]:
        self.request_headers.append(
            cast(Mapping[str, str] | None, kwargs.get("headers"))
        )
        result = self.responses.pop(0)
        if isinstance(result, Exception):
            raise result
        yield result


class RetryAll:
    def decide(self, failure: Exception, attempt: int) -> RetryDecision:
        return RetryDecision.RETRY


class NeverRetry:
    def decide(self, failure: Exception, attempt: int) -> RetryDecision:
        return RetryDecision.STOP


NOW = datetime(2026, 7, 28, 12, 0, tzinfo=UTC)


def make_downloader(client: FakeHttpClient) -> MetSnapshotDownloader:
    return MetSnapshotDownloader(
        http_client=client,
        url="https://github.example.test/MetObjects.csv",
        connect_timeout_seconds=1,
        read_timeout_seconds=1,
        chunk_size_bytes=4,
        max_attempts=3,
        clock=lambda: NOW,
    )


def test_streams_snapshot_and_records_integrity(tmp_path: Path) -> None:
    body = b"Object ID,Title\n42,Synthetic\n"
    client = FakeHttpClient(
        [
            FakeResponse(
                200,
                body,
                {
                    "content-length": str(len(body)),
                    "content-type": "text/csv",
                    "etag": '"met-v1"',
                    "last-modified": "Tue, 28 Jul 2026 12:00:00 GMT",
                },
            )
        ]
    )

    result = make_downloader(client).download(tmp_path, RetryAll())

    assert isinstance(result, DownloadedMetSnapshot)
    assert result.local_path.read_bytes() == body
    assert result.size_bytes == len(body)
    assert (
        result.sha256
        == "ea163f58efb1a2b5a5ddd8f66c258a8e2000e8fab21463727faaafcf7763fc7a"
    )
    assert result.etag == '"met-v1"'
    assert result.final_url == "https://media.example.test/MetObjects.csv"


def test_conditional_304_does_not_replace_cache(tmp_path: Path) -> None:
    cached = tmp_path / "MetObjects.csv"
    cached.write_bytes(b"cached")
    client = FakeHttpClient([FakeResponse(304, headers={"etag": '"met-v1"'})])

    result = make_downloader(client).download_if_changed(
        tmp_path,
        RetryAll(),
        MetSnapshotConditional(
            etag='"met-v1"',
            last_modified="Tue, 28 Jul 2026 12:00:00 GMT",
        ),
    )

    assert isinstance(result, MetSnapshotNotModified)
    assert cached.read_bytes() == b"cached"
    assert client.request_headers == [
        {
            "If-None-Match": '"met-v1"',
            "If-Modified-Since": "Tue, 28 Jul 2026 12:00:00 GMT",
        }
    ]


def test_retries_after_removing_partial_file(tmp_path: Path) -> None:
    client = FakeHttpClient(
        [
            FakeResponse(503, b"unavailable"),
            FakeResponse(200, b"Object ID\n42\n", {"content-length": "13"}),
        ]
    )

    result = make_downloader(client).download(tmp_path, RetryAll())

    assert isinstance(result, DownloadedMetSnapshot)
    assert result.local_path.read_bytes() == b"Object ID\n42\n"
    assert not (tmp_path / "MetObjects.csv.part").exists()


@pytest.mark.parametrize(
    "response",
    [
        FakeResponse(404),
        FakeResponse(200, b""),
        FakeResponse(200, b"short", {"content-length": "10"}),
    ],
)
def test_rejects_terminal_or_incomplete_downloads(
    tmp_path: Path,
    response: FakeResponse,
) -> None:
    with pytest.raises((HttpDownloadError, DownloadIntegrityError)):
        make_downloader(FakeHttpClient([response])).download(tmp_path, NeverRetry())


def test_rejects_artifacts_over_the_single_put_ceiling(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(download_module, "MAX_ARTIFACT_SIZE_BYTES", 4)
    client = FakeHttpClient([FakeResponse(200, b"12345")])

    with pytest.raises(DownloadSizeLimitError):
        make_downloader(client).download(tmp_path, NeverRetry())

    assert not (tmp_path / "MetObjects.csv").exists()
    assert not (tmp_path / "MetObjects.csv.part").exists()
