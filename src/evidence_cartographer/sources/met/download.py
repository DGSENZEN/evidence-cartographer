import hashlib
from collections.abc import Callable, Mapping
from datetime import datetime
from pathlib import Path

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from evidence_cartographer.application.errors import (
    DownloadIntegrityError,
    DownloadSizeLimitError,
    HttpDownloadError,
)
from evidence_cartographer.application.retry import RetryDecider, RetryDecision
from evidence_cartographer.infrastructure.conditional_object_client import (
    MAX_SINGLE_PUT_SIZE_BYTES,
)
from evidence_cartographer.infrastructure.http import StreamingHttpClient

MAX_ARTIFACT_SIZE_BYTES = MAX_SINGLE_PUT_SIZE_BYTES
RETRYABLE_STATUSES = frozenset({408, 429, 500, 502, 503, 504})


class DownloadedMetSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    local_path: Path
    requested_url: str
    final_url: str
    media_type: str
    etag: str | None = None
    last_modified: str | None = None
    size_bytes: int = Field(gt=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    retrieved_at: AwareDatetime


class MetSnapshotConditional(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    etag: str | None = None
    last_modified: str | None = None


class MetSnapshotNotModified(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    requested_url: str
    final_url: str
    etag: str | None = None
    last_modified: str | None = None
    retrieved_at: AwareDatetime


class _RetryableHttpDownloadError(HttpDownloadError):
    pass


class MetSnapshotDownloader:
    def __init__(
        self,
        *,
        http_client: StreamingHttpClient,
        url: str,
        connect_timeout_seconds: float,
        read_timeout_seconds: float,
        chunk_size_bytes: int,
        max_attempts: int,
        clock: Callable[[], datetime],
    ) -> None:
        if chunk_size_bytes < 1:
            raise ValueError("chunk_size_bytes must be at least 1")
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        self._http_client = http_client
        self._url = url
        self._connect_timeout_seconds = connect_timeout_seconds
        self._read_timeout_seconds = read_timeout_seconds
        self._chunk_size_bytes = chunk_size_bytes
        self._max_attempts = max_attempts
        self._clock = clock

    @property
    def url(self) -> str:
        return self._url

    def download(
        self,
        destination_dir: Path,
        retry_decider: RetryDecider,
    ) -> DownloadedMetSnapshot:
        result = self._run(destination_dir, retry_decider, conditional=None)
        if isinstance(result, MetSnapshotNotModified):
            raise HttpDownloadError("unconditional download returned HTTP 304")
        return result

    def download_if_changed(
        self,
        destination_dir: Path,
        retry_decider: RetryDecider,
        conditional: MetSnapshotConditional,
    ) -> DownloadedMetSnapshot | MetSnapshotNotModified:
        return self._run(destination_dir, retry_decider, conditional=conditional)

    def _run(
        self,
        destination_dir: Path,
        retry_decider: RetryDecider,
        *,
        conditional: MetSnapshotConditional | None,
    ) -> DownloadedMetSnapshot | MetSnapshotNotModified:
        try:
            destination_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise HttpDownloadError(
                f"could not create download directory {destination_dir}"
            ) from exc

        part_path = destination_dir / "MetObjects.csv.part"
        for attempt in range(1, self._max_attempts + 1):
            try:
                part_path.unlink(missing_ok=True)
                return self._attempt(destination_dir, part_path, conditional)
            except DownloadSizeLimitError:
                part_path.unlink(missing_ok=True)
                raise
            except (DownloadIntegrityError, _RetryableHttpDownloadError) as exc:
                part_path.unlink(missing_ok=True)
                if (
                    attempt < self._max_attempts
                    and retry_decider.decide(exc, attempt) is RetryDecision.RETRY
                ):
                    continue
                raise
            except HttpDownloadError:
                part_path.unlink(missing_ok=True)
                raise
            except Exception as exc:
                part_path.unlink(missing_ok=True)
                failure = _RetryableHttpDownloadError(
                    f"HTTP download failed for {self._url}: {exc}"
                )
                if (
                    attempt < self._max_attempts
                    and retry_decider.decide(failure, attempt) is RetryDecision.RETRY
                ):
                    continue
                raise failure from exc
        raise AssertionError("download attempts exhausted without a result")

    def _attempt(
        self,
        destination_dir: Path,
        part_path: Path,
        conditional: MetSnapshotConditional | None,
    ) -> DownloadedMetSnapshot | MetSnapshotNotModified:
        request_headers = _conditional_headers(conditional)
        with self._http_client.open(
            self._url,
            headers=request_headers,
            connect_timeout_seconds=self._connect_timeout_seconds,
            read_timeout_seconds=self._read_timeout_seconds,
        ) as response:
            headers = _normalized_headers(response.headers)
            if response.status == 304 and conditional is not None:
                return MetSnapshotNotModified(
                    requested_url=self._url,
                    final_url=response.final_url,
                    etag=headers.get("etag"),
                    last_modified=headers.get("last-modified"),
                    retrieved_at=self._clock(),
                )
            if response.status in RETRYABLE_STATUSES:
                raise _RetryableHttpDownloadError(
                    f"HTTP {response.status} while downloading {self._url}"
                )
            if response.status != 200:
                raise HttpDownloadError(
                    f"HTTP {response.status} while downloading {self._url}"
                )

            expected_length = _content_length(headers)
            digest = hashlib.sha256()
            size_bytes = 0
            with part_path.open("wb") as destination:
                while chunk := response.body.read(self._chunk_size_bytes):
                    size_bytes += len(chunk)
                    if size_bytes > MAX_ARTIFACT_SIZE_BYTES:
                        raise DownloadSizeLimitError(
                            f"download exceeds {MAX_ARTIFACT_SIZE_BYTES} bytes"
                        )
                    destination.write(chunk)
                    digest.update(chunk)

            if size_bytes == 0:
                raise DownloadIntegrityError("downloaded artifact is empty")
            if expected_length is not None and size_bytes != expected_length:
                raise DownloadIntegrityError(
                    f"expected {expected_length} bytes, downloaded {size_bytes}"
                )

            target_path = destination_dir / "MetObjects.csv"
            part_path.replace(target_path)
            return DownloadedMetSnapshot(
                local_path=target_path,
                requested_url=self._url,
                final_url=response.final_url,
                media_type=headers.get("content-type", "text/csv"),
                etag=headers.get("etag"),
                last_modified=headers.get("last-modified"),
                size_bytes=size_bytes,
                sha256=digest.hexdigest(),
                retrieved_at=self._clock(),
            )


def _conditional_headers(
    conditional: MetSnapshotConditional | None,
) -> dict[str, str]:
    if conditional is None:
        return {}
    headers: dict[str, str] = {}
    if conditional.etag is not None:
        headers["If-None-Match"] = conditional.etag
    if conditional.last_modified is not None:
        headers["If-Modified-Since"] = conditional.last_modified
    return headers


def _normalized_headers(headers: Mapping[str, str]) -> dict[str, str]:
    return {key.lower(): value for key, value in headers.items()}


def _content_length(headers: Mapping[str, str]) -> int | None:
    value = headers.get("content-length")
    if value is None:
        return None
    try:
        parsed = int(value)
    except ValueError as exc:
        raise DownloadIntegrityError(
            f"invalid Content-Length header: {value!r}"
        ) from exc
    if parsed < 0:
        raise DownloadIntegrityError(f"invalid Content-Length header: {value!r}")
    return parsed
