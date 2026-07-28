# Met Snapshot Ingestion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Download the complete official Met Open Access CSV, validate every readable row through the versioned Met contract, and commit the unchanged artifact plus evidence through the immutable Bronze boundary.

**Architecture:** A streaming `urllib3` boundary downloads the artifact and records acquisition metadata. A Met-specific preflight and Polars batched reader produce lazy record evidence; an ordinary application service coordinates target resolution and Bronze commit, while Prefect remains a thin wrapper.

**Tech Stack:** Python 3.12, Pydantic 2, urllib3 2.7, Polars 1.x, Pandera Polars, MinIO 7.2, Prefect 3, pytest, Ruff, mypy.

## Global Constraints

- Use the official configurable snapshot URL `https://github.com/metmuseum/openaccess/raw/refs/heads/master/MetObjects.csv`.
- Download and process the complete real CSV; do not substitute a sample in the live test.
- Preserve the downloaded artifact byte-for-byte and keep all CSV values as strings or nulls.
- Never collect the complete snapshot or evidence iterable in memory.
- Use bounded HTTP chunks and configurable Polars batches.
- Enforce the existing 5 GiB (5,368,709,120-byte) single-object ceiling.
- Perform CSV header preflight before any Bronze write.
- Missing required headers fail the run; additional unknown headers create one bundle-level warning.
- Every readable CSV row produces exactly one `BronzeRecordEvidence`.
- Row warnings, quarantine, and rejection do not prevent `_SUCCESS.json`.
- Missing/invalid Object IDs quarantine; later duplicate positive Object IDs reject.
- Resolve the Bronze artifact URI before constructing `SourceRecord`.
- The normal pytest suite downloads and processes the complete real snapshot once per session, with a verified persistent conditional-request cache.
- The live test uses a counting Bronze store and does not require MinIO.
- Keep the application service directly testable and Prefect orchestration thin.
- Do not add API enrichment, Silver mapping, image downloads, PostgreSQL persistence, AIC ingestion, or a CLI.
- Never read, display, modify, stage, or commit the ignored local `.env`.
- Keep Python `>=3.12,<3.13`, `minio==7.2.20`, and `urllib3==2.7.0`.

---

## File Map

- `src/evidence_cartographer/application/bronze.py`: add pre-resolved bundle targets and bundle-level contract messages.
- `src/evidence_cartographer/application/errors.py`: add typed HTTP download and Met CSV failures.
- `src/evidence_cartographer/application/ports.py`: give raw records an optional source row number.
- `src/evidence_cartographer/infrastructure/http.py`: reusable streaming HTTP protocol and urllib3 implementation.
- `src/evidence_cartographer/infrastructure/minio_bronze.py`: implement target resolution and persist contract messages.
- `src/evidence_cartographer/infrastructure/settings.py`: configure Met snapshot acquisition and batch sizes.
- `src/evidence_cartographer/sources/met/download.py`: stream and verify the Met artifact.
- `src/evidence_cartographer/sources/met/contract.py`: header and row contract rules.
- `src/evidence_cartographer/sources/met/csv.py`: Polars batch reader and evidence generation.
- `src/evidence_cartographer/sources/met/service.py`: coordinate the vertical slice.
- `src/evidence_cartographer/orchestration/met.py`: Prefect wrapper and production composition.
- `tests/live/conftest.py`: session/persistent real-snapshot cache.
- `tests/live/test_met_snapshot.py`: complete live snapshot integration.

---

### Task 1: Extend the Bronze boundary for pre-resolved targets and contract messages

**Files:**
- Modify: `src/evidence_cartographer/application/bronze.py`
- Modify: `src/evidence_cartographer/infrastructure/minio_bronze.py`
- Modify: `tests/application/test_bronze.py`
- Modify: `tests/infrastructure/test_minio_bronze.py`

**Interfaces:**
- Produces: `BronzeBundleTarget`
- Produces: `BronzeArtifactStore.target_for(artifact: BronzeArtifact) -> BronzeBundleTarget`
- Changes: `BronzeArtifactStore.store_bundle(..., contract_messages: tuple[ValidationMessage, ...] = ())`
- Persists: `BronzeCompletionManifest.contract_messages`

- [ ] **Step 1: Write failing application-boundary tests**

Append to `tests/application/test_bronze.py`:

```python
from evidence_cartographer.application.contracts import ValidationMessage


def test_bronze_store_resolves_target_before_evidence() -> None:
    target_hints = get_type_hints(BronzeArtifactStore.target_for)
    store_parameters = signature(BronzeArtifactStore.store_bundle).parameters

    assert target_hints["artifact"] is BronzeArtifact
    assert target_hints["return"] is BronzeBundleTarget
    assert store_parameters["contract_messages"].default == ()


def test_completion_manifest_defaults_to_no_contract_messages() -> None:
    now = datetime.now(UTC)
    stored = StoredObject(
        uri="s3://bronze/raw/met/source.csv",
        size_bytes=6,
        sha256="0" * 64,
    )
    manifest = BronzeCompletionManifest(
        source=SourceName.MET,
        ingestion_run_id=uuid4(),
        retrieved_at=now,
        source_url="https://example.test/met.csv",
        contract_version="1.0.0",
        artifact=stored,
        evidence_manifest=stored,
        total_records=0,
        outcome_counts=dict.fromkeys(ContractOutcome, 0),
    )
    assert manifest.contract_messages == ()
    assert ValidationMessage.model_fields["rule_id"].is_required()
```

Add `BronzeBundleTarget`, `BronzeCompletionManifest`, `StoredObject`, and
`ContractOutcome` to the existing imports.

- [ ] **Step 2: Run the tests and verify the missing-interface failure**

Run:

```bash
.venv/bin/pytest tests/application/test_bronze.py -q
```

Expected: FAIL because `BronzeBundleTarget` and `target_for` do not exist.

- [ ] **Step 3: Implement the application models and protocol**

In `src/evidence_cartographer/application/bronze.py`, import
`ValidationMessage`, then add:

```python
class BronzeBundleTarget(BronzeModel):
    artifact_uri: str
    evidence_manifest_uri: str
    completion_manifest_uri: str
```

Add this field to `BronzeCompletionManifest`:

```python
    contract_messages: tuple[ValidationMessage, ...] = ()
```

Replace `BronzeArtifactStore` with:

```python
class BronzeArtifactStore(Protocol):
    def target_for(self, artifact: BronzeArtifact) -> BronzeBundleTarget: ...

    def store_bundle(
        self,
        artifact: BronzeArtifact,
        evidence: Iterable[BronzeRecordEvidence],
        *,
        contract_messages: tuple[ValidationMessage, ...] = (),
    ) -> BronzeBundleReceipt: ...
```

- [ ] **Step 4: Write failing MinIO target/message tests**

In `tests/infrastructure/test_minio_bronze.py`, add:

```python
def test_target_for_matches_uploaded_bundle_locations(tmp_path: Path) -> None:
    path = tmp_path / "met.csv"
    path.write_bytes(b"source")
    client = FakeConditionalObjectClient()
    store = MinioBronzeArtifactStore(client, "bronze", "raw")
    artifact = make_artifact(path)

    target = store.target_for(artifact)
    receipt = store.store_bundle(artifact, ())

    assert target.artifact_uri == receipt.artifact.uri
    assert target.evidence_manifest_uri == receipt.evidence_manifest.uri
    assert target.completion_manifest_uri == receipt.completion_manifest_uri


def test_persists_bundle_contract_messages_once(tmp_path: Path) -> None:
    path = tmp_path / "met.csv"
    path.write_bytes(b"source")
    client = FakeConditionalObjectClient()
    store = MinioBronzeArtifactStore(client, "bronze", "raw")
    message = ValidationMessage(
        rule_id="unexpected_columns",
        message="Met CSV added columns: Future Field",
    )

    store.store_bundle(make_artifact(path), (), contract_messages=(message,))

    success_key = next(key for key in client.objects if key.endswith("_SUCCESS.json"))
    completion = json.loads(client.objects[success_key])
    assert completion["contract_messages"] == [
        {
            "field": None,
            "message": "Met CSV added columns: Future Field",
            "rule_id": "unexpected_columns",
        }
    ]
```

Import `ValidationMessage`.

- [ ] **Step 5: Implement MinIO target resolution and message persistence**

In `src/evidence_cartographer/infrastructure/minio_bronze.py`, import
`ValidationMessage` and `BronzeBundleTarget`. Add:

```python
    def target_for(self, artifact: BronzeArtifact) -> BronzeBundleTarget:
        keys = build_bronze_object_keys(artifact, self._bronze_prefix)
        return BronzeBundleTarget(
            artifact_uri=self._uri(keys.artifact),
            evidence_manifest_uri=self._uri(keys.evidence_manifest),
            completion_manifest_uri=self._uri(keys.completion_manifest),
        )
```

Update `store_bundle`:

```python
    def store_bundle(
        self,
        artifact: BronzeArtifact,
        evidence: Iterable[BronzeRecordEvidence],
        *,
        contract_messages: tuple[ValidationMessage, ...] = (),
    ) -> BronzeBundleReceipt:
```

Pass `contract_messages=contract_messages` into `BronzeCompletionManifest`.
Use `target_for` when constructing returned URIs, while continuing to use the
same deterministic `BronzeObjectKeys` for object names.

- [ ] **Step 6: Run focused and application/infrastructure verification**

Run:

```bash
.venv/bin/pytest tests/application/test_bronze.py tests/infrastructure/test_minio_bronze.py -q
.venv/bin/pytest tests/application tests/infrastructure -q
.venv/bin/ruff check src/evidence_cartographer/application/bronze.py src/evidence_cartographer/infrastructure/minio_bronze.py tests/application/test_bronze.py tests/infrastructure/test_minio_bronze.py
.venv/bin/ruff format --check src/evidence_cartographer/application/bronze.py src/evidence_cartographer/infrastructure/minio_bronze.py tests/application/test_bronze.py tests/infrastructure/test_minio_bronze.py
.venv/bin/mypy src/evidence_cartographer/application/bronze.py src/evidence_cartographer/infrastructure/minio_bronze.py
```

Expected: all commands PASS.

- [ ] **Step 7: Commit the Bronze boundary**

```bash
git add src/evidence_cartographer/application/bronze.py src/evidence_cartographer/infrastructure/minio_bronze.py tests/application/test_bronze.py tests/infrastructure/test_minio_bronze.py
git commit -m "feat: resolve Bronze bundle targets"
```

---

### Task 2: Build the reusable HTTP boundary and Met snapshot downloader

**Files:**
- Create: `src/evidence_cartographer/infrastructure/http.py`
- Create: `src/evidence_cartographer/sources/met/download.py`
- Modify: `src/evidence_cartographer/application/errors.py`
- Modify: `src/evidence_cartographer/infrastructure/settings.py`
- Create: `tests/infrastructure/test_http.py`
- Create: `tests/sources/met/test_download.py`
- Modify: `tests/infrastructure/test_settings.py`

**Interfaces:**
- Produces: `StreamingHttpClient.open(...) -> ContextManager[StreamingHttpResponse]`
- Produces: `Urllib3StreamingHttpClient`
- Produces: `DownloadedMetSnapshot`, `MetSnapshotNotModified`, `MetSnapshotConditional`
- Produces: `MetSnapshotDownloader.download(...)`
- Produces: `MetSnapshotDownloader.download_if_changed(...)`

- [ ] **Step 1: Write failing HTTP-boundary tests**

Create `tests/infrastructure/test_http.py`:

```python
from collections.abc import Mapping
from typing import BinaryIO, get_type_hints

from urllib3 import PoolManager

from evidence_cartographer.infrastructure.http import (
    StreamingHttpClient,
    StreamingHttpResponse,
    Urllib3StreamingHttpClient,
)


def test_urllib3_client_satisfies_streaming_protocol() -> None:
    client = Urllib3StreamingHttpClient(PoolManager())
    typed: StreamingHttpClient = client
    assert typed is client


def test_response_protocol_exposes_streaming_metadata() -> None:
    hints = get_type_hints(StreamingHttpResponse)
    assert hints["status"] is int
    assert hints["headers"] == Mapping[str, str]
    assert hints["final_url"] is str
    assert hints["body"] is BinaryIO
```

- [ ] **Step 2: Run the HTTP tests and verify the missing module failure**

Run:

```bash
.venv/bin/pytest tests/infrastructure/test_http.py -q
```

Expected: FAIL because `infrastructure.http` does not exist.

- [ ] **Step 3: Implement the HTTP protocol and urllib3 adapter**

Create `src/evidence_cartographer/infrastructure/http.py`:

```python
from collections.abc import Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from typing import BinaryIO, Protocol, cast
from urllib.parse import urljoin

import urllib3
from urllib3 import PoolManager, Timeout

REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})


class StreamingHttpResponse(Protocol):
    status: int
    headers: Mapping[str, str]
    final_url: str
    body: BinaryIO


class StreamingHttpClient(Protocol):
    def open(
        self,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        connect_timeout_seconds: float,
        read_timeout_seconds: float,
        max_redirects: int = 5,
    ) -> AbstractContextManager[StreamingHttpResponse]: ...


@dataclass(slots=True)
class _StreamingResponse:
    status: int
    headers: Mapping[str, str]
    final_url: str
    body: BinaryIO


class Urllib3StreamingHttpClient:
    def __init__(self, pool: PoolManager) -> None:
        self._pool = pool

    @contextmanager
    def open(
        self,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        connect_timeout_seconds: float,
        read_timeout_seconds: float,
        max_redirects: int = 5,
    ) -> Iterator[StreamingHttpResponse]:
        current_url = url
        response: urllib3.response.BaseHTTPResponse | None = None
        for redirect_count in range(max_redirects + 1):
            response = self._pool.request(
                "GET",
                current_url,
                headers=dict(headers or {}),
                preload_content=False,
                redirect=False,
                retries=False,
                timeout=Timeout(
                    connect=connect_timeout_seconds,
                    read=read_timeout_seconds,
                ),
            )
            if response.status not in REDIRECT_STATUSES:
                break
            location = response.headers.get("location")
            response.release_conn()
            response = None
            if location is None or redirect_count == max_redirects:
                raise RuntimeError("HTTP redirect chain is invalid or too long")
            current_url = urljoin(current_url, location)

        if response is None:
            raise RuntimeError("HTTP response was not created")
        try:
            yield _StreamingResponse(
                status=response.status,
                headers=cast(Mapping[str, str], response.headers),
                final_url=current_url,
                body=cast(BinaryIO, response),
            )
        finally:
            response.release_conn()
```

- [ ] **Step 4: Add downloader error and settings tests**

Append to `tests/infrastructure/test_settings.py`:

```python
def test_met_snapshot_acquisition_defaults() -> None:
    settings = Settings.model_validate(
        {
            "postgres": {"password": "test"},
            "object_store": {
                "access_key": "test",
                "secret_key": "test",
            },
        }
    )
    assert settings.sources.met_snapshot_url.endswith("/MetObjects.csv")
    assert settings.sources.http_connect_timeout_seconds == 10.0
    assert settings.sources.http_read_timeout_seconds == 300.0
    assert settings.sources.download_chunk_size_bytes == 1024 * 1024
    assert settings.sources.met_csv_batch_size == 50_000
```

Append typed error assertions to `tests/application/test_errors.py`:

```python
from evidence_cartographer.application.errors import (
    DownloadIntegrityError,
    DownloadSizeLimitError,
    HttpDownloadError,
    MetCsvParseError,
    MetCsvSchemaError,
)


def test_source_acquisition_errors_remain_typed() -> None:
    assert issubclass(HttpDownloadError, AcquisitionError)
    assert issubclass(DownloadIntegrityError, AcquisitionError)
    assert issubclass(DownloadSizeLimitError, AcquisitionError)
    assert issubclass(MetCsvSchemaError, ContractError)
    assert issubclass(MetCsvParseError, ContractError)
```

- [ ] **Step 5: Add typed errors and settings**

Append to `src/evidence_cartographer/application/errors.py`:

```python
class HttpDownloadError(AcquisitionError):
    """A source HTTP request failed or returned a terminal status."""


class DownloadIntegrityError(AcquisitionError):
    """A downloaded artifact is empty, truncated, or length-mismatched."""


class DownloadSizeLimitError(AcquisitionError):
    """A downloaded artifact exceeds the supported artifact ceiling."""


class MetCsvSchemaError(ContractError):
    """The Met CSV header does not satisfy the versioned contract."""


class MetCsvParseError(ContractError):
    """The Met CSV cannot be segmented into readable records."""
```

Extend `SourceEndpoints` in
`src/evidence_cartographer/infrastructure/settings.py`:

```python
class SourceEndpoints(BaseModel):
    met_api_base_url: str = "https://collectionapi.metmuseum.org/public/collection/v1"
    met_snapshot_url: str = (
        "https://github.com/metmuseum/openaccess/raw/refs/heads/master/MetObjects.csv"
    )
    aic_api_base_url: str = "https://api.artic.edu/api/v1"
    http_connect_timeout_seconds: float = Field(default=10.0, gt=0)
    http_read_timeout_seconds: float = Field(default=300.0, gt=0)
    download_chunk_size_bytes: int = Field(default=1024 * 1024, gt=0)
    met_csv_batch_size: int = Field(default=50_000, gt=0)
```

- [ ] **Step 6: Write failing downloader tests**

Create `tests/sources/met/test_download.py` with deterministic fake responses:

```python
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path

import pytest

from evidence_cartographer.application.errors import (
    DownloadIntegrityError,
    HttpDownloadError,
)
from evidence_cartographer.application.retry import RetryDecision
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
            kwargs.get("headers")  # type: ignore[arg-type]
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
```

- [ ] **Step 7: Run the downloader tests and verify the missing module failure**

Run:

```bash
.venv/bin/pytest tests/sources/met/test_download.py -q
```

Expected: FAIL because `sources.met.download` does not exist.

- [ ] **Step 8: Implement the downloader models and bounded algorithm**

Create `src/evidence_cartographer/sources/met/download.py` with frozen
Pydantic models:

```python
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
```

Implement `MetSnapshotDownloader` with:

```python
MAX_ARTIFACT_SIZE_BYTES = 5 * 1024**3
RETRYABLE_STATUSES = frozenset({408, 429, 500, 502, 503, 504})


def download(
    self,
    destination_dir: Path,
    retry_decider: RetryDecider,
) -> DownloadedMetSnapshot:
    result = self._run(destination_dir, retry_decider, conditional=None)
    if isinstance(result, MetSnapshotNotModified):
        raise HttpDownloadError("unconditional download returned 304")
    return result


def download_if_changed(
    self,
    destination_dir: Path,
    retry_decider: RetryDecider,
    conditional: MetSnapshotConditional,
) -> DownloadedMetSnapshot | MetSnapshotNotModified:
    return self._run(destination_dir, retry_decider, conditional=conditional)
```

The private attempt must:

1. create the destination directory;
2. remove only `MetObjects.csv.part`;
3. call the injected `StreamingHttpClient`;
4. return `MetSnapshotNotModified` only for conditional HTTP 304;
5. translate non-200 statuses to `HttpDownloadError`;
6. stream `body.read(chunk_size_bytes)` until EOF;
7. update SHA-256/size and stop above `MAX_ARTIFACT_SIZE_BYTES`;
8. reject zero bytes and Content-Length mismatch;
9. flush/close the part file;
10. atomically replace `MetObjects.csv`; and
11. retry only while attempts remain and the injected `RetryDecider` returns
    `RetryDecision.RETRY`.

Do not catch `KeyboardInterrupt`, `SystemExit`, or `BaseException`.

- [ ] **Step 9: Run Task 2 verification**

Run:

```bash
.venv/bin/pytest tests/application/test_errors.py tests/infrastructure/test_http.py tests/infrastructure/test_settings.py tests/sources/met/test_download.py -q
.venv/bin/ruff check src/evidence_cartographer/infrastructure/http.py src/evidence_cartographer/sources/met/download.py src/evidence_cartographer/application/errors.py src/evidence_cartographer/infrastructure/settings.py tests/infrastructure/test_http.py tests/sources/met/test_download.py
.venv/bin/ruff format --check src/evidence_cartographer/infrastructure/http.py src/evidence_cartographer/sources/met/download.py tests/infrastructure/test_http.py tests/sources/met/test_download.py
.venv/bin/mypy src/evidence_cartographer/infrastructure/http.py src/evidence_cartographer/sources/met/download.py
```

Expected: all commands PASS.

- [ ] **Step 10: Commit the acquisition boundary**

```bash
git add src/evidence_cartographer/application/errors.py src/evidence_cartographer/infrastructure/http.py src/evidence_cartographer/infrastructure/settings.py src/evidence_cartographer/sources/met/download.py tests/application/test_errors.py tests/infrastructure/test_http.py tests/infrastructure/test_settings.py tests/sources/met/test_download.py
git commit -m "feat: download Met snapshot artifacts"
```

---

### Task 3: Implement the Met v1 contract and Polars batched evidence reader

**Files:**
- Create: `src/evidence_cartographer/sources/met/contract.py`
- Create: `src/evidence_cartographer/sources/met/csv.py`
- Modify: `src/evidence_cartographer/application/ports.py`
- Create: `tests/sources/met/fixtures/met-small.csv`
- Create: `tests/sources/met/test_contract.py`
- Create: `tests/sources/met/test_csv.py`
- Modify: `tests/application/test_ports.py`

**Interfaces:**
- Produces: `MetCsvPreflight`, `MetCsvPreflightResult`, `MetRowContract`
- Produces: `MetEvidenceContext`
- Produces: `MetCsvEvidenceReader.iter_evidence(...)`
- Changes: `RawRecord.source_row_number: int | None = None`

- [ ] **Step 1: Add the synthetic CSV fixture**

Create `tests/sources/met/fixtures/met-small.csv`:

```csv
Object Number,Is Highlight,Is Timeline Work,Is Public Domain,Object ID,Gallery Number,Department,AccessionYear,Object Name,Title,Culture,Period,Dynasty,Reign,Portfolio,Constituent ID,Artist Role,Artist Prefix,Artist Display Name,Artist Display Bio,Artist Suffix,Artist Alpha Sort,Artist Nationality,Artist Begin Date,Artist End Date,Artist Gender,Artist ULAN URL,Artist Wikidata URL,Object Date,Object Begin Date,Object End Date,Medium,Dimensions,Credit Line,Geography Type,City,State,County,Country,Region,Subregion,Locale,Locus,Excavation,River,Classification,Rights and Reproduction,Link Resource,Object Wikidata URL,Metadata Date,Repository,Tags,Tags AAT URL,Tags Wikidata URL
MET-42,False,False,True,42,,European Paintings,1900,Painting,Synthetic Work,,,,,,,Artist,,Synthetic Artist,,,,,,,,,,1900,1900,1900,Oil,,,,,,,,,,,,,,Paintings,,https://www.metmuseum.org/art/collection/search/42,,2026-07-28T12:00:00Z,Metropolitan Museum of Art,,,
,False,False,,invalid-id,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,
MET-42,False,False,True,42,,European Paintings,1900,Painting,Duplicate Work,,,,,,,Artist,,Synthetic Artist,,,,,,,,,,1900,1900,1900,Oil,,,,,,,,,,,,,,Paintings,,https://www.metmuseum.org/art/collection/search/42,,2026-07-28T12:00:00Z,Metropolitan Museum of Art,,,
```

- [ ] **Step 2: Write failing preflight/contract tests**

Create `tests/sources/met/test_contract.py`:

```python
from pathlib import Path

import pytest

from evidence_cartographer.application.errors import MetCsvSchemaError
from evidence_cartographer.application.ports import RawRecord
from evidence_cartographer.domain.enums import ContractOutcome
from evidence_cartographer.sources.met.contract import (
    MET_V1_KNOWN_COLUMNS,
    MetCsvPreflight,
    MetRowContract,
)


FIXTURE = Path("tests/sources/met/fixtures/met-small.csv")


def test_preflight_accepts_current_header() -> None:
    result = MetCsvPreflight().inspect(FIXTURE)
    assert set(result.columns) == MET_V1_KNOWN_COLUMNS
    assert result.contract_messages == ()


def test_preflight_warns_once_for_added_columns(tmp_path: Path) -> None:
    header = FIXTURE.read_text().splitlines()[0]
    path = tmp_path / "met.csv"
    path.write_text(f"{header},Future Field\n")
    result = MetCsvPreflight().inspect(path)
    assert result.contract_messages[0].rule_id == "unexpected_columns"
    assert "Future Field" in result.contract_messages[0].message


def test_preflight_rejects_missing_required_header(tmp_path: Path) -> None:
    path = tmp_path / "met.csv"
    path.write_text("Object Number,Title\nMET-42,Synthetic\n")
    with pytest.raises(MetCsvSchemaError):
        MetCsvPreflight().inspect(path)


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        (
            {
                "Object ID": "42",
                "Object Number": "MET-42",
                "Department": "European Paintings",
                "Object Name": "Painting",
                "Title": "Synthetic",
                "Link Resource": "https://example.test/42",
                "Metadata Date": "2026-07-28T12:00:00Z",
                "Repository": "Metropolitan Museum of Art",
                "Is Public Domain": "True",
            },
            ContractOutcome.ACCEPTED,
        ),
        (
            {
                "Object ID": "42",
                "Object Number": "",
                "Department": "European Paintings",
                "Object Name": "Painting",
                "Title": "Synthetic",
                "Link Resource": "https://example.test/42",
                "Metadata Date": "not-a-date",
                "Repository": "Metropolitan Museum of Art",
                "Is Public Domain": "",
            },
            ContractOutcome.ACCEPTED_WITH_WARNINGS,
        ),
        (
            {"Object ID": "not-an-id"},
            ContractOutcome.QUARANTINED,
        ),
    ],
)
def test_met_row_outcomes(
    payload: dict[str, str],
    expected: ContractOutcome,
) -> None:
    record = RawRecord(
        source_record_id="42",
        source_row_number=2,
        payload=payload,
    )
    assert MetRowContract().validate(record).outcome is expected
```

- [ ] **Step 3: Run contract tests and verify missing module failure**

Run:

```bash
.venv/bin/pytest tests/sources/met/test_contract.py -q
```

Expected: FAIL because `sources.met.contract` does not exist and `RawRecord`
lacks `source_row_number`.

- [ ] **Step 4: Add source row location and implement the contract**

Modify `RawRecord` in `src/evidence_cartographer/application/ports.py`:

```python
class RawRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    source_record_id: str
    source_row_number: int | None = Field(default=None, ge=2)
    payload: Mapping[str, Any]
```

Import `Field`. Existing callers remain valid because the field defaults to
`None`.

Create `src/evidence_cartographer/sources/met/contract.py` with:

```python
MET_V1_KNOWN_COLUMNS = frozenset(
    {
        "Object Number",
        "Is Highlight",
        "Is Timeline Work",
        "Is Public Domain",
        "Object ID",
        "Gallery Number",
        "Department",
        "AccessionYear",
        "Object Name",
        "Title",
        "Culture",
        "Period",
        "Dynasty",
        "Reign",
        "Portfolio",
        "Constituent ID",
        "Artist Role",
        "Artist Prefix",
        "Artist Display Name",
        "Artist Display Bio",
        "Artist Suffix",
        "Artist Alpha Sort",
        "Artist Nationality",
        "Artist Begin Date",
        "Artist End Date",
        "Artist Gender",
        "Artist ULAN URL",
        "Artist Wikidata URL",
        "Object Date",
        "Object Begin Date",
        "Object End Date",
        "Medium",
        "Dimensions",
        "Credit Line",
        "Geography Type",
        "City",
        "State",
        "County",
        "Country",
        "Region",
        "Subregion",
        "Locale",
        "Locus",
        "Excavation",
        "River",
        "Classification",
        "Rights and Reproduction",
        "Link Resource",
        "Object Wikidata URL",
        "Metadata Date",
        "Repository",
        "Tags",
        "Tags AAT URL",
        "Tags Wikidata URL",
    }
)

MET_V1_REQUIRED_COLUMNS = frozenset(
    {
        "Object ID",
        "Object Number",
        "Is Public Domain",
        "Department",
        "Object Name",
        "Title",
        "Artist Display Name",
        "Object Date",
        "Classification",
        "Link Resource",
        "Metadata Date",
        "Repository",
    }
)
```

Also define:

- `MetCsvPreflightResult(columns, contract_messages)`;
- `MetCsvPreflight.inspect(path)`;
- `parse_positive_object_id(value) -> int | None`; and
- `MetRowContract.validate(record)`.

Use `csv.reader` with `encoding="utf-8-sig"` and `newline=""`. Sort missing and
unexpected column names before constructing deterministic messages. Row
validation must emit:

```python
ValidationMessage(
    rule_id="invalid_object_id",
    field="Object ID",
    message="Object ID must be a positive integer",
)
```

for quarantine, and field-specific `missing_recommended_value`,
`invalid_public_domain_value`, and `invalid_metadata_date` warnings. Parse
metadata dates with `datetime.fromisoformat(value.replace("Z", "+00:00"))`.

- [ ] **Step 5: Write failing batched evidence tests**

Create `tests/sources/met/test_csv.py`:

```python
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

from evidence_cartographer.domain.enums import ContractOutcome
from evidence_cartographer.sources.met.contract import MetCsvPreflight
from evidence_cartographer.sources.met.csv import (
    MetCsvEvidenceReader,
    MetEvidenceContext,
)

FIXTURE = Path("tests/sources/met/fixtures/met-small.csv")
RUN_ID = UUID("00000000-0000-0000-0000-000000000042")
MUSEUM_ID = UUID("d3fe97c7-14b2-54a5-a58b-ebed9307ae93")
NOW = datetime(2026, 7, 28, 12, 0, tzinfo=UTC)


def context() -> MetEvidenceContext:
    return MetEvidenceContext(
        museum_id=MUSEUM_ID,
        ingestion_run_id=RUN_ID,
        observed_at=NOW,
        retrieved_at=NOW,
        source_url="https://github.example.test/MetObjects.csv",
        raw_uri="s3://bronze/raw/met/source.csv",
        raw_checksum="0" * 64,
        contract_version="1.0.0",
    )


def test_emits_one_evidence_item_per_row_with_all_outcomes() -> None:
    preflight = MetCsvPreflight().inspect(FIXTURE)
    evidence = list(
        MetCsvEvidenceReader(batch_size=2).iter_evidence(
            FIXTURE,
            preflight,
            context(),
        )
    )
    assert [item.result.outcome for item in evidence] == [
        ContractOutcome.ACCEPTED,
        ContractOutcome.QUARANTINED,
        ContractOutcome.REJECTED,
    ]
    assert evidence[0].provenance.source_record_id == "42"
    assert evidence[1].provenance.source_record_id == f"{RUN_ID}:row:3"
    assert evidence[2].result.messages[0].rule_id == "duplicate_object_id"


def test_preserves_string_values_and_provenance() -> None:
    preflight = MetCsvPreflight().inspect(FIXTURE)
    first = next(
        MetCsvEvidenceReader(batch_size=1).iter_evidence(
            FIXTURE,
            preflight,
            context(),
        )
    )
    assert first.provenance.raw_uri == "s3://bronze/raw/met/source.csv"
    assert first.provenance.raw_checksum == "0" * 64
    assert first.provenance.attribution_text == "Synthetic Artist"
```

- [ ] **Step 6: Run evidence tests and verify missing module failure**

Run:

```bash
.venv/bin/pytest tests/sources/met/test_csv.py -q
```

Expected: FAIL because `sources.met.csv` does not exist.

- [ ] **Step 7: Implement the Polars/Pandera evidence reader**

Create `src/evidence_cartographer/sources/met/csv.py`.

Define:

```python
class MetEvidenceContext(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    museum_id: UUID
    ingestion_run_id: UUID
    observed_at: AwareDatetime
    retrieved_at: AwareDatetime
    source_url: str
    raw_uri: str
    raw_checksum: str = Field(pattern=r"^[0-9a-f]{64}$")
    contract_version: str
```

`MetCsvEvidenceReader.iter_evidence` must:

1. build a Pandera Polars schema with required columns as nullable strings;
2. call `pl.scan_csv` with `schema_overrides` mapping every preflight column to
   `pl.String`, `infer_schema_length=0`, `encoding="utf8"`,
   `empty_string_is_null=True`, `row_index_name="__source_row_number"`, and
   `row_index_offset=2`;
3. stream batches with `collect_batches(chunk_size=self._batch_size)`;
4. validate the batch schema;
5. iterate named rows and remove `__source_row_number` from payload;
6. parse Object ID and create the fallback `<run-id>:row:<row-number>`;
7. reject later duplicate positive IDs with:

```python
ContractResult(
    outcome=ContractOutcome.REJECTED,
    messages=(
        ValidationMessage(
            rule_id="duplicate_object_id",
            field="Object ID",
            message=f"Object ID {object_id} repeats within this snapshot",
        ),
    ),
)
```

8. otherwise call `MetRowContract.validate`;
9. build the `SourceRecord` exactly as follows; and
10. yield `BronzeRecordEvidence` immediately.

```python
SourceRecord(
    created_at=context.observed_at,
    museum_id=context.museum_id,
    source=SourceName.MET,
    source_record_id=source_record_id,
    contract_version=context.contract_version,
    ingestion_run_id=context.ingestion_run_id,
    observed_at=context.observed_at,
    source_url=context.source_url,
    retrieval_status=RetrievalStatus.CACHED,
    retrieved_at=context.retrieved_at,
    raw_uri=context.raw_uri,
    acquisition_context=AcquisitionContext(
        mode=IngestionMode.FULL_SNAPSHOT,
    ),
    raw_checksum=context.raw_checksum,
    attribution_text=payload.get("Artist Display Name"),
    outcome=result.outcome,
)
```

Translate Polars, Pandera, Unicode, and CSV failures to `MetCsvParseError`
without catching evidence-consumer or storage exceptions.

- [ ] **Step 8: Run Task 3 verification**

Run:

```bash
.venv/bin/pytest tests/application/test_ports.py tests/sources/met/test_contract.py tests/sources/met/test_csv.py -q
.venv/bin/pytest tests/sources -q
.venv/bin/ruff check src/evidence_cartographer/application/ports.py src/evidence_cartographer/sources/met tests/application/test_ports.py tests/sources/met
.venv/bin/ruff format --check src/evidence_cartographer/application/ports.py src/evidence_cartographer/sources/met tests/application/test_ports.py tests/sources/met
.venv/bin/mypy src/evidence_cartographer/application/ports.py src/evidence_cartographer/sources/met
```

Expected: all commands PASS.

- [ ] **Step 9: Commit the Met contract and reader**

```bash
git add src/evidence_cartographer/application/ports.py src/evidence_cartographer/sources/met/contract.py src/evidence_cartographer/sources/met/csv.py tests/application/test_ports.py tests/sources/met
git commit -m "feat: validate Met CSV records"
```

---

### Task 4: Implement the Met snapshot application service

**Files:**
- Create: `src/evidence_cartographer/sources/met/service.py`
- Create: `tests/sources/met/test_service.py`

**Interfaces:**
- Consumes: `MetSnapshotDownloader`, `MetCsvPreflight`, `MetCsvEvidenceReader`, `BronzeArtifactStore`
- Produces: `MetSnapshotIngestionService.run(retry_decider: RetryDecider) -> MetSnapshotIngestionResult`
- Produces: `MET_MUSEUM_ID`

- [ ] **Step 1: Write a failing service orchestration test**

Create `tests/sources/met/test_service.py`:

```python
import hashlib
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

from evidence_cartographer.application.bronze import (
    BronzeArtifact,
    BronzeBundleReceipt,
    BronzeBundleTarget,
    BronzeRecordEvidence,
    StoredObject,
)
from evidence_cartographer.application.contracts import ValidationMessage
from evidence_cartographer.application.retry import RetryDecision, RetryDecider
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
        for item in evidence:
            self.outcomes[item.result.outcome] += 1
            assert item.provenance.raw_uri == self.target_for(artifact).artifact_uri
        stored = StoredObject(
            uri=self.target_for(artifact).artifact_uri,
            size_bytes=artifact.local_path.stat().st_size,
            sha256=artifact.expected_sha256 or "0" * 64,
        )
        return BronzeBundleReceipt(
            source=artifact.source,
            ingestion_run_id=artifact.ingestion_run_id,
            artifact=stored,
            evidence_manifest=stored,
            completion_manifest_uri=self.target_for(artifact).completion_manifest_uri,
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
```

- [ ] **Step 2: Run the service test and verify missing module failure**

Run:

```bash
.venv/bin/pytest tests/sources/met/test_service.py -q
```

Expected: FAIL because `sources.met.service` does not exist.

- [ ] **Step 3: Implement the typed service result and orchestration**

Create `src/evidence_cartographer/sources/met/service.py`.

Use:

```python
MET_MUSEUM_ID = UUID("d3fe97c7-14b2-54a5-a58b-ebed9307ae93")


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
```

Define the structural downloader boundary:

```python
class MetSnapshotDownloadPort(Protocol):
    def download(
        self,
        destination_dir: Path,
        retry_decider: RetryDecider,
    ) -> DownloadedMetSnapshot: ...
```

Type the storage dependency as the existing `BronzeArtifactStore` protocol so
both the production adapter and test doubles type-check.

`MetSnapshotIngestionService.run` must:

1. allocate `TemporaryDirectory(prefix="ec-met-snapshot-")`;
2. generate the run ID;
3. call `downloader.download`;
4. preflight the downloaded path;
5. create `BronzeArtifact` using download metadata and expected SHA;
6. call `bronze_store.target_for`;
7. create `MetEvidenceContext` with the target artifact URI;
8. wrap the evidence iterator with an outcome counter;
9. call `store_bundle(..., contract_messages=preflight.contract_messages)`;
10. construct the result only after the iterator was fully consumed; and
11. copy requested/final URL, media type, size, and SHA-256 into the result; and
12. exit the temporary directory before returning.

Do not catch acquisition, contract, evidence, or storage errors; the temporary
directory context performs cleanup and Prefect records the typed failure.

- [ ] **Step 4: Add cleanup-on-failure coverage**

Append:

```python
def test_service_cleans_download_after_store_failure() -> None:
    class FailingStore(CountingStore):
        def store_bundle(self, *args: object, **kwargs: object) -> BronzeBundleReceipt:
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
```

- [ ] **Step 5: Run Task 4 verification**

Run:

```bash
.venv/bin/pytest tests/sources/met/test_service.py -q
.venv/bin/pytest tests/sources/met -q
.venv/bin/ruff check src/evidence_cartographer/sources/met/service.py tests/sources/met/test_service.py
.venv/bin/ruff format --check src/evidence_cartographer/sources/met/service.py tests/sources/met/test_service.py
.venv/bin/mypy src/evidence_cartographer/sources/met/service.py
```

Expected: all commands PASS.

- [ ] **Step 6: Commit the application service**

```bash
git add src/evidence_cartographer/sources/met/service.py tests/sources/met/test_service.py
git commit -m "feat: orchestrate Met snapshot ingestion"
```

---

### Task 5: Add Prefect orchestration and production composition

**Files:**
- Create: `src/evidence_cartographer/orchestration/met.py`
- Create: `tests/orchestration/test_met.py`
- Modify: `docs/architecture.md`

**Interfaces:**
- Produces: `build_met_snapshot_service(settings: Settings) -> MetSnapshotIngestionService`
- Produces: `build_met_snapshot_flow(service, retry_decider) -> Flow[..., MetSnapshotIngestionResult]`

- [ ] **Step 1: Write failing Prefect delegation and composition tests**

Create `tests/orchestration/test_met.py`:

```python
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
```

- [ ] **Step 2: Run orchestration tests and verify missing module failure**

Run:

```bash
.venv/bin/pytest tests/orchestration/test_met.py -q
```

Expected: FAIL because `orchestration.met` does not exist.

- [ ] **Step 3: Implement production composition and thin flow**

Create `src/evidence_cartographer/orchestration/met.py`.

`build_met_snapshot_service` constructs, in order:

```python
pool = urllib3.PoolManager()
http_client = Urllib3StreamingHttpClient(pool)
downloader = MetSnapshotDownloader(
    http_client=http_client,
    url=settings.sources.met_snapshot_url,
    connect_timeout_seconds=settings.sources.http_connect_timeout_seconds,
    read_timeout_seconds=settings.sources.http_read_timeout_seconds,
    chunk_size_bytes=settings.sources.download_chunk_size_bytes,
    max_attempts=3,
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
```

Return `MetSnapshotIngestionService` with `MET_MUSEUM_ID`,
`settings.contracts.met_version`, and `settings.sources.met_csv_batch_size`.

Expose read-only `snapshot_url` and `batch_size` properties on the service for
composition verification; do not expose credentials or concrete client
members.

Build the flow:

```python
def build_met_snapshot_flow(
    service: MetSnapshotIngestionService,
    retry_decider: RetryDecider,
) -> Flow[..., MetSnapshotIngestionResult]:
    @flow(name="met-full-snapshot-ingestion")
    def met_snapshot_flow() -> MetSnapshotIngestionResult:
        return service.run(retry_decider)

    return met_snapshot_flow
```

- [ ] **Step 4: Document the vertical slice**

Append to `docs/architecture.md`:

```markdown
## Met full snapshot

The Met full-snapshot service streams the official weekly CSV into a temporary
artifact, performs required-header preflight, resolves its immutable Bronze
target, and emits one contract-evidence record per CSV row through bounded
Polars batches. Prefect delegates to the application service; production
composition supplies urllib3 and the conditional MinIO client.

Missing headers fail before storage. Added headers are persisted once in the
completion manifest. Row warnings, quarantine, and rejection remain committed
to Bronze and do not affect Silver/Gold policy.
```

- [ ] **Step 5: Run Task 5 verification**

Run:

```bash
.venv/bin/pytest tests/orchestration/test_met.py tests/orchestration/test_flows.py -q
.venv/bin/pytest tests/orchestration -q
.venv/bin/ruff check src/evidence_cartographer/orchestration/met.py tests/orchestration/test_met.py
.venv/bin/ruff format --check src/evidence_cartographer/orchestration/met.py tests/orchestration/test_met.py
.venv/bin/mypy src/evidence_cartographer/orchestration/met.py
```

Expected: all commands PASS.

- [ ] **Step 6: Commit orchestration**

```bash
git add src/evidence_cartographer/orchestration/met.py tests/orchestration/test_met.py docs/architecture.md
git commit -m "feat: compose Met snapshot flow"
```

---

### Task 6: Add the complete live Met snapshot integration and final verification

**Files:**
- Create: `tests/live/__init__.py`
- Create: `tests/live/conftest.py`
- Create: `tests/live/test_met_snapshot.py`
- Modify: `README.md`

**Interfaces:**
- Uses: official complete Met CSV
- Uses: persistent `.pytest_cache/met` or `EC_TEST_MET_SNAPSHOT_CACHE_DIR`
- Does not use: local `.env`, MinIO, PostgreSQL, Met Collection API

- [ ] **Step 1: Add live cache helpers and session fixture**

Create `tests/live/conftest.py` with:

```python
import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path

import pytest
import urllib3
from pydantic import ValidationError

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
    def decide(self, failure: Exception, attempt: int) -> RetryDecision:
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
                update={"retrieved_at": result.retrieved_at}
            )

    metadata = result.model_dump(mode="json", exclude={"local_path"})
    metadata_part = metadata_path.with_suffix(".json.part")
    metadata_part.write_text(json.dumps(metadata, sort_keys=True))
    metadata_part.replace(metadata_path)
    return result
```

Create an empty `tests/live/__init__.py`.

- [ ] **Step 2: Write the full live vertical-slice test**

Create `tests/live/test_met_snapshot.py`:

```python
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
    RetryDecision,
    RetryDecider,
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
```

Do not mark the test optional or skip it when offline. Internet/upstream
failure is intentionally a normal-suite failure.

- [ ] **Step 3: Run the live test against the official complete snapshot**

Run:

```bash
.venv/bin/pytest tests/live/test_met_snapshot.py -q -s
```

Expected: PASS after downloading and processing the complete official
`MetObjects.csv`. The first run downloads approximately 303 MB; later runs may
receive HTTP 304 and reuse the verified cache.

This step requires network permission. Do not replace it with a fixture or
sample if the network is unavailable.

- [ ] **Step 4: Document normal-suite network behavior**

Add to `README.md`:

```markdown
## Live source tests

The normal pytest suite downloads and processes the complete official Met Open
Access CSV. The verified artifact is cached under `.pytest_cache/met`; set
`EC_TEST_MET_SNAPSHOT_CACHE_DIR` to use another local cache directory.
Subsequent sessions issue a conditional request and reuse the cache only after
size and SHA-256 verification.

The live test intentionally fails when the public source is unavailable or its
required schema changes. It does not require MinIO, PostgreSQL, or local
credentials.
```

- [ ] **Step 5: Run complete verification**

Run:

```bash
.venv/bin/pytest
.venv/bin/ruff check .
.venv/bin/ruff format --check .
.venv/bin/mypy src
mkdir -p /tmp/evidence-cartographer-dbt-profile-met
cp dbt/profiles.yml.example /tmp/evidence-cartographer-dbt-profile-met/profiles.yml
.venv/bin/dbt parse --project-dir dbt --profiles-dir /tmp/evidence-cartographer-dbt-profile-met --quiet
EC_POSTGRES__PASSWORD=verification-only EC_OBJECT_STORE__ACCESS_KEY=verification-only EC_OBJECT_STORE__SECRET_KEY=verification-only docker compose --env-file /dev/null -f infra/compose.yaml config --quiet
git diff --check
git check-ignore --quiet .env
! git ls-files --error-unmatch .env >/dev/null 2>&1
git status --short
```

Expected:

- the full real snapshot is conditionally checked and fully processed;
- all tests PASS;
- Ruff, formatting, mypy, dbt, and Compose checks exit 0;
- `.env` remains ignored and untracked; and
- only intentional Task 6 files remain before commit.

- [ ] **Step 6: Commit the live integration**

```bash
git add tests/live README.md
git commit -m "test: ingest complete live Met snapshot"
```

---

## Plan Self-Review

- **Spec coverage:** Tasks cover the production HTTP downloader, official URL,
  redirect handling, integrity metadata, retries, CSV preflight, known/required
  columns, bundle messages, Polars/Pandera batches, all row outcomes,
  provenance, pre-resolved Bronze URI, service cleanup, Prefect composition,
  persistent conditional cache, complete real-data processing, and final
  verification.
- **Scope control:** No task adds API enrichment, Silver mapping, image
  retrieval, PostgreSQL persistence, AIC ingestion, or a CLI.
- **Type consistency:** `BronzeBundleTarget`, `DownloadedMetSnapshot`,
  `MetCsvPreflightResult`, `MetEvidenceContext`, and
  `MetSnapshotIngestionResult` are defined before their consumers.
- **Streaming behavior:** HTTP bytes, Polars batches, and evidence remain lazy;
  only the integer duplicate-ID set grows with record count.
- **Failure semantics:** Artifact/header failures stop before completion;
  readable row failures remain evidence and do not block Bronze commit.
- **Live-test decision:** The normal test suite requires the real complete Met
  source and intentionally fails when the network or required upstream schema
  is unavailable.
