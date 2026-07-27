# Bronze Artifact Storage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist complete source artifacts and streamed record-level contract evidence as immutable, completion-marked Bronze bundles in MinIO.

**Architecture:** Application models define an acquired artifact, record evidence, stored-object metadata, the completed bundle receipt, and the `BronzeArtifactStore` port. Infrastructure code builds deterministic partitioned keys and implements the port with an injected MinIO-compatible client; the original file, NDJSON evidence, and `_SUCCESS.json` are uploaded in that order.

**Tech Stack:** Python 3.12, Pydantic 2, MinIO Python SDK, pytest, Ruff, mypy, uv.

## Global Constraints

- Store the original acquired artifact byte-for-byte unchanged.
- Stream artifacts and record evidence; never materialize a complete museum snapshot or evidence iterable in memory.
- Use `{bronze_prefix}/{source}/year=YYYY/month=MM/day=DD/run={run_id}/` as the deterministic key prefix.
- Upload `source.{extension}`, `records.manifest.jsonl`, then `_SUCCESS.json`.
- Treat `_SUCCESS.json` as the only committed-bundle marker.
- Never overwrite or automatically delete an object.
- A retry after a partial write uses a new ingestion-run ID.
- Preserve the complete `SourceRecord` and `ContractResult` for every evidence line.
- Do not implement acquisition, parsing, mapping, bucket creation, live-service tests, compression, or storage cleanup.
- Delete `.env.example` from Git; retain the ignored local `.env` without reading, displaying, modifying, or committing it.
- Keep Python `>=3.12,<3.13` and the existing locked dependency set.

---

## File Map

- `src/evidence_cartographer/application/bronze.py`: Bronze command, evidence, manifest, receipt, and port types.
- `src/evidence_cartographer/application/ports.py`: remove the superseded per-record `BronzeWriter`.
- `src/evidence_cartographer/application/errors.py`: typed Bronze storage failures.
- `src/evidence_cartographer/infrastructure/object_keys.py`: deterministic Bronze key construction.
- `src/evidence_cartographer/infrastructure/minio_bronze.py`: streaming MinIO adapter.
- `tests/application/test_bronze.py`: application type and port invariants.
- `tests/infrastructure/test_object_keys.py`: deterministic key and extension tests.
- `tests/infrastructure/test_minio_bronze.py`: fake-client storage behavior.
- `tests/application/test_ports.py`: remove assertions for the superseded `BronzeWriter`.
- `tests/config/test_manifests.py`: repository environment-file policy.
- `README.md`: local `.env` instructions without a template file.
- `.env.example`: remove from Git.

---

### Task 1: Define the Bronze bundle application contract

**Files:**
- Create: `src/evidence_cartographer/application/bronze.py`
- Modify: `src/evidence_cartographer/application/ports.py`
- Modify: `src/evidence_cartographer/application/errors.py`
- Create: `tests/application/test_bronze.py`
- Modify: `tests/application/test_ports.py`

**Interfaces:**
- Consumes: `SourceName`, `SourceRecord`, `ContractOutcome`, and `ContractResult`
- Produces: `BronzeArtifact`, `BronzeRecordEvidence`, `StoredObject`, `BronzeCompletionManifest`, `BronzeBundleReceipt`, and `BronzeArtifactStore`
- Replaces: `BronzeWriter.append(record, provenance, result)`

- [ ] **Step 1: Write failing application-contract tests**

```python
# tests/application/test_bronze.py
from datetime import UTC, datetime
from inspect import Parameter, signature
from pathlib import Path
from typing import get_type_hints
from uuid import uuid4

import pytest
from pydantic import ValidationError

from evidence_cartographer.application.bronze import (
    BronzeArtifact,
    BronzeArtifactStore,
    BronzeRecordEvidence,
    BronzeBundleReceipt,
)
from evidence_cartographer.application.contracts import ContractResult
from evidence_cartographer.domain.enums import SourceName
from evidence_cartographer.domain.models import SourceRecord


def test_bronze_artifact_normalizes_a_safe_extension() -> None:
    artifact = BronzeArtifact(
        source=SourceName.AIC,
        ingestion_run_id=uuid4(),
        retrieved_at=datetime.now(UTC),
        source_url="https://example.test/aic.json.gz",
        media_type="application/gzip",
        extension=".JSON.GZ",
        contract_version="1.0.0",
        local_path=Path("aic.json.gz"),
    )
    assert artifact.extension == "json.gz"


@pytest.mark.parametrize("extension", ("../json", "json/gz", "", "."))
def test_bronze_artifact_rejects_unsafe_extensions(extension: str) -> None:
    with pytest.raises(ValidationError):
        BronzeArtifact(
            source=SourceName.MET,
            ingestion_run_id=uuid4(),
            retrieved_at=datetime.now(UTC),
            source_url="https://example.test/met.csv",
            media_type="text/csv",
            extension=extension,
            contract_version="1.0.0",
            local_path=Path("met.csv"),
        )


def test_bronze_artifact_store_accepts_lazy_evidence() -> None:
    annotations = get_type_hints(BronzeArtifactStore.store_bundle)
    parameters = signature(BronzeArtifactStore.store_bundle).parameters

    assert annotations["artifact"] is BronzeArtifact
    assert parameters["evidence"].default is Parameter.empty
    assert annotations["return"] is BronzeBundleReceipt
    assert BronzeRecordEvidence.model_fields["provenance"].annotation is SourceRecord
    assert BronzeRecordEvidence.model_fields["result"].annotation is ContractResult
```

In `tests/application/test_ports.py`, remove the import of `BronzeWriter`,
remove `test_bronze_writer_requires_contract_validation_evidence`, and retain
all other port tests unchanged.

- [ ] **Step 2: Run tests and verify the missing module failure**

Run: `.venv/bin/pytest tests/application/test_bronze.py tests/application/test_ports.py -q`

Expected: FAIL during collection with `ModuleNotFoundError: No module named 'evidence_cartographer.application.bronze'`.

- [ ] **Step 3: Implement the Bronze application models and port**

```python
# src/evidence_cartographer/application/bronze.py
import re
from collections.abc import Iterable
from pathlib import Path
from typing import Protocol
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
)

from evidence_cartographer.application.contracts import ContractResult
from evidence_cartographer.domain.enums import ContractOutcome, SourceName
from evidence_cartographer.domain.models import SourceRecord

SAFE_EXTENSION = re.compile(r"^[a-z0-9]+(?:[._-][a-z0-9]+)*$")


class BronzeModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class BronzeArtifact(BronzeModel):
    source: SourceName
    ingestion_run_id: UUID
    retrieved_at: AwareDatetime
    source_url: str
    media_type: str = Field(min_length=1)
    extension: str
    contract_version: str = Field(min_length=1)
    local_path: Path
    expected_sha256: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )

    @field_validator("extension", mode="before")
    @classmethod
    def normalize_extension(cls, value: object) -> object:
        if not isinstance(value, str):
            return value
        normalized = value.removeprefix(".").lower()
        if len(normalized) > 32 or SAFE_EXTENSION.fullmatch(normalized) is None:
            raise ValueError("extension must be a safe file suffix")
        return normalized


class BronzeRecordEvidence(BronzeModel):
    provenance: SourceRecord
    result: ContractResult


class StoredObject(BronzeModel):
    uri: str
    size_bytes: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class BronzeCompletionManifest(BronzeModel):
    source: SourceName
    ingestion_run_id: UUID
    retrieved_at: AwareDatetime
    source_url: str
    contract_version: str
    artifact: StoredObject
    evidence_manifest: StoredObject
    total_records: int = Field(ge=0)
    outcome_counts: dict[ContractOutcome, int]


class BronzeBundleReceipt(BronzeModel):
    source: SourceName
    ingestion_run_id: UUID
    artifact: StoredObject
    evidence_manifest: StoredObject
    completion_manifest_uri: str


class BronzeArtifactStore(Protocol):
    def store_bundle(
        self,
        artifact: BronzeArtifact,
        evidence: Iterable[BronzeRecordEvidence],
    ) -> BronzeBundleReceipt: ...
```

- [ ] **Step 4: Replace the old port and add typed errors**

Delete the `BronzeWriter` protocol from
`src/evidence_cartographer/application/ports.py` and remove its now-unused
`ContractResult` import. Leave `ContractValidator` and all other protocols
unchanged.

Append to `src/evidence_cartographer/application/errors.py`:

```python
class ArtifactNotFoundError(StorageError):
    """The local acquisition artifact is missing or is not a regular file."""


class ArtifactIntegrityError(StorageError):
    """The acquired artifact does not match its expected checksum."""


class ObjectAlreadyExistsError(StorageError):
    """A deterministic Bronze object key already exists."""


class ManifestSerializationError(StorageError):
    """Record evidence could not be serialized into the Bronze manifest."""


class ObjectStoreError(StorageError):
    """The object store rejected a Bronze operation."""
```

- [ ] **Step 5: Run focused and full application tests**

Run: `.venv/bin/pytest tests/application/test_bronze.py tests/application/test_ports.py -q`

Expected: PASS.

Run: `.venv/bin/pytest tests/application -q`

Expected: PASS.

Run: `.venv/bin/ruff check src/evidence_cartographer/application tests/application`

Expected: PASS.

Run: `.venv/bin/mypy src/evidence_cartographer/application`

Expected: PASS.

- [ ] **Step 6: Commit the application boundary**

```bash
git add src/evidence_cartographer/application tests/application
git commit -m "feat: define Bronze artifact bundle contract"
```

---

### Task 2: Build deterministic Bronze object keys

**Files:**
- Create: `src/evidence_cartographer/infrastructure/object_keys.py`
- Create: `tests/infrastructure/test_object_keys.py`

**Interfaces:**
- Consumes: `BronzeArtifact`
- Produces: `BronzeObjectKeys`
- Produces: `build_bronze_object_keys(artifact: BronzeArtifact, bronze_prefix: str) -> BronzeObjectKeys`

- [ ] **Step 1: Write failing deterministic-key tests**

```python
# tests/infrastructure/test_object_keys.py
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

import pytest

from evidence_cartographer.application.bronze import BronzeArtifact
from evidence_cartographer.domain.enums import SourceName
from evidence_cartographer.infrastructure.object_keys import (
    build_bronze_object_keys,
)


def test_builds_utc_partitioned_bundle_keys() -> None:
    artifact = BronzeArtifact(
        source=SourceName.MET,
        ingestion_run_id=UUID("00000000-0000-0000-0000-000000000042"),
        retrieved_at=datetime(
            2026,
            7,
            27,
            23,
            30,
            tzinfo=timezone(timedelta(hours=-5)),
        ),
        source_url="https://example.test/met.csv",
        media_type="text/csv",
        extension="csv",
        contract_version="1.0.0",
        local_path=Path("met.csv"),
    )

    keys = build_bronze_object_keys(artifact, "/raw/")

    expected_prefix = (
        "raw/met/year=2026/month=07/day=28/"
        "run=00000000-0000-0000-0000-000000000042"
    )
    assert keys.artifact == f"{expected_prefix}/source.csv"
    assert keys.evidence_manifest == f"{expected_prefix}/records.manifest.jsonl"
    assert keys.completion_manifest == f"{expected_prefix}/_SUCCESS.json"


@pytest.mark.parametrize("prefix", ("", "/", "../raw", "raw//objects"))
def test_rejects_unsafe_bronze_prefixes(prefix: str) -> None:
    artifact = BronzeArtifact(
        source=SourceName.AIC,
        ingestion_run_id=UUID("00000000-0000-0000-0000-000000000042"),
        retrieved_at=datetime(2026, 7, 27, tzinfo=UTC),
        source_url="https://example.test/aic.json",
        media_type="application/json",
        extension="json",
        contract_version="1.0.0",
        local_path=Path("aic.json"),
    )
    with pytest.raises(ValueError):
        build_bronze_object_keys(artifact, prefix)
```

- [ ] **Step 2: Run tests and verify the missing module failure**

Run: `.venv/bin/pytest tests/infrastructure/test_object_keys.py -q`

Expected: FAIL during collection because `infrastructure.object_keys` does not exist.

- [ ] **Step 3: Implement deterministic key construction**

```python
# src/evidence_cartographer/infrastructure/object_keys.py
from dataclasses import dataclass
from datetime import UTC

from evidence_cartographer.application.bronze import BronzeArtifact


@dataclass(frozen=True, slots=True)
class BronzeObjectKeys:
    artifact: str
    evidence_manifest: str
    completion_manifest: str


def build_bronze_object_keys(
    artifact: BronzeArtifact,
    bronze_prefix: str,
) -> BronzeObjectKeys:
    normalized_prefix = bronze_prefix.strip("/")
    parts = normalized_prefix.split("/")
    if (
        not normalized_prefix
        or any(part in {"", ".", ".."} for part in parts)
        or "\\" in normalized_prefix
    ):
        raise ValueError("bronze_prefix must contain safe non-empty path segments")

    retrieved_at = artifact.retrieved_at.astimezone(UTC)
    bundle_prefix = (
        f"{normalized_prefix}/{artifact.source.value}/"
        f"year={retrieved_at:%Y}/month={retrieved_at:%m}/day={retrieved_at:%d}/"
        f"run={artifact.ingestion_run_id}"
    )
    return BronzeObjectKeys(
        artifact=f"{bundle_prefix}/source.{artifact.extension}",
        evidence_manifest=f"{bundle_prefix}/records.manifest.jsonl",
        completion_manifest=f"{bundle_prefix}/_SUCCESS.json",
    )
```

- [ ] **Step 4: Run key tests and static checks**

Run: `.venv/bin/pytest tests/infrastructure/test_object_keys.py -q`

Expected: PASS.

Run: `.venv/bin/ruff check src/evidence_cartographer/infrastructure/object_keys.py tests/infrastructure/test_object_keys.py`

Expected: PASS.

Run: `.venv/bin/mypy src/evidence_cartographer/infrastructure/object_keys.py`

Expected: PASS.

- [ ] **Step 5: Commit key construction**

```bash
git add src/evidence_cartographer/infrastructure/object_keys.py tests/infrastructure/test_object_keys.py
git commit -m "feat: add deterministic Bronze object keys"
```

---

### Task 3: Implement the streaming MinIO Bronze adapter

**Files:**
- Create: `src/evidence_cartographer/infrastructure/minio_bronze.py`
- Create: `tests/infrastructure/test_minio_bronze.py`

**Interfaces:**
- Consumes: `BronzeArtifact`, lazy `Iterable[BronzeRecordEvidence]`, and `BronzeObjectKeys`
- Produces: `MinioBronzeArtifactStore.store_bundle(...) -> BronzeBundleReceipt`
- Depends on: injected `MinioClient` structural protocol with `stat_object` and `put_object`

- [ ] **Step 1: Write a fake client and failing success-path test**

Create `tests/infrastructure/test_minio_bronze.py` with these helpers and the
first test:

```python
import hashlib
import json
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import BinaryIO
from uuid import UUID, uuid4

import pytest
from minio.error import S3Error

from evidence_cartographer.application.bronze import (
    BronzeArtifact,
    BronzeRecordEvidence,
)
from evidence_cartographer.application.contracts import (
    ContractResult,
    ValidationMessage,
)
from evidence_cartographer.application.errors import (
    ArtifactIntegrityError,
    ArtifactNotFoundError,
    ManifestSerializationError,
    ObjectAlreadyExistsError,
    ObjectStoreError,
)
from evidence_cartographer.domain.enums import (
    ContractOutcome,
    IngestionMode,
    RetrievalStatus,
    SourceName,
)
from evidence_cartographer.domain.models import AcquisitionContext, SourceRecord
from evidence_cartographer.infrastructure.minio_bronze import (
    MinioBronzeArtifactStore,
)

RUN_ID = UUID("00000000-0000-0000-0000-000000000042")
RETRIEVED_AT = datetime(2026, 7, 27, 18, 30, tzinfo=UTC)


def missing_object_error(bucket: str, object_name: str) -> S3Error:
    return S3Error(
        response=None,  # type: ignore[arg-type]
        code="NoSuchKey",
        message="not found",
        resource=object_name,
        request_id="request",
        host_id="host",
        bucket_name=bucket,
        object_name=object_name,
    )


class FakeMinioClient:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.content_types: dict[str, str] = {}
        self.upload_order: list[str] = []
        self.fail_on: str | None = None

    def stat_object(self, bucket_name: str, object_name: str) -> object:
        if object_name not in self.objects:
            raise missing_object_error(bucket_name, object_name)
        return object()

    def put_object(
        self,
        bucket_name: str,
        object_name: str,
        data: BinaryIO,
        length: int,
        content_type: str = "application/octet-stream",
    ) -> object:
        if self.fail_on == object_name:
            raise RuntimeError("synthetic MinIO failure")
        payload = data.read(length)
        assert len(payload) == length
        self.objects[object_name] = payload
        self.content_types[object_name] = content_type
        self.upload_order.append(object_name)
        return object()


def make_artifact(path: Path, expected_sha256: str | None = None) -> BronzeArtifact:
    return BronzeArtifact(
        source=SourceName.MET,
        ingestion_run_id=RUN_ID,
        retrieved_at=RETRIEVED_AT,
        source_url="https://example.test/met.csv",
        media_type="text/csv",
        extension="csv",
        contract_version="1.0.0",
        local_path=path,
        expected_sha256=expected_sha256,
    )


def make_evidence(
    outcome: ContractOutcome,
    source_record_id: str,
) -> BronzeRecordEvidence:
    provenance = SourceRecord(
        created_at=RETRIEVED_AT,
        museum_id=uuid4(),
        source=SourceName.MET,
        source_record_id=source_record_id,
        contract_version="1.0.0",
        ingestion_run_id=RUN_ID,
        observed_at=RETRIEVED_AT,
        source_url="https://example.test/met.csv",
        retrieval_status=RetrievalStatus.CACHED,
        retrieved_at=RETRIEVED_AT,
        raw_uri="s3://bronze/raw/met/source.csv",
        acquisition_context=AcquisitionContext(mode=IngestionMode.FULL_SNAPSHOT),
        outcome=outcome,
    )
    return BronzeRecordEvidence(
        provenance=provenance,
        result=ContractResult(
            outcome=outcome,
            messages=(
                ValidationMessage(
                    rule_id="synthetic",
                    message=f"{outcome.value} evidence",
                ),
            ),
        ),
    )


def test_stores_original_artifact_evidence_and_completion_marker(
    tmp_path: Path,
) -> None:
    artifact_bytes = b"Object ID,Title\n42,Synthetic work\n"
    artifact_path = tmp_path / "met.csv"
    artifact_path.write_bytes(artifact_bytes)
    client = FakeMinioClient()
    store = MinioBronzeArtifactStore(client, "bronze", "raw")
    consumed: list[str] = []

    def evidence() -> Iterable[BronzeRecordEvidence]:
        for outcome, record_id in (
            (ContractOutcome.ACCEPTED, "42"),
            (ContractOutcome.QUARANTINED, "43"),
        ):
            consumed.append(record_id)
            yield make_evidence(outcome, record_id)

    receipt = store.store_bundle(make_artifact(artifact_path), evidence())

    artifact_key, evidence_key, success_key = client.upload_order
    assert artifact_key.endswith("/source.csv")
    assert evidence_key.endswith("/records.manifest.jsonl")
    assert success_key.endswith("/_SUCCESS.json")
    assert client.objects[artifact_key] == artifact_bytes
    assert consumed == ["42", "43"]
    assert client.content_types == {
        artifact_key: "text/csv",
        evidence_key: "application/x-ndjson",
        success_key: "application/json",
    }

    evidence_payload = client.objects[evidence_key]
    evidence_lines = evidence_payload.decode().splitlines()
    assert len(evidence_lines) == 2
    assert all(
        line
        == json.dumps(
            json.loads(line),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        for line in evidence_lines
    )
    assert json.loads(evidence_lines[1])["result"]["messages"][0]["rule_id"] == (
        "synthetic"
    )

    completion = json.loads(client.objects[success_key])
    assert completion["total_records"] == 2
    assert completion["outcome_counts"] == {
        "accepted": 1,
        "accepted_with_warnings": 0,
        "quarantined": 1,
        "rejected": 0,
    }
    assert completion["artifact"]["sha256"] == hashlib.sha256(
        artifact_bytes
    ).hexdigest()
    assert completion["evidence_manifest"]["sha256"] == hashlib.sha256(
        evidence_payload
    ).hexdigest()
    assert receipt.artifact.size_bytes == len(artifact_bytes)
    assert receipt.evidence_manifest.size_bytes == len(evidence_payload)
    assert receipt.completion_manifest_uri == f"s3://bronze/{success_key}"
```

- [ ] **Step 2: Run the success test and verify the missing adapter failure**

Run: `.venv/bin/pytest tests/infrastructure/test_minio_bronze.py::test_stores_original_artifact_evidence_and_completion_marker -q`

Expected: FAIL during collection because `infrastructure.minio_bronze` does not exist.

- [ ] **Step 3: Implement the adapter**

```python
# src/evidence_cartographer/infrastructure/minio_bronze.py
import hashlib
import json
from collections.abc import Iterable
from tempfile import SpooledTemporaryFile
from io import BytesIO
from typing import BinaryIO, Protocol

from minio.error import S3Error

from evidence_cartographer.application.bronze import (
    BronzeArtifact,
    BronzeArtifactStore,
    BronzeBundleReceipt,
    BronzeCompletionManifest,
    BronzeRecordEvidence,
    StoredObject,
)
from evidence_cartographer.application.errors import (
    ArtifactIntegrityError,
    ArtifactNotFoundError,
    ManifestSerializationError,
    ObjectAlreadyExistsError,
    ObjectStoreError,
)
from evidence_cartographer.domain.enums import ContractOutcome
from evidence_cartographer.infrastructure.object_keys import (
    BronzeObjectKeys,
    build_bronze_object_keys,
)

HASH_CHUNK_SIZE = 1024 * 1024
SPOOL_MAX_SIZE = 8 * 1024 * 1024
MISSING_OBJECT_CODES = frozenset({"NoSuchKey", "NoSuchObject", "NoSuchResource"})


class MinioClient(Protocol):
    def stat_object(self, bucket_name: str, object_name: str) -> object: ...

    def put_object(
        self,
        bucket_name: str,
        object_name: str,
        data: BinaryIO,
        length: int,
        content_type: str = "application/octet-stream",
    ) -> object: ...


class MinioBronzeArtifactStore(BronzeArtifactStore):
    def __init__(
        self,
        client: MinioClient,
        bucket_name: str,
        bronze_prefix: str,
    ) -> None:
        self._client = client
        self._bucket_name = bucket_name
        self._bronze_prefix = bronze_prefix

    def store_bundle(
        self,
        artifact: BronzeArtifact,
        evidence: Iterable[BronzeRecordEvidence],
    ) -> BronzeBundleReceipt:
        if not artifact.local_path.is_file():
            raise ArtifactNotFoundError(str(artifact.local_path))

        size_bytes, sha256 = self._file_metadata(artifact)
        keys = build_bronze_object_keys(artifact, self._bronze_prefix)
        self._assert_bundle_absent(keys)
        artifact_object = StoredObject(
            uri=self._uri(keys.artifact),
            size_bytes=size_bytes,
            sha256=sha256,
        )

        with artifact.local_path.open("rb") as source:
            self._put(keys.artifact, source, size_bytes, artifact.media_type)

        evidence_object, total_records, outcome_counts = self._write_evidence(
            artifact,
            evidence,
            keys.evidence_manifest,
        )
        completion = BronzeCompletionManifest(
            source=artifact.source,
            ingestion_run_id=artifact.ingestion_run_id,
            retrieved_at=artifact.retrieved_at,
            source_url=artifact.source_url,
            contract_version=artifact.contract_version,
            artifact=artifact_object,
            evidence_manifest=evidence_object,
            total_records=total_records,
            outcome_counts=outcome_counts,
        )
        completion_bytes = _canonical_json(completion.model_dump(mode="json"))
        self._put(
            keys.completion_manifest,
            BytesIO(completion_bytes),
            len(completion_bytes),
            "application/json",
        )
        return BronzeBundleReceipt(
            source=artifact.source,
            ingestion_run_id=artifact.ingestion_run_id,
            artifact=artifact_object,
            evidence_manifest=evidence_object,
            completion_manifest_uri=self._uri(keys.completion_manifest),
        )

    def _file_metadata(self, artifact: BronzeArtifact) -> tuple[int, str]:
        digest = hashlib.sha256()
        size_bytes = 0
        with artifact.local_path.open("rb") as source:
            while chunk := source.read(HASH_CHUNK_SIZE):
                digest.update(chunk)
                size_bytes += len(chunk)
        sha256 = digest.hexdigest()
        if artifact.expected_sha256 is not None and sha256 != artifact.expected_sha256:
            raise ArtifactIntegrityError(
                f"expected {artifact.expected_sha256}, computed {sha256}"
            )
        return size_bytes, sha256

    def _assert_bundle_absent(self, keys: BronzeObjectKeys) -> None:
        for key in (
            keys.artifact,
            keys.evidence_manifest,
            keys.completion_manifest,
        ):
            if self._exists(key):
                raise ObjectAlreadyExistsError(self._uri(key))

    def _exists(self, key: str) -> bool:
        try:
            self._client.stat_object(self._bucket_name, key)
        except S3Error as exc:
            if exc.code in MISSING_OBJECT_CODES:
                return False
            raise ObjectStoreError(f"could not inspect {self._uri(key)}") from exc
        except Exception as exc:
            raise ObjectStoreError(f"could not inspect {self._uri(key)}") from exc
        return True

    def _write_evidence(
        self,
        artifact: BronzeArtifact,
        evidence: Iterable[BronzeRecordEvidence],
        key: str,
    ) -> tuple[StoredObject, int, dict[ContractOutcome, int]]:
        counts = dict.fromkeys(ContractOutcome, 0)
        total_records = 0
        digest = hashlib.sha256()
        try:
            with SpooledTemporaryFile(max_size=SPOOL_MAX_SIZE, mode="w+b") as spool:
                for item in evidence:
                    self._validate_evidence(artifact, item)
                    line = _canonical_json(item.model_dump(mode="json")) + b"\n"
                    spool.write(line)
                    digest.update(line)
                    counts[item.result.outcome] += 1
                    total_records += 1
                size_bytes = spool.tell()
                spool.seek(0)
                self._put(key, spool, size_bytes, "application/x-ndjson")
        except (ManifestSerializationError, ObjectStoreError):
            raise
        except Exception as exc:
            raise ManifestSerializationError(
                "could not serialize Bronze record evidence"
            ) from exc
        return (
            StoredObject(
                uri=self._uri(key),
                size_bytes=size_bytes,
                sha256=digest.hexdigest(),
            ),
            total_records,
            counts,
        )

    @staticmethod
    def _validate_evidence(
        artifact: BronzeArtifact,
        evidence: BronzeRecordEvidence,
    ) -> None:
        provenance = evidence.provenance
        if provenance.source is not artifact.source:
            raise ManifestSerializationError("evidence source does not match artifact")
        if provenance.ingestion_run_id != artifact.ingestion_run_id:
            raise ManifestSerializationError("evidence run does not match artifact")
        if provenance.contract_version != artifact.contract_version:
            raise ManifestSerializationError(
                "evidence contract version does not match artifact"
            )
        if provenance.outcome is not evidence.result.outcome:
            raise ManifestSerializationError(
                "provenance outcome does not match contract result"
            )

    def _put(
        self,
        key: str,
        data: BinaryIO,
        length: int,
        content_type: str,
    ) -> None:
        try:
            self._client.put_object(
                self._bucket_name,
                key,
                data,
                length,
                content_type=content_type,
            )
        except Exception as exc:
            raise ObjectStoreError(f"could not write {self._uri(key)}") from exc

    def _uri(self, key: str) -> str:
        return f"s3://{self._bucket_name}/{key}"


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
```

- [ ] **Step 4: Add failure and immutability tests**

Append these tests to `tests/infrastructure/test_minio_bronze.py`:

```python
def test_rejects_missing_artifact(tmp_path: Path) -> None:
    store = MinioBronzeArtifactStore(FakeMinioClient(), "bronze", "raw")
    with pytest.raises(ArtifactNotFoundError):
        store.store_bundle(make_artifact(tmp_path / "missing.csv"), ())


def test_rejects_checksum_mismatch_before_upload(tmp_path: Path) -> None:
    path = tmp_path / "met.csv"
    path.write_bytes(b"actual")
    client = FakeMinioClient()
    store = MinioBronzeArtifactStore(client, "bronze", "raw")
    with pytest.raises(ArtifactIntegrityError):
        store.store_bundle(make_artifact(path, "0" * 64), ())
    assert client.upload_order == []


@pytest.mark.parametrize(
    "existing_suffix",
    ("source.csv", "records.manifest.jsonl", "_SUCCESS.json"),
)
def test_rejects_existing_key_before_upload(
    tmp_path: Path,
    existing_suffix: str,
) -> None:
    path = tmp_path / "met.csv"
    path.write_bytes(b"source")
    client = FakeMinioClient()
    store = MinioBronzeArtifactStore(client, "bronze", "raw")
    artifact = make_artifact(path)
    existing_key = (
        "raw/met/year=2026/month=07/day=27/"
        f"run={RUN_ID}/{existing_suffix}"
    )
    client.objects[existing_key] = b"existing"

    with pytest.raises(ObjectAlreadyExistsError):
        store.store_bundle(artifact, ())
    assert client.upload_order == []


@pytest.mark.parametrize(
    "failed_suffix",
    ("source.csv", "records.manifest.jsonl", "_SUCCESS.json"),
)
def test_does_not_write_success_marker_after_upload_failure(
    tmp_path: Path,
    failed_suffix: str,
) -> None:
    path = tmp_path / "met.csv"
    path.write_bytes(b"source")
    client = FakeMinioClient()
    failed_key = (
        "raw/met/year=2026/month=07/day=27/"
        f"run={RUN_ID}/{failed_suffix}"
    )
    client.fail_on = failed_key
    store = MinioBronzeArtifactStore(client, "bronze", "raw")

    with pytest.raises(ObjectStoreError):
        store.store_bundle(
            make_artifact(path),
            (make_evidence(ContractOutcome.ACCEPTED, "42"),),
        )

    assert not any(key.endswith("/_SUCCESS.json") for key in client.objects)


def test_translates_evidence_iteration_failure(tmp_path: Path) -> None:
    path = tmp_path / "met.csv"
    path.write_bytes(b"source")
    client = FakeMinioClient()
    store = MinioBronzeArtifactStore(client, "bronze", "raw")

    def broken_evidence() -> Iterable[BronzeRecordEvidence]:
        yield make_evidence(ContractOutcome.ACCEPTED, "42")
        raise RuntimeError("synthetic iterator failure")

    with pytest.raises(ManifestSerializationError):
        store.store_bundle(make_artifact(path), broken_evidence())

    assert not any(key.endswith("/_SUCCESS.json") for key in client.objects)


def test_rejects_incoherent_evidence(tmp_path: Path) -> None:
    path = tmp_path / "met.csv"
    path.write_bytes(b"source")
    client = FakeMinioClient()
    store = MinioBronzeArtifactStore(client, "bronze", "raw")
    evidence = make_evidence(ContractOutcome.ACCEPTED, "42").model_copy(
        update={
            "result": ContractResult(outcome=ContractOutcome.REJECTED),
        }
    )

    with pytest.raises(ManifestSerializationError):
        store.store_bundle(make_artifact(path), (evidence,))

    assert not any(key.endswith("/_SUCCESS.json") for key in client.objects)
```

- [ ] **Step 5: Run adapter tests and fix only concrete failures**

Run: `.venv/bin/pytest tests/infrastructure/test_minio_bronze.py -q`

Expected: PASS.

Run: `.venv/bin/pytest tests/infrastructure -q`

Expected: PASS.

Run: `.venv/bin/ruff check src/evidence_cartographer/infrastructure tests/infrastructure`

Expected: PASS.

Run: `.venv/bin/ruff format --check src/evidence_cartographer/infrastructure tests/infrastructure`

Expected: PASS.

Run: `.venv/bin/mypy src/evidence_cartographer/infrastructure`

Expected: PASS.

- [ ] **Step 6: Commit the MinIO adapter**

```bash
git add src/evidence_cartographer/infrastructure/minio_bronze.py tests/infrastructure/test_minio_bronze.py
git commit -m "feat: store immutable Bronze bundles in MinIO"
```

---

### Task 4: Remove the repository environment template and document local setup

**Files:**
- Delete: `.env.example`
- Modify: `README.md`
- Modify: `tests/config/test_manifests.py`

**Interfaces:**
- Preserves: local ignored `.env`
- Removes: tracked `.env.example`
- Documents: required environment variable names without values

- [ ] **Step 1: Replace environment-template tests with policy tests**

In `tests/config/test_manifests.py`, delete
`test_env_example_documents_complete_nested_configuration` and
`test_required_credentials_remain_blank_in_env_example`. Add:

```python
import subprocess


def test_repository_has_no_environment_template() -> None:
    assert not Path(".env.example").exists()


def test_real_environment_file_is_ignored_and_untracked() -> None:
    ignored = subprocess.run(
        ["git", "check-ignore", "--quiet", ".env"],
        check=False,
    )
    tracked = subprocess.run(
        ["git", "ls-files", "--error-unmatch", ".env"],
        check=False,
        capture_output=True,
    )
    assert ignored.returncode == 0
    assert tracked.returncode != 0


def test_readme_documents_required_environment_names_without_values() -> None:
    readme = Path("README.md").read_text()
    required = (
        "EC_POSTGRES__PASSWORD",
        "EC_OBJECT_STORE__ACCESS_KEY",
        "EC_OBJECT_STORE__SECRET_KEY",
    )
    assert all(name in readme for name in required)
    assert ".env.example" not in readme
```

- [ ] **Step 2: Run policy tests and verify the current documentation failure**

Run: `.venv/bin/pytest tests/config/test_manifests.py -q`

Expected: FAIL because README still instructs users to copy `.env.example`.

- [ ] **Step 3: Remove the template and update README**

Delete `.env.example` with `apply_patch`.

Replace the README setup section with:

````markdown
## Setup

```bash
uv sync --all-extras
```

Create a local `.env` file. It is ignored by Git and must never be committed.
Fill these three required values before starting local services:

- `EC_POSTGRES__PASSWORD`
- `EC_OBJECT_STORE__ACCESS_KEY`
- `EC_OBJECT_STORE__SECRET_KEY`

Additional optional settings are defined by
`src/evidence_cartographer/infrastructure/settings.py`.

```bash
docker compose --env-file .env -f infra/compose.yaml up -d
```
````

Do not read or modify the existing local `.env`.

- [ ] **Step 4: Run environment policy and full verification**

Run: `.venv/bin/pytest tests/config/test_manifests.py -q`

Expected: PASS.

Run: `.venv/bin/pytest`

Expected: all tests PASS.

Run: `.venv/bin/ruff check .`

Expected: PASS.

Run: `.venv/bin/ruff format --check .`

Expected: PASS.

Run: `.venv/bin/mypy src`

Expected: PASS.

Run:

```bash
mkdir -p /tmp/evidence-cartographer-dbt-profile
cp dbt/profiles.yml.example /tmp/evidence-cartographer-dbt-profile/profiles.yml
.venv/bin/dbt parse --project-dir dbt --profiles-dir /tmp/evidence-cartographer-dbt-profile --quiet
```

Expected: exit code 0.

Run: `docker compose --env-file .env -f infra/compose.yaml config --quiet`

Expected: exit code 0 without displaying `.env` contents.

Run: `git status --short`

Expected: `.env` is absent from status, `.env.example` is deleted, and only
intentional task files are present.

- [ ] **Step 5: Commit environment policy and documentation**

```bash
git add .env.example README.md tests/config/test_manifests.py
git commit -m "docs: keep environment configuration local"
```

---

## Plan Self-Review

- **Spec coverage:** Tasks cover application types, replacement of the old
  Bronze port, typed errors, deterministic keys, streaming original bytes,
  bounded NDJSON spooling, checksums, evidence preservation, outcome counts,
  completion-last semantics, overwrite rejection, partial-failure behavior,
  fake-client tests, and repository environment policy.
- **Scope control:** No task downloads, parses, normalizes, compresses, creates
  buckets, starts services, or adds a live integration test.
- **Type consistency:** `BronzeArtifact`, `BronzeRecordEvidence`,
  `BronzeBundleReceipt`, `BronzeCompletionManifest`, and `StoredObject` are
  defined in Task 1 and consumed with identical names in Tasks 2 and 3.
- **Memory behavior:** The artifact is read in chunks and reopened for upload;
  evidence is iterated once into a bounded `SpooledTemporaryFile`.
- **Immutability:** All three target keys are checked before the first upload,
  and `_SUCCESS.json` is always written last.
