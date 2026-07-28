import hashlib
import json
from collections.abc import Iterable
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path
from typing import Any, BinaryIO
from uuid import UUID, uuid4

import pytest
from minio import Minio
from minio.datatypes import Object
from minio.error import S3Error
from urllib3 import HTTPHeaderDict

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
    ArtifactStagingError,
    EvidenceStagingError,
    ManifestSerializationError,
    ObjectAlreadyExistsError,
    ObjectStoreError,
    SinglePutSizeLimitError,
)
from evidence_cartographer.domain.enums import (
    ContractOutcome,
    IngestionMode,
    RetrievalStatus,
    SourceName,
)
from evidence_cartographer.domain.models import AcquisitionContext, SourceRecord
from evidence_cartographer.infrastructure.conditional_object_client import (
    ConditionalObjectExistsError,
    ConditionalObjectMetadata,
    ConditionalPutDiagnostic,
    MinioConditionalObjectClient,
)
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
        self.create_before_put: str | None = None
        self.mutate_path_on_stat: tuple[Path, bytes] | None = None
        self.streams: dict[str, BinaryIO] = {}
        self.sha256_metadata: dict[str, str] = {}

    def stat_object(
        self,
        bucket_name: str,
        object_name: str,
    ) -> ConditionalObjectMetadata:
        if self.mutate_path_on_stat is not None:
            path, replacement = self.mutate_path_on_stat
            path.write_bytes(replacement)
            self.mutate_path_on_stat = None
        if object_name not in self.objects:
            raise missing_object_error(bucket_name, object_name)
        payload = self.objects[object_name]
        return ConditionalObjectMetadata(
            size_bytes=len(payload),
            sha256=self.sha256_metadata.get(object_name),
            declared_size_bytes=len(payload),
        )

    def put_object_if_absent(
        self,
        bucket_name: str,
        object_name: str,
        data: BinaryIO,
        length: int,
        sha256: str,
        content_type: str = "application/octet-stream",
    ) -> None:
        self._create_racing_object(object_name)
        if self.fail_on == object_name:
            raise RuntimeError("synthetic MinIO failure")
        if object_name in self.objects:
            raise ConditionalObjectExistsError(
                ConditionalPutDiagnostic(
                    status_code=412,
                    code="PreconditionFailed",
                    message="synthetic object exists",
                )
            )
        self._store(object_name, data, length, sha256, content_type)

    def _create_racing_object(self, object_name: str) -> None:
        if self.create_before_put == object_name:
            self.objects[object_name] = b"written by another producer"
            self.create_before_put = None

    def _store(
        self,
        object_name: str,
        data: BinaryIO,
        length: int,
        sha256: str,
        content_type: str,
    ) -> None:
        self.streams[object_name] = data
        payload = data.read(length)
        assert len(payload) == length
        self.objects[object_name] = payload
        self.sha256_metadata[object_name] = sha256
        self.content_types[object_name] = content_type
        self.upload_order.append(object_name)


class MissingObjectMinio(Minio):
    def stat_object(self, bucket_name: str, object_name: str) -> object:
        raise missing_object_error(bucket_name, object_name)


class MatchingAfterPreflightMinio(Minio):
    def __init__(
        self,
        *args: Any,
        expected_size: int,
        expected_sha256: str,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.expected_size = expected_size
        self.expected_sha256 = expected_sha256
        self.stat_calls = 0

    def stat_object(self, bucket_name: str, object_name: str) -> Object:
        self.stat_calls += 1
        if self.stat_calls <= 3:
            raise missing_object_error(bucket_name, object_name)
        return Object(
            bucket_name=bucket_name,
            object_name=object_name,
            size=self.expected_size,
            metadata={
                "x-amz-meta-ec-sha256": self.expected_sha256,
                "x-amz-meta-ec-size": str(self.expected_size),
            },
        )


class FakeHttpResponse:
    def __init__(
        self,
        status: int,
        *,
        data: bytes = b"",
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status = status
        self.data = data
        self.headers = HTTPHeaderDict(headers or {})


class RecordingHttpClient:
    def __init__(self, *responses: FakeHttpResponse) -> None:
        self.responses = list(responses)
        self.requests: list[tuple[str, str, bytes, dict[str, str]]] = []

    def urlopen(self, method: str, url: str, **kwargs: Any) -> object:
        body = kwargs["body"]
        headers = kwargs["headers"]
        self.requests.append((method, url, body.read(), dict(headers)))
        if self.responses:
            return self.responses.pop(0)
        return FakeHttpResponse(200)

    def clear(self) -> None:
        return None


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
    assert (
        completion["artifact"]["sha256"] == hashlib.sha256(artifact_bytes).hexdigest()
    )
    assert (
        completion["evidence_manifest"]["sha256"]
        == hashlib.sha256(evidence_payload).hexdigest()
    )
    assert receipt.artifact.size_bytes == len(artifact_bytes)
    assert receipt.evidence_manifest.size_bytes == len(evidence_payload)
    assert receipt.completion_manifest_uri == f"s3://bronze/{success_key}"
    assert client.sha256_metadata == {
        artifact_key: hashlib.sha256(artifact_bytes).hexdigest(),
        evidence_key: hashlib.sha256(evidence_payload).hexdigest(),
        success_key: hashlib.sha256(client.objects[success_key]).hexdigest(),
    }


def test_target_for_matches_uploaded_bundle_locations(tmp_path: Path) -> None:
    path = tmp_path / "met.csv"
    path.write_bytes(b"source")
    client = FakeMinioClient()
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
    client = FakeMinioClient()
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


def test_translates_artifact_read_failure_to_typed_staging_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "met.csv"
    path.write_bytes(b"source")
    real_open = Path.open

    def failing_open(
        candidate: Path,
        *args: Any,
        **kwargs: Any,
    ) -> BinaryIO:
        if candidate == path:
            raise OSError("synthetic artifact read failure")
        return real_open(candidate, *args, **kwargs)

    monkeypatch.setattr(Path, "open", failing_open)
    store = MinioBronzeArtifactStore(FakeMinioClient(), "bronze", "raw")

    with pytest.raises(ArtifactStagingError) as raised:
        store.store_bundle(make_artifact(path), ())

    assert isinstance(raised.value.__cause__, OSError)


def test_configurable_spool_directory_reports_artifact_disk_failure(
    tmp_path: Path,
) -> None:
    path = tmp_path / "met.csv"
    path.write_bytes(b"source")
    client = FakeMinioClient()
    store = MinioBronzeArtifactStore(
        client,
        "bronze",
        "raw",
        spool_max_memory_bytes=1,
        spool_directory=tmp_path / "missing",
    )

    with pytest.raises(ArtifactStagingError):
        store.store_bundle(make_artifact(path), ())

    assert client.upload_order == []


def test_configurable_spool_directory_reports_evidence_disk_failure(
    tmp_path: Path,
) -> None:
    path = tmp_path / "met.csv"
    path.write_bytes(b"")
    client = FakeMinioClient()
    store = MinioBronzeArtifactStore(
        client,
        "bronze",
        "raw",
        spool_max_memory_bytes=1,
        spool_directory=tmp_path / "missing",
    )

    with pytest.raises(EvidenceStagingError):
        store.store_bundle(
            make_artifact(path),
            (make_evidence(ContractOutcome.ACCEPTED, "42"),),
        )

    assert client.upload_order == [
        f"raw/met/year=2026/month=07/day=27/run={RUN_ID}/source.csv"
    ]


@pytest.mark.parametrize(
    ("artifact_bytes", "evidence", "expected_uploads"),
    [
        (b"123456", (), 0),
        (b"", (make_evidence(ContractOutcome.ACCEPTED, "42"),), 1),
        (b"", (), 2),
    ],
    ids=("artifact", "evidence", "completion"),
)
def test_applies_single_put_ceiling_to_every_bundle_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    artifact_bytes: bytes,
    evidence: tuple[BronzeRecordEvidence, ...],
    expected_uploads: int,
) -> None:
    path = tmp_path / "met.csv"
    path.write_bytes(artifact_bytes)
    client = FakeMinioClient()
    store = MinioBronzeArtifactStore(client, "bronze", "raw")
    monkeypatch.setattr(
        "evidence_cartographer.infrastructure.minio_bronze.MAX_SINGLE_PUT_SIZE_BYTES",
        5,
    )

    with pytest.raises(SinglePutSizeLimitError) as raised:
        store.store_bundle(make_artifact(path), evidence)

    assert raised.value.max_size_bytes == 5
    assert len(client.upload_order) == expected_uploads
    assert not any(key.endswith("/_SUCCESS.json") for key in client.objects)


def test_stops_reading_artifact_when_single_put_ceiling_is_crossed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class CountingSource(BytesIO):
        read_calls = 0

        def read(self, size: int = -1) -> bytes:
            self.read_calls += 1
            return super().read(size)

    path = tmp_path / "met.csv"
    path.write_bytes(b"real path only establishes a regular file")
    source = CountingSource(b"0123456789")
    real_open = Path.open

    def tracked_open(
        candidate: Path,
        *args: Any,
        **kwargs: Any,
    ) -> BinaryIO:
        if candidate == path:
            return source
        return real_open(candidate, *args, **kwargs)

    monkeypatch.setattr(Path, "open", tracked_open)
    monkeypatch.setattr(
        "evidence_cartographer.infrastructure.minio_bronze.HASH_CHUNK_SIZE",
        4,
    )
    monkeypatch.setattr(
        "evidence_cartographer.infrastructure.minio_bronze.MAX_SINGLE_PUT_SIZE_BYTES",
        5,
    )
    store = MinioBronzeArtifactStore(FakeMinioClient(), "bronze", "raw")

    with pytest.raises(SinglePutSizeLimitError):
        store.store_bundle(make_artifact(path), ())

    assert source.read_calls == 2


def test_rejects_racing_writer_without_overwriting_its_artifact(
    tmp_path: Path,
) -> None:
    path = tmp_path / "met.csv"
    path.write_bytes(b"source")
    client = FakeMinioClient()
    artifact_key = f"raw/met/year=2026/month=07/day=27/run={RUN_ID}/source.csv"
    client.create_before_put = artifact_key
    store = MinioBronzeArtifactStore(client, "bronze", "raw")

    with pytest.raises(ObjectAlreadyExistsError):
        store.store_bundle(make_artifact(path), ())

    assert client.objects[artifact_key] == b"written by another producer"
    assert client.upload_order == []


def test_uploads_the_hashed_artifact_snapshot_when_local_file_mutates(
    tmp_path: Path,
) -> None:
    original_bytes = b"original"
    replacement_bytes = b"replaced"
    path = tmp_path / "met.csv"
    path.write_bytes(original_bytes)
    client = FakeMinioClient()
    client.mutate_path_on_stat = (path, replacement_bytes)
    store = MinioBronzeArtifactStore(client, "bronze", "raw")

    receipt = store.store_bundle(make_artifact(path), ())

    assert path.read_bytes() == replacement_bytes
    assert client.objects[receipt.artifact.uri.removeprefix("s3://bronze/")] == (
        original_bytes
    )
    assert receipt.artifact.sha256 == hashlib.sha256(original_bytes).hexdigest()


def test_closes_artifact_spool_before_evidence_staging(tmp_path: Path) -> None:
    path = tmp_path / "met.csv"
    path.write_bytes(b"source")
    artifact_key = f"raw/met/year=2026/month=07/day=27/run={RUN_ID}/source.csv"
    client = FakeMinioClient()
    store = MinioBronzeArtifactStore(client, "bronze", "raw")
    observed_closed_state: list[bool] = []

    def evidence() -> Iterable[BronzeRecordEvidence]:
        observed_closed_state.append(client.streams[artifact_key].closed)
        yield make_evidence(ContractOutcome.ACCEPTED, "42")

    store.store_bundle(make_artifact(path), evidence())

    assert observed_closed_state == [True]


def test_real_minio_client_uses_streaming_conditional_puts(tmp_path: Path) -> None:
    artifact_bytes = b"source"
    path = tmp_path / "met.csv"
    path.write_bytes(artifact_bytes)
    http_client = RecordingHttpClient()
    client = MissingObjectMinio(
        "minio.test",
        access_key="access",
        secret_key="secret",
        region="us-east-1",
    )
    client._http = http_client  # type: ignore[assignment]
    store = MinioBronzeArtifactStore(
        MinioConditionalObjectClient(client),
        "bronze",
        "raw",
    )

    receipt = store.store_bundle(
        make_artifact(path),
        (make_evidence(ContractOutcome.ACCEPTED, "42"),),
    )

    assert [request[0] for request in http_client.requests] == ["PUT", "PUT", "PUT"]
    assert all(request[3]["If-None-Match"] == "*" for request in http_client.requests)
    assert http_client.requests[0][2] == artifact_bytes
    assert receipt.artifact.sha256 == hashlib.sha256(artifact_bytes).hexdigest()


def test_first_matching_412_is_store_collision(tmp_path: Path) -> None:
    artifact_bytes = b"source"
    artifact_sha256 = hashlib.sha256(artifact_bytes).hexdigest()
    path = tmp_path / "met.csv"
    path.write_bytes(artifact_bytes)
    precondition_failed = FakeHttpResponse(
        412,
        data=(
            b"<Error><Code>PreconditionFailed</Code>"
            b"<Message>object exists</Message></Error>"
        ),
        headers={"content-type": "application/xml"},
    )
    http_client = RecordingHttpClient(precondition_failed)
    client = MatchingAfterPreflightMinio(
        "minio.test",
        access_key="access",
        secret_key="secret",
        region="us-east-1",
        expected_size=len(artifact_bytes),
        expected_sha256=artifact_sha256,
    )
    client._http = http_client  # type: ignore[assignment]
    store = MinioBronzeArtifactStore(
        MinioConditionalObjectClient(client),
        "bronze",
        "raw",
    )

    with pytest.raises(ObjectAlreadyExistsError) as raised:
        store.store_bundle(make_artifact(path), ())

    assert isinstance(raised.value.__cause__, ConditionalObjectExistsError)
    assert client.stat_calls == 3
    assert len(http_client.requests) == 1


def test_preserves_evidence_create_collision_as_object_exists_error(
    tmp_path: Path,
) -> None:
    path = tmp_path / "met.csv"
    path.write_bytes(b"source")
    client = FakeMinioClient()
    evidence_key = (
        f"raw/met/year=2026/month=07/day=27/run={RUN_ID}/records.manifest.jsonl"
    )
    client.create_before_put = evidence_key
    store = MinioBronzeArtifactStore(client, "bronze", "raw")

    with pytest.raises(ObjectAlreadyExistsError):
        store.store_bundle(
            make_artifact(path),
            (make_evidence(ContractOutcome.ACCEPTED, "42"),),
        )

    assert client.upload_order == [
        f"raw/met/year=2026/month=07/day=27/run={RUN_ID}/source.csv"
    ]
    assert not any(key.endswith("/_SUCCESS.json") for key in client.objects)


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
    existing_key = f"raw/met/year=2026/month=07/day=27/run={RUN_ID}/{existing_suffix}"
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
    failed_key = f"raw/met/year=2026/month=07/day=27/run={RUN_ID}/{failed_suffix}"
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
