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
