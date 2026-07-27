import hashlib
import json
from collections.abc import Iterable
from io import BytesIO
from tempfile import SpooledTemporaryFile
from typing import BinaryIO, Protocol, cast

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
                self._put(
                    key,
                    cast(BinaryIO, spool),
                    size_bytes,
                    "application/x-ndjson",
                )
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
