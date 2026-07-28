import hashlib
import json
from collections.abc import Iterable
from io import BytesIO
from pathlib import Path
from tempfile import SpooledTemporaryFile
from typing import BinaryIO, cast

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
    ArtifactStagingError,
    EvidenceStagingError,
    ManifestSerializationError,
    ObjectAlreadyExistsError,
    ObjectStoreError,
    SinglePutSizeLimitError,
)
from evidence_cartographer.domain.enums import ContractOutcome
from evidence_cartographer.infrastructure.conditional_object_client import (
    MAX_SINGLE_PUT_SIZE_BYTES,
    MISSING_OBJECT_CODES,
    ConditionalObjectClient,
    ConditionalObjectExistsError,
)
from evidence_cartographer.infrastructure.object_keys import (
    BronzeObjectKeys,
    build_bronze_object_keys,
)

HASH_CHUNK_SIZE = 1024 * 1024
DEFAULT_SPOOL_MAX_MEMORY_BYTES = 8 * 1024 * 1024


class MinioBronzeArtifactStore(BronzeArtifactStore):
    def __init__(
        self,
        client: ConditionalObjectClient,
        bucket_name: str,
        bronze_prefix: str,
        *,
        spool_max_memory_bytes: int = DEFAULT_SPOOL_MAX_MEMORY_BYTES,
        spool_directory: Path | None = None,
    ) -> None:
        if spool_max_memory_bytes < 1:
            raise ValueError("spool_max_memory_bytes must be at least 1")
        self._client = client
        self._bucket_name = bucket_name
        self._bronze_prefix = bronze_prefix
        self._spool_max_memory_bytes = spool_max_memory_bytes
        self._spool_directory = spool_directory

    def store_bundle(
        self,
        artifact: BronzeArtifact,
        evidence: Iterable[BronzeRecordEvidence],
    ) -> BronzeBundleReceipt:
        if not artifact.local_path.is_file():
            raise ArtifactNotFoundError(str(artifact.local_path))

        keys = build_bronze_object_keys(artifact, self._bronze_prefix)
        try:
            with SpooledTemporaryFile(
                max_size=self._spool_max_memory_bytes,
                mode="w+b",
                dir=(
                    str(self._spool_directory)
                    if self._spool_directory is not None
                    else None
                ),
            ) as snapshot:
                binary_snapshot = cast(BinaryIO, snapshot)
                size_bytes, sha256 = self._snapshot_artifact(
                    artifact,
                    binary_snapshot,
                    self._uri(keys.artifact),
                )
                self._assert_bundle_absent(keys)
                artifact_object = StoredObject(
                    uri=self._uri(keys.artifact),
                    size_bytes=size_bytes,
                    sha256=sha256,
                )

                snapshot.seek(0)
                self._put_if_absent(
                    keys.artifact,
                    binary_snapshot,
                    size_bytes,
                    sha256,
                    artifact.media_type,
                )
        except OSError as exc:
            raise ArtifactStagingError(
                f"could not stage acquisition artifact {artifact.local_path}"
            ) from exc

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
        self._put_if_absent(
            keys.completion_manifest,
            BytesIO(completion_bytes),
            len(completion_bytes),
            hashlib.sha256(completion_bytes).hexdigest(),
            "application/json",
        )
        return BronzeBundleReceipt(
            source=artifact.source,
            ingestion_run_id=artifact.ingestion_run_id,
            artifact=artifact_object,
            evidence_manifest=evidence_object,
            completion_manifest_uri=self._uri(keys.completion_manifest),
        )

    def _snapshot_artifact(
        self,
        artifact: BronzeArtifact,
        snapshot: BinaryIO,
        object_uri: str,
    ) -> tuple[int, str]:
        digest = hashlib.sha256()
        size_bytes = 0
        with artifact.local_path.open("rb") as source:
            while chunk := source.read(HASH_CHUNK_SIZE):
                size_bytes += len(chunk)
                if size_bytes > MAX_SINGLE_PUT_SIZE_BYTES:
                    raise SinglePutSizeLimitError(
                        object_uri,
                        size_bytes,
                        MAX_SINGLE_PUT_SIZE_BYTES,
                    )
                digest.update(chunk)
                snapshot.write(chunk)
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
            with SpooledTemporaryFile(
                max_size=self._spool_max_memory_bytes,
                mode="w+b",
                dir=(
                    str(self._spool_directory)
                    if self._spool_directory is not None
                    else None
                ),
            ) as spool:
                binary_spool = cast(BinaryIO, spool)
                for item in evidence:
                    self._validate_evidence(artifact, item)
                    line = _canonical_json(item.model_dump(mode="json")) + b"\n"
                    projected_size = binary_spool.tell() + len(line)
                    if projected_size > MAX_SINGLE_PUT_SIZE_BYTES:
                        raise SinglePutSizeLimitError(
                            self._uri(key),
                            projected_size,
                            MAX_SINGLE_PUT_SIZE_BYTES,
                        )
                    binary_spool.write(line)
                    digest.update(line)
                    counts[item.result.outcome] += 1
                    total_records += 1
                size_bytes = binary_spool.tell()
                binary_spool.seek(0)
                self._put_if_absent(
                    key,
                    binary_spool,
                    size_bytes,
                    digest.hexdigest(),
                    "application/x-ndjson",
                )
        except (
            ManifestSerializationError,
            ObjectAlreadyExistsError,
            ObjectStoreError,
            SinglePutSizeLimitError,
        ):
            raise
        except OSError as exc:
            raise EvidenceStagingError(
                "could not stage Bronze record evidence"
            ) from exc
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

    def _put_if_absent(
        self,
        key: str,
        data: BinaryIO,
        length: int,
        sha256: str,
        content_type: str,
    ) -> None:
        if length > MAX_SINGLE_PUT_SIZE_BYTES:
            raise SinglePutSizeLimitError(
                self._uri(key),
                length,
                MAX_SINGLE_PUT_SIZE_BYTES,
            )
        try:
            self._client.put_object_if_absent(
                self._bucket_name,
                key,
                data,
                length,
                sha256,
                content_type=content_type,
            )
        except SinglePutSizeLimitError:
            raise
        except ConditionalObjectExistsError as exc:
            raise ObjectAlreadyExistsError(f"{self._uri(key)}: {exc}") from exc
        except Exception as exc:
            raise ObjectStoreError(f"could not write {self._uri(key)}: {exc}") from exc

    def _uri(self, key: str) -> str:
        return f"s3://{self._bucket_name}/{key}"


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
