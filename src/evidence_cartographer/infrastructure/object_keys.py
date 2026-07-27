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
