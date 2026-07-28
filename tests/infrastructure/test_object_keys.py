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
        "raw/met/year=2026/month=07/day=28/run=00000000-0000-0000-0000-000000000042"
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
