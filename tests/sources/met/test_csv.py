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
