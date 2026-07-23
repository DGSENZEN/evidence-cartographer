from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pydantic import ValidationError

from evidence_cartographer.domain.enums import ContractOutcome, SourceName
from evidence_cartographer.domain.models import SourceRecord


def test_contract_outcome_values_are_stable() -> None:
    assert {item.value for item in ContractOutcome} == {
        "accepted",
        "accepted_with_warnings",
        "quarantined",
        "rejected",
    }


def test_source_record_requires_payload_provenance() -> None:
    now = datetime.now(UTC)
    with pytest.raises(ValidationError):
        SourceRecord.model_validate(
            {
                "created_at": now,
                "museum_id": uuid4(),
                "source": SourceName.MET,
                "source_record_id": "42",
                "contract_version": "1.0.0",
                "ingestion_run_id": uuid4(),
                "observed_at": now,
                "outcome": ContractOutcome.ACCEPTED,
            }
        )
