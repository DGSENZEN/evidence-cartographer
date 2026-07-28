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
