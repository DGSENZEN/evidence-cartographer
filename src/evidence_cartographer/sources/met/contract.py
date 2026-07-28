import csv
from datetime import datetime
from pathlib import Path

from pydantic import BaseModel, ConfigDict

from evidence_cartographer.application.contracts import (
    ContractResult,
    ValidationMessage,
)
from evidence_cartographer.application.errors import MetCsvSchemaError
from evidence_cartographer.application.ports import RawRecord
from evidence_cartographer.domain.enums import ContractOutcome

MET_V1_KNOWN_COLUMNS = frozenset(
    {
        "Object Number",
        "Is Highlight",
        "Is Timeline Work",
        "Is Public Domain",
        "Object ID",
        "Gallery Number",
        "Department",
        "AccessionYear",
        "Object Name",
        "Title",
        "Culture",
        "Period",
        "Dynasty",
        "Reign",
        "Portfolio",
        "Constituent ID",
        "Artist Role",
        "Artist Prefix",
        "Artist Display Name",
        "Artist Display Bio",
        "Artist Suffix",
        "Artist Alpha Sort",
        "Artist Nationality",
        "Artist Begin Date",
        "Artist End Date",
        "Artist Gender",
        "Artist ULAN URL",
        "Artist Wikidata URL",
        "Object Date",
        "Object Begin Date",
        "Object End Date",
        "Medium",
        "Dimensions",
        "Credit Line",
        "Geography Type",
        "City",
        "State",
        "County",
        "Country",
        "Region",
        "Subregion",
        "Locale",
        "Locus",
        "Excavation",
        "River",
        "Classification",
        "Rights and Reproduction",
        "Link Resource",
        "Object Wikidata URL",
        "Metadata Date",
        "Repository",
        "Tags",
        "Tags AAT URL",
        "Tags Wikidata URL",
    }
)

MET_V1_REQUIRED_COLUMNS = frozenset(
    {
        "Object ID",
        "Object Number",
        "Is Public Domain",
        "Department",
        "Object Name",
        "Title",
        "Artist Display Name",
        "Object Date",
        "Classification",
        "Link Resource",
        "Metadata Date",
        "Repository",
    }
)

RECOMMENDED_VALUE_FIELDS = (
    "Object Number",
    "Department",
    "Object Name",
    "Title",
    "Link Resource",
    "Metadata Date",
    "Repository",
)
PUBLIC_DOMAIN_VALUES = frozenset({"true", "false"})


class MetCsvPreflightResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    columns: tuple[str, ...]
    contract_messages: tuple[ValidationMessage, ...] = ()


class MetCsvPreflight:
    def inspect(self, path: Path) -> MetCsvPreflightResult:
        try:
            with path.open("r", encoding="utf-8-sig", newline="") as source:
                header = next(csv.reader(source))
        except StopIteration as exc:
            raise MetCsvSchemaError("Met CSV is empty") from exc
        except (csv.Error, OSError, UnicodeError) as exc:
            raise MetCsvSchemaError(f"could not read Met CSV header: {exc}") from exc

        if not header or any(not column for column in header):
            raise MetCsvSchemaError("Met CSV header contains a blank column name")
        if len(header) != len(set(header)):
            raise MetCsvSchemaError("Met CSV header contains duplicate column names")

        columns = tuple(header)
        missing = sorted(MET_V1_REQUIRED_COLUMNS.difference(columns))
        if missing:
            raise MetCsvSchemaError(
                f"Met CSV is missing required columns: {', '.join(missing)}"
            )

        unexpected = sorted(set(columns).difference(MET_V1_KNOWN_COLUMNS))
        messages: tuple[ValidationMessage, ...] = ()
        if unexpected:
            messages = (
                ValidationMessage(
                    rule_id="unexpected_columns",
                    message=f"Met CSV added columns: {', '.join(unexpected)}",
                ),
            )
        return MetCsvPreflightResult(
            columns=columns,
            contract_messages=messages,
        )


def parse_positive_object_id(value: object) -> int | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = int(value.strip())
    except ValueError:
        return None
    return parsed if parsed > 0 else None


class MetRowContract:
    def validate(self, record: RawRecord) -> ContractResult:
        if parse_positive_object_id(record.payload.get("Object ID")) is None:
            return ContractResult(
                outcome=ContractOutcome.QUARANTINED,
                messages=(
                    ValidationMessage(
                        rule_id="invalid_object_id",
                        field="Object ID",
                        message="Object ID must be a positive integer",
                    ),
                ),
            )

        messages: list[ValidationMessage] = []
        for field in RECOMMENDED_VALUE_FIELDS:
            value = record.payload.get(field)
            if not isinstance(value, str) or not value.strip():
                messages.append(
                    ValidationMessage(
                        rule_id="missing_recommended_value",
                        field=field,
                        message=f"{field} is blank",
                    )
                )

        public_domain = record.payload.get("Is Public Domain")
        if (
            not isinstance(public_domain, str)
            or public_domain.strip().lower() not in PUBLIC_DOMAIN_VALUES
        ):
            messages.append(
                ValidationMessage(
                    rule_id="invalid_public_domain_value",
                    field="Is Public Domain",
                    message="Is Public Domain must be True or False",
                )
            )

        metadata_date = record.payload.get("Metadata Date")
        if isinstance(metadata_date, str) and metadata_date.strip():
            try:
                datetime.fromisoformat(metadata_date.strip().replace("Z", "+00:00"))
            except ValueError:
                messages.append(
                    ValidationMessage(
                        rule_id="invalid_metadata_date",
                        field="Metadata Date",
                        message="Metadata Date must be an ISO-style date or timestamp",
                    )
                )

        return ContractResult(
            outcome=(
                ContractOutcome.ACCEPTED_WITH_WARNINGS
                if messages
                else ContractOutcome.ACCEPTED
            ),
            messages=tuple(messages),
        )
