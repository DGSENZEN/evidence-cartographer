import csv
from collections.abc import Iterator
from pathlib import Path
from uuid import UUID

import pandera.polars as pa
import polars as pl
from pandera.errors import SchemaError, SchemaErrors
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from evidence_cartographer.application.bronze import BronzeRecordEvidence
from evidence_cartographer.application.contracts import (
    ContractResult,
    ValidationMessage,
)
from evidence_cartographer.application.errors import MetCsvParseError
from evidence_cartographer.application.ports import RawRecord
from evidence_cartographer.domain.enums import (
    ContractOutcome,
    IngestionMode,
    RetrievalStatus,
    SourceName,
)
from evidence_cartographer.domain.models import AcquisitionContext, SourceRecord
from evidence_cartographer.sources.met.contract import (
    MET_V1_REQUIRED_COLUMNS,
    MetCsvPreflightResult,
    MetRowContract,
    parse_positive_object_id,
)

SOURCE_ROW_COLUMN = "__source_row_number"


class MetEvidenceContext(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    museum_id: UUID
    ingestion_run_id: UUID
    observed_at: AwareDatetime
    retrieved_at: AwareDatetime
    source_url: str
    raw_uri: str
    raw_checksum: str = Field(pattern=r"^[0-9a-f]{64}$")
    contract_version: str


class MetCsvEvidenceReader:
    def __init__(self, *, batch_size: int) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be at least 1")
        self._batch_size = batch_size
        self._contract = MetRowContract()

    def iter_evidence(
        self,
        path: Path,
        preflight: MetCsvPreflightResult,
        context: MetEvidenceContext,
    ) -> Iterator[BronzeRecordEvidence]:
        seen_object_ids: set[int] = set()
        schema = pa.DataFrameSchema(
            {
                column: pa.Column(str, nullable=True)
                for column in MET_V1_REQUIRED_COLUMNS
            },
            strict=False,
        )
        try:
            batches = pl.scan_csv(
                path,
                schema_overrides={column: pl.String for column in preflight.columns},
                infer_schema_length=0,
                encoding="utf8",
                empty_string_is_null=True,
                row_index_name=SOURCE_ROW_COLUMN,
                row_index_offset=2,
            ).collect_batches(chunk_size=self._batch_size)
            for batch in batches:
                validated = schema.validate(batch)
                for row in validated.iter_rows(named=True):
                    payload = dict(row)
                    source_row_number = payload.pop(SOURCE_ROW_COLUMN)
                    if not isinstance(source_row_number, int):
                        raise MetCsvParseError(
                            "Polars emitted a noninteger source row number"
                        )
                    yield self._evidence_for_row(
                        payload,
                        source_row_number,
                        seen_object_ids,
                        context,
                    )
        except MetCsvParseError:
            raise
        except (
            csv.Error,
            OSError,
            UnicodeError,
            pl.exceptions.PolarsError,
            SchemaError,
            SchemaErrors,
        ) as exc:
            raise MetCsvParseError(f"could not parse Met CSV records: {exc}") from exc

    def _evidence_for_row(
        self,
        payload: dict[str, object],
        source_row_number: int,
        seen_object_ids: set[int],
        context: MetEvidenceContext,
    ) -> BronzeRecordEvidence:
        object_id = parse_positive_object_id(payload.get("Object ID"))
        source_record_id = (
            str(object_id)
            if object_id is not None
            else f"{context.ingestion_run_id}:row:{source_row_number}"
        )
        record = RawRecord(
            source_record_id=source_record_id,
            source_row_number=source_row_number,
            payload=payload,
        )
        if object_id is not None and object_id in seen_object_ids:
            result = ContractResult(
                outcome=ContractOutcome.REJECTED,
                messages=(
                    ValidationMessage(
                        rule_id="duplicate_object_id",
                        field="Object ID",
                        message=(f"Object ID {object_id} repeats within this snapshot"),
                    ),
                ),
            )
        else:
            if object_id is not None:
                seen_object_ids.add(object_id)
            result = self._contract.validate(record)

        artist = payload.get("Artist Display Name")
        provenance = SourceRecord(
            created_at=context.observed_at,
            museum_id=context.museum_id,
            source=SourceName.MET,
            source_record_id=source_record_id,
            contract_version=context.contract_version,
            ingestion_run_id=context.ingestion_run_id,
            observed_at=context.observed_at,
            source_url=context.source_url,
            retrieval_status=RetrievalStatus.CACHED,
            retrieved_at=context.retrieved_at,
            raw_uri=context.raw_uri,
            acquisition_context=AcquisitionContext(
                mode=IngestionMode.FULL_SNAPSHOT,
            ),
            raw_checksum=context.raw_checksum,
            attribution_text=artist if isinstance(artist, str) and artist else None,
            outcome=result.outcome,
        )
        return BronzeRecordEvidence(
            provenance=provenance,
            result=result,
        )
