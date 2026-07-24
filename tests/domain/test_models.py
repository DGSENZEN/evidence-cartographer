from datetime import UTC, date, datetime, timedelta
from uuid import uuid4

import pytest
from pydantic import ValidationError

from evidence_cartographer.domain.enums import (
    ContractOutcome,
    IngestionMode,
    RetrievalStatus,
    RunStatus,
    SourceName,
)
from evidence_cartographer.domain.models import (
    AcquisitionContext,
    Classification,
    Culture,
    DataQualityResult,
    Department,
    Image,
    IngestionRun,
    Museum,
    Object,
    ObjectImage,
    ObjectPersonRole,
    Person,
    Place,
    QualityCheck,
    SCD2Period,
    SourceRecord,
)


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


def test_all_thirteen_canonical_entities_accept_synthetic_data() -> None:
    now = datetime.now(UTC)
    museum = Museum(created_at=now, name="Synthetic Museum", source=SourceName.MET)
    department = Department(
        created_at=now,
        museum_id=museum.id,
        name="Synthetic Department",
    )
    classification = Classification(
        created_at=now,
        museum_id=museum.id,
        name="Synthetic Classification",
    )
    culture = Culture(created_at=now, name="Synthetic Culture")
    place = Place(
        created_at=now,
        name="Synthetic Place",
        latitude=41.88,
        longitude=-87.63,
    )
    source_record = SourceRecord(
        created_at=now,
        museum_id=museum.id,
        source=SourceName.MET,
        source_record_id="synthetic-42",
        contract_version="1.0.0",
        ingestion_run_id=uuid4(),
        observed_at=now,
        source_url="https://example.test/objects/42",
        retrieval_status=RetrievalStatus.CACHED,
        retrieved_at=now,
        raw_uri="s3://bronze/raw/synthetic-42.json",
        acquisition_context=AcquisitionContext(mode=IngestionMode.FULL_SNAPSHOT),
        outcome=ContractOutcome.ACCEPTED,
    )
    object_ = Object(
        created_at=now,
        museum_id=museum.id,
        source_record_id=source_record.id,
        accession_number="SYN-42",
        title="Synthetic Work",
        department_id=department.id,
        classification_id=classification.id,
        culture_id=culture.id,
        place_id=place.id,
    )
    person = Person(
        created_at=now,
        display_name="Synthetic Artist",
        normalized_name="synthetic artist",
        birth_date=date(1900, 1, 1),
        death_date=date(1980, 1, 1),
        authority_ids={"ulan": "500000001"},
    )
    image = Image(
        created_at=now,
        museum_id=museum.id,
        source_url="https://example.test/images/42.jpg",
        retrieval_status=RetrievalStatus.CACHED,
        cached_uri="s3://image-cache/objects/42.jpg",
    )
    object_person_role = ObjectPersonRole(
        created_at=now,
        object_id=object_.id,
        person_id=person.id,
        role="artist",
        attribution_text="Synthetic Artist",
    )
    object_image = ObjectImage(
        created_at=now,
        object_id=object_.id,
        image_id=image.id,
        is_primary=True,
        sequence=0,
    )
    ingestion_run = IngestionRun(
        created_at=now,
        source=SourceName.MET,
        mode=IngestionMode.FULL_SNAPSHOT,
        status=RunStatus.RUNNING,
        started_at=now,
    )
    quality_result = DataQualityResult(
        created_at=now,
        source_record_id=source_record.id,
        score=1.0,
        checks=(QualityCheck(rule_id="synthetic", passed=True),),
        evaluated_at=now,
    )

    entities = (
        museum,
        object_,
        person,
        image,
        department,
        classification,
        culture,
        place,
        object_person_role,
        object_image,
        source_record,
        ingestion_run,
        quality_result,
    )
    assert len(entities) == 13
    assert all(entity.created_at == now for entity in entities)


@pytest.mark.parametrize(
    ("valid_from", "valid_to", "is_current"),
    [
        (
            datetime(2026, 7, 24, tzinfo=UTC),
            datetime(2026, 7, 23, tzinfo=UTC),
            False,
        ),
        (
            datetime(2026, 7, 24, tzinfo=UTC),
            datetime(2026, 7, 25, tzinfo=UTC),
            True,
        ),
        (datetime(2026, 7, 24, tzinfo=UTC), None, False),
    ],
)
def test_scd2_period_rejects_contradictory_state(
    valid_from: datetime,
    valid_to: datetime | None,
    is_current: bool,
) -> None:
    with pytest.raises(ValidationError):
        SCD2Period(
            valid_from=valid_from,
            valid_to=valid_to,
            is_current=is_current,
        )


@pytest.mark.parametrize(
    ("retrieval_status", "cached_uri"),
    [
        (RetrievalStatus.CACHED, None),
        (RetrievalStatus.AVAILABLE, "s3://image-cache/objects/42.jpg"),
    ],
)
def test_image_rejects_contradictory_cache_state(
    retrieval_status: RetrievalStatus,
    cached_uri: str | None,
) -> None:
    with pytest.raises(ValidationError):
        Image(
            created_at=datetime.now(UTC),
            museum_id=uuid4(),
            source_url="https://example.test/images/42.jpg",
            retrieval_status=retrieval_status,
            cached_uri=cached_uri,
        )


@pytest.mark.parametrize(
    ("status", "ended_at_offset"),
    [
        (RunStatus.SUCCEEDED, None),
        (RunStatus.FAILED, None),
        (RunStatus.PENDING, timedelta(seconds=1)),
        (RunStatus.RUNNING, timedelta(seconds=1)),
        (RunStatus.SUCCEEDED, timedelta(seconds=-1)),
    ],
)
def test_ingestion_run_rejects_contradictory_lifecycle_state(
    status: RunStatus,
    ended_at_offset: timedelta | None,
) -> None:
    started_at = datetime.now(UTC)
    ended_at = None if ended_at_offset is None else started_at + ended_at_offset
    with pytest.raises(ValidationError):
        IngestionRun(
            created_at=started_at,
            source=SourceName.AIC,
            mode=IngestionMode.INCREMENTAL,
            status=status,
            started_at=started_at,
            ended_at=ended_at,
        )
