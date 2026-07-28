from datetime import UTC, datetime
from inspect import Parameter, signature
from pathlib import Path
from typing import get_type_hints
from uuid import uuid4

import pytest
from pydantic import ValidationError

from evidence_cartographer.application.bronze import (
    BronzeArtifact,
    BronzeArtifactStore,
    BronzeBundleReceipt,
    BronzeBundleTarget,
    BronzeCompletionManifest,
    BronzeRecordEvidence,
    StoredObject,
)
from evidence_cartographer.application.contracts import (
    ContractResult,
    ValidationMessage,
)
from evidence_cartographer.domain.enums import ContractOutcome, SourceName
from evidence_cartographer.domain.models import SourceRecord


def test_bronze_artifact_normalizes_a_safe_extension() -> None:
    artifact = BronzeArtifact(
        source=SourceName.AIC,
        ingestion_run_id=uuid4(),
        retrieved_at=datetime.now(UTC),
        source_url="https://example.test/aic.json.gz",
        media_type="application/gzip",
        extension=".JSON.GZ",
        contract_version="1.0.0",
        local_path=Path("aic.json.gz"),
    )
    assert artifact.extension == "json.gz"


@pytest.mark.parametrize("extension", ("../json", "json/gz", "", "."))
def test_bronze_artifact_rejects_unsafe_extensions(extension: str) -> None:
    with pytest.raises(ValidationError):
        BronzeArtifact(
            source=SourceName.MET,
            ingestion_run_id=uuid4(),
            retrieved_at=datetime.now(UTC),
            source_url="https://example.test/met.csv",
            media_type="text/csv",
            extension=extension,
            contract_version="1.0.0",
            local_path=Path("met.csv"),
        )


def test_bronze_artifact_store_accepts_lazy_evidence() -> None:
    annotations = get_type_hints(BronzeArtifactStore.store_bundle)
    parameters = signature(BronzeArtifactStore.store_bundle).parameters

    assert annotations["artifact"] is BronzeArtifact
    assert parameters["evidence"].default is Parameter.empty
    assert annotations["return"] is BronzeBundleReceipt
    assert BronzeRecordEvidence.model_fields["provenance"].annotation is SourceRecord
    assert BronzeRecordEvidence.model_fields["result"].annotation is ContractResult


def test_bronze_store_resolves_target_before_evidence() -> None:
    target_hints = get_type_hints(BronzeArtifactStore.target_for)
    store_parameters = signature(BronzeArtifactStore.store_bundle).parameters

    assert target_hints["artifact"] is BronzeArtifact
    assert target_hints["return"] is BronzeBundleTarget
    assert store_parameters["contract_messages"].default == ()


def test_completion_manifest_defaults_to_no_contract_messages() -> None:
    now = datetime.now(UTC)
    stored = StoredObject(
        uri="s3://bronze/raw/met/source.csv",
        size_bytes=6,
        sha256="0" * 64,
    )
    manifest = BronzeCompletionManifest(
        source=SourceName.MET,
        ingestion_run_id=uuid4(),
        retrieved_at=now,
        source_url="https://example.test/met.csv",
        contract_version="1.0.0",
        artifact=stored,
        evidence_manifest=stored,
        total_records=0,
        outcome_counts=dict.fromkeys(ContractOutcome, 0),
    )
    assert manifest.contract_messages == ()
    assert ValidationMessage.model_fields["rule_id"].is_required()
