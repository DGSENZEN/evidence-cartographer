from evidence_cartographer.application.contracts import (
    ContractManifest,
    GoldEligibilitySignals,
)
from evidence_cartographer.domain.enums import ContractOutcome, SourceName


def test_contract_manifest_contains_all_outcomes() -> None:
    manifest = ContractManifest(
        source=SourceName.MET,
        version="1.0.0",
        formats=("csv", "json_api"),
        outcomes=tuple(ContractOutcome),
    )
    assert manifest.outcomes == tuple(ContractOutcome)


def test_gold_signals_do_not_embed_publication_policy() -> None:
    signals = GoldEligibilitySignals(
        rights_are_permissive=None,
        has_usable_image=False,
        metadata_quality_score=0.72,
    )
    assert signals.rights_are_permissive is None
    assert not hasattr(signals, "is_eligible")
