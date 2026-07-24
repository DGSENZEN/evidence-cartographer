from pathlib import Path

import yaml

EXPECTED_OUTCOMES = [
    "accepted",
    "accepted_with_warnings",
    "quarantined",
    "rejected",
]


def test_source_contract_manifests_are_versioned() -> None:
    for source in ("met", "aic"):
        path = Path("contracts") / source / "v1.yaml"
        manifest = yaml.safe_load(path.read_text())
        assert manifest["source"] == source
        assert manifest["version"] == "1.0.0"
        assert manifest["outcomes"] == EXPECTED_OUTCOMES
