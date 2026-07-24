from pathlib import Path

import yaml

EXPECTED_OUTCOMES = [
    "accepted",
    "accepted_with_warnings",
    "quarantined",
    "rejected",
]
EXPECTED_PROVENANCE = [
    "source_record_id",
    "ingestion_run_id",
    "observed_at",
    "source_url",
    "retrieval_status",
    "retrieved_at",
    "raw_uri",
    "acquisition_context",
]


def test_source_contract_manifests_are_versioned() -> None:
    for source in ("met", "aic"):
        path = Path("contracts") / source / "v1.yaml"
        manifest = yaml.safe_load(path.read_text())
        assert manifest["source"] == source
        assert manifest["version"] == "1.0.0"
        assert manifest["outcomes"] == EXPECTED_OUTCOMES
        assert manifest["provenance"]["required"] == EXPECTED_PROVENANCE


def test_dbt_example_profile_uses_settings_default_path() -> None:
    profile = Path("dbt/profiles.yml.example").read_text()
    assert (
        "{{ env_var('EC_DUCKDB__PATH', 'data/evidence_cartographer.duckdb') }}"
        in profile
    )


def test_compose_binds_services_to_loopback_without_credential_defaults() -> None:
    compose = yaml.safe_load(Path("infra/compose.yaml").read_text())
    postgres = compose["services"]["postgres"]
    minio = compose["services"]["minio"]

    assert postgres["ports"] == ["127.0.0.1:5432:5432"]
    assert minio["ports"] == [
        "127.0.0.1:9000:9000",
        "127.0.0.1:9001:9001",
    ]
    assert postgres["environment"]["POSTGRES_PASSWORD"] == (
        "${EC_POSTGRES__PASSWORD:?EC_POSTGRES__PASSWORD is required}"
    )
    assert minio["environment"]["MINIO_ROOT_USER"] == (
        "${EC_OBJECT_STORE__ACCESS_KEY:?EC_OBJECT_STORE__ACCESS_KEY is required}"
    )
    assert minio["environment"]["MINIO_ROOT_PASSWORD"] == (
        "${EC_OBJECT_STORE__SECRET_KEY:?EC_OBJECT_STORE__SECRET_KEY is required}"
    )


def test_env_example_documents_complete_nested_configuration() -> None:
    env_example = Path(".env.example").read_text()
    expected_keys = {
        "EC_LAKE__BRONZE_PREFIX",
        "EC_LAKE__MANIFEST_PREFIX",
        "EC_LAKE__QUARANTINE_PREFIX",
        "EC_LAKE__SILVER_PREFIX",
        "EC_LAKE__GOLD_PREFIX",
        "EC_PREFECT__API_URL",
        "EC_PREFECT__WORK_POOL_NAME",
        "EC_CONTRACTS__MET_VERSION",
        "EC_CONTRACTS__AIC_VERSION",
        "EC_IMAGE_CACHE__ENABLED",
        "EC_IMAGE_CACHE__PREFIX",
        "EC_IMAGE_CACHE__SELECTION",
    }
    documented_keys = {
        line.split("=", maxsplit=1)[0]
        for line in env_example.splitlines()
        if line and not line.startswith("#") and "=" in line
    }
    assert expected_keys <= documented_keys


def test_required_credentials_remain_blank_in_env_example() -> None:
    env_lines = set(Path(".env.example").read_text().splitlines())
    assert "EC_POSTGRES__PASSWORD=" in env_lines
    assert "EC_OBJECT_STORE__ACCESS_KEY=" in env_lines
    assert "EC_OBJECT_STORE__SECRET_KEY=" in env_lines


def test_compose_examples_use_the_root_environment_file() -> None:
    paths = (
        Path("README.md"),
        Path("docs/superpowers/plans/2026-07-23-project-scaffold.md"),
    )
    compose_examples = [
        line.strip()
        for path in paths
        for line in path.read_text().splitlines()
        if "docker compose" in line
    ]

    assert compose_examples
    assert all(
        "docker compose --env-file .env -f infra/compose.yaml" in example
        for example in compose_examples
    )


def test_readme_requires_users_to_fill_all_three_credentials() -> None:
    readme = Path("README.md").read_text()
    required_credentials = (
        "EC_POSTGRES__PASSWORD",
        "EC_OBJECT_STORE__ACCESS_KEY",
        "EC_OBJECT_STORE__SECRET_KEY",
    )

    assert "fill" in readme.lower()
    assert all(credential in readme for credential in required_credentials)
