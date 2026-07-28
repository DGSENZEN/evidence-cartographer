import subprocess
import tomllib
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


def test_private_minio_request_dependencies_are_exactly_pinned() -> None:
    with Path("pyproject.toml").open("rb") as project_file:
        project = tomllib.load(project_file)
    with Path("uv.lock").open("rb") as lock_file:
        lock = tomllib.load(lock_file)
    dependencies = set(project["project"]["dependencies"])
    root_package = next(
        package
        for package in lock["package"]
        if package["name"] == "evidence-cartographer"
    )
    locked_requirements = {
        requirement["name"]: requirement["specifier"]
        for requirement in root_package["metadata"]["requires-dist"]
        if requirement["name"] in {"minio", "urllib3"}
    }
    locked_versions = {
        package["name"]: package["version"]
        for package in lock["package"]
        if package["name"] in {"minio", "urllib3"}
    }

    assert "minio==7.2.20" in dependencies
    assert "urllib3==2.7.0" in dependencies
    assert locked_requirements == {
        "minio": "==7.2.20",
        "urllib3": "==2.7.0",
    }
    assert locked_versions == {
        "minio": "7.2.20",
        "urllib3": "2.7.0",
    }


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


def test_repository_has_no_environment_template() -> None:
    assert not Path(".env.example").exists()


def test_real_environment_file_is_ignored_and_untracked() -> None:
    ignored = subprocess.run(
        ["git", "check-ignore", "--quiet", ".env"],
        check=False,
    )
    tracked = subprocess.run(
        ["git", "ls-files", "--error-unmatch", ".env"],
        check=False,
        capture_output=True,
    )
    assert ignored.returncode == 0
    assert tracked.returncode != 0


def test_readme_documents_required_environment_names_without_values() -> None:
    readme = Path("README.md").read_text()
    required = (
        "EC_POSTGRES__PASSWORD",
        "EC_OBJECT_STORE__ACCESS_KEY",
        "EC_OBJECT_STORE__SECRET_KEY",
    )
    assert all(name in readme for name in required)
    assert ".env.example" not in readme


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
