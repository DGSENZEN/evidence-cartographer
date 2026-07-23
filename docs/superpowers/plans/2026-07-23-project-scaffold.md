# Evidence Cartographer Project Scaffold Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create a thin, typed, and testable project scaffold for the Met/AIC lake-first ETL platform without implementing museum mappings, ingestion, scoring, or publication policy.

**Architecture:** A Python 3.12 `src` package separates the canonical domain core, generic application ports, museum-specific source descriptors, orchestration, and infrastructure configuration. MinIO/PostgreSQL local infrastructure, DuckDB/dbt directories, Prefect scheduling boundaries, and versioned source-contract manifests sit beside the package.

**Tech Stack:** Python 3.12, uv, Pydantic, Pydantic Settings, Polars, Pandera, DuckDB, dbt Core, dbt-duckdb, Prefect, MinIO, PostgreSQL/psycopg, structlog, pytest, Ruff, mypy, pre-commit, Docker Compose.

## Global Constraints

- Put the project directly at the Git repository root.
- Target Python `>=3.12,<3.13`.
- Use `uv` for dependency and environment management.
- Provide only boilerplate, package boundaries, configuration, typed interfaces, synthetic fixtures, and invariant tests.
- Do not download museum datasets, call museum APIs, map production records, define final quality or Gold policy, fuzzy-merge entities, or download collection images.
- Bronze is immutable and source-shaped; Silver contains all valid canonical records; Gold exposes only policy-eligible records.
- Preserve append-only source history and provide SCD Type 2 field conventions.
- Treat weak entity-resolution matches as review candidates, never automatic merges.

---

## File Map

### Root and developer tooling

- `.python-version`: selects Python 3.12.
- `pyproject.toml`: packaging, dependency, test, lint, formatting, and type-check configuration.
- `.env.example`: safe local defaults and all supported environment keys.
- `.pre-commit-config.yaml`: local formatting and static checks.
- `README.md`: project purpose, architecture, and common commands.
- `.github/workflows/ci.yml`: reproducible scaffold checks.

### Python package

- `src/evidence_cartographer/domain/enums.py`: stable source, run, contract, retrieval, and resolution values.
- `src/evidence_cartographer/domain/models.py`: canonical entity and provenance models.
- `src/evidence_cartographer/application/contracts.py`: versioned contract and validation result types.
- `src/evidence_cartographer/application/ports.py`: acquisition, Bronze, mapping, and publication protocols.
- `src/evidence_cartographer/application/resolution.py`: evidence, candidate, and review types.
- `src/evidence_cartographer/sources/catalog.py`: source descriptor type.
- `src/evidence_cartographer/sources/met/__init__.py`: Met source descriptor.
- `src/evidence_cartographer/sources/aic/__init__.py`: AIC source descriptor.
- `src/evidence_cartographer/infrastructure/settings.py`: nested environment settings.
- `src/evidence_cartographer/infrastructure/logging.py`: structured logging configuration.
- `src/evidence_cartographer/orchestration/flows.py`: generic Prefect flow factory.
- `src/evidence_cartographer/orchestration/schedules.py`: weekly and daily cron defaults.

### Data and infrastructure configuration

- `contracts/met/v1.yaml`: versioned Met contract manifest.
- `contracts/aic/v1.yaml`: versioned AIC contract manifest.
- `dbt/dbt_project.yml`: dbt project configuration.
- `dbt/profiles.yml.example`: environment-driven DuckDB profile.
- `dbt/models/{staging,intermediate,marts}/README.md`: model ownership boundaries.
- `infra/compose.yaml`: PostgreSQL and MinIO local services.

### Tests

- `tests/domain/test_models.py`: canonical and provenance invariants.
- `tests/application/test_contracts.py`: contract outcome and Gold-input invariants.
- `tests/application/test_resolution.py`: deterministic/review resolution types.
- `tests/sources/test_catalog.py`: source descriptors.
- `tests/infrastructure/test_settings.py`: environment settings.
- `tests/orchestration/test_schedules.py`: refresh schedule defaults.

---

### Task 1: Consolidate the repository root and configure Python tooling

**Files:**
- Delete: `evidence-cartographer/.python-version`
- Delete: `evidence-cartographer/README.md`
- Delete: `evidence-cartographer/main.py`
- Delete: `evidence-cartographer/pyproject.toml`
- Create: `.python-version`
- Create: `pyproject.toml`
- Create: `src/evidence_cartographer/__init__.py`
- Create: `tests/test_package.py`

**Interfaces:**
- Produces: importable package `evidence_cartographer`
- Produces: `evidence_cartographer.__version__ == "0.1.0"`
- Produces: commands `uv run pytest`, `uv run ruff check .`, and `uv run mypy src`

- [ ] **Step 1: Write the package smoke test**

```python
# tests/test_package.py
import evidence_cartographer


def test_package_exposes_version() -> None:
    assert evidence_cartographer.__version__ == "0.1.0"
```

- [ ] **Step 2: Run the test and verify the package does not exist**

Run: `uv run --python 3.12 pytest tests/test_package.py -q`

Expected: FAIL because `evidence_cartographer` cannot be imported.

- [ ] **Step 3: Replace the nested starter with the root package configuration**

Delete the four untracked nested starter files listed above using `apply_patch`.

```text
# .python-version
3.12
```

```toml
# pyproject.toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "evidence-cartographer"
version = "0.1.0"
description = "Lake-first collection metadata pipeline for the Met and AIC."
readme = "README.md"
requires-python = ">=3.12,<3.13"
dependencies = [
  "duckdb>=1.2,<2",
  "minio>=7.2,<8",
  "pandera[polars]>=0.23,<1",
  "polars>=1.24,<2",
  "psycopg[binary,pool]>=3.2,<4",
  "pydantic>=2.10,<3",
  "pydantic-settings>=2.7,<3",
  "structlog>=24.4,<26",
]

[project.optional-dependencies]
orchestration = ["prefect>=3.1,<4"]
transforms = ["dbt-core>=1.9,<2", "dbt-duckdb>=1.9,<2"]

[dependency-groups]
dev = [
  "mypy>=1.14,<2",
  "pre-commit>=4,<5",
  "pyyaml>=6,<7",
  "pytest>=8.3,<9",
  "pytest-cov>=6,<7",
  "ruff>=0.9,<1",
  "types-pyyaml>=6.0,<7",
]

[tool.hatch.build.targets.wheel]
packages = ["src/evidence_cartographer"]

[tool.pytest.ini_options]
addopts = "-ra --strict-config --strict-markers"
testpaths = ["tests"]

[tool.ruff]
target-version = "py312"
line-length = 88
src = ["src", "tests"]

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B", "SIM", "RUF"]

[tool.mypy]
python_version = "3.12"
strict = true
packages = ["evidence_cartographer"]
plugins = ["pydantic.mypy"]

[[tool.mypy.overrides]]
module = ["minio.*", "pandera.*"]
ignore_missing_imports = true

[tool.coverage.run]
source = ["evidence_cartographer"]
branch = true

[tool.coverage.report]
show_missing = true
skip_covered = true
```

```python
# src/evidence_cartographer/__init__.py
"""Evidence Cartographer package."""

__version__ = "0.1.0"
```

- [ ] **Step 4: Resolve and install the environment**

Run: `uv sync --all-extras`

Expected: `uv.lock` is created and the project plus all extras install under Python 3.12.

- [ ] **Step 5: Run the smoke test**

Run: `uv run pytest tests/test_package.py -q`

Expected: PASS.

- [ ] **Step 6: Commit the root scaffold**

```bash
git add .python-version pyproject.toml uv.lock src/evidence_cartographer/__init__.py tests/test_package.py
git add -u evidence-cartographer
git commit -m "build: configure Python project scaffold"
```

---

### Task 2: Define canonical domain types

**Files:**
- Create: `src/evidence_cartographer/domain/__init__.py`
- Create: `src/evidence_cartographer/domain/enums.py`
- Create: `src/evidence_cartographer/domain/models.py`
- Create: `tests/domain/test_models.py`

**Interfaces:**
- Produces: `SourceName`, `ContractOutcome`, `IngestionMode`, `RunStatus`, `RetrievalStatus`, and `ResolutionDecision`
- Produces: all thirteen canonical entity models named in the design
- Produces: immutable Pydantic models with forbidden unknown fields

- [ ] **Step 1: Write failing tests for enum stability and provenance**

```python
# tests/domain/test_models.py
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
```

- [ ] **Step 2: Run the tests and verify missing domain modules**

Run: `uv run pytest tests/domain/test_models.py -q`

Expected: FAIL because `evidence_cartographer.domain` does not exist.

- [ ] **Step 3: Implement stable enums**

```python
# src/evidence_cartographer/domain/enums.py
from enum import StrEnum


class SourceName(StrEnum):
    MET = "met"
    AIC = "aic"


class ContractOutcome(StrEnum):
    ACCEPTED = "accepted"
    ACCEPTED_WITH_WARNINGS = "accepted_with_warnings"
    QUARANTINED = "quarantined"
    REJECTED = "rejected"


class IngestionMode(StrEnum):
    FULL_SNAPSHOT = "full_snapshot"
    INCREMENTAL = "incremental"


class RunStatus(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class RetrievalStatus(StrEnum):
    NOT_REQUESTED = "not_requested"
    AVAILABLE = "available"
    CACHED = "cached"
    UNAVAILABLE = "unavailable"
    FAILED = "failed"


class ResolutionDecision(StrEnum):
    UNREVIEWED = "unreviewed"
    LINK = "link"
    REJECT = "reject"
```

- [ ] **Step 4: Implement the canonical model skeleton**

```python
# src/evidence_cartographer/domain/models.py
from datetime import date
from uuid import UUID, uuid4

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from .enums import (
    ContractOutcome,
    IngestionMode,
    RetrievalStatus,
    RunStatus,
    SourceName,
)


class DomainModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Entity(DomainModel):
    id: UUID = Field(default_factory=uuid4)
    created_at: AwareDatetime


class Museum(Entity):
    name: str
    source: SourceName


class Department(Entity):
    museum_id: UUID
    name: str


class Classification(Entity):
    museum_id: UUID
    name: str


class Culture(Entity):
    name: str


class Place(Entity):
    name: str
    latitude: float | None = None
    longitude: float | None = None


class Person(Entity):
    display_name: str
    normalized_name: str | None = None
    birth_date: date | None = None
    death_date: date | None = None
    authority_ids: dict[str, str] = Field(default_factory=dict)


class Object(Entity):
    museum_id: UUID
    source_record_id: UUID
    accession_number: str | None = None
    title: str | None = None
    department_id: UUID | None = None
    classification_id: UUID | None = None
    culture_id: UUID | None = None
    place_id: UUID | None = None


class Image(Entity):
    museum_id: UUID
    source_url: str
    retrieval_url: str | None = None
    iiif_id: str | None = None
    width: int | None = Field(default=None, gt=0)
    height: int | None = Field(default=None, gt=0)
    rights_text: str | None = None
    license_uri: str | None = None
    checksum: str | None = None
    retrieval_status: RetrievalStatus = RetrievalStatus.NOT_REQUESTED
    cached_uri: str | None = None


class ObjectPersonRole(Entity):
    object_id: UUID
    person_id: UUID
    role: str
    attribution_text: str | None = None


class ObjectImage(Entity):
    object_id: UUID
    image_id: UUID
    is_primary: bool = False
    sequence: int | None = Field(default=None, ge=0)


class SourceRecord(Entity):
    museum_id: UUID
    source: SourceName
    source_record_id: str
    contract_version: str
    ingestion_run_id: UUID
    observed_at: AwareDatetime
    raw_uri: str
    raw_checksum: str | None = None
    attribution_text: str | None = None
    outcome: ContractOutcome


class IngestionRun(Entity):
    source: SourceName
    mode: IngestionMode
    status: RunStatus
    started_at: AwareDatetime
    ended_at: AwareDatetime | None = None


class QualityCheck(DomainModel):
    rule_id: str
    passed: bool
    message: str | None = None


class DataQualityResult(Entity):
    source_record_id: UUID
    score: float = Field(ge=0.0, le=1.0)
    checks: tuple[QualityCheck, ...] = ()
    evaluated_at: AwareDatetime


class SCD2Period(DomainModel):
    valid_from: AwareDatetime
    valid_to: AwareDatetime | None = None
    is_current: bool
```

```python
# src/evidence_cartographer/domain/__init__.py
"""Canonical domain models and stable value types."""
```

- [ ] **Step 5: Run domain tests**

Run: `uv run pytest tests/domain/test_models.py -q`

Expected: PASS.

- [ ] **Step 6: Commit the domain core**

```bash
git add src/evidence_cartographer/domain tests/domain
git commit -m "feat: add canonical domain model skeleton"
```

---

### Task 3: Define application contracts and entity-resolution evidence

**Files:**
- Create: `src/evidence_cartographer/application/__init__.py`
- Create: `src/evidence_cartographer/application/contracts.py`
- Create: `src/evidence_cartographer/application/ports.py`
- Create: `src/evidence_cartographer/application/resolution.py`
- Create: `tests/application/test_contracts.py`
- Create: `tests/application/test_resolution.py`

**Interfaces:**
- Consumes: domain enums and models from Task 2
- Produces: `ContractManifest`, `ValidationMessage`, `ContractResult`, and `GoldEligibilitySignals`
- Produces: `RawRecord`, `SourceAdapter`, `BronzeWriter`, `CanonicalMapper`, and `GoldPublisher` protocols
- Produces: deterministic evidence and reviewable `ResolutionCandidate`

- [ ] **Step 1: Write failing application-boundary tests**

```python
# tests/application/test_contracts.py
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
```

```python
# tests/application/test_resolution.py
from uuid import uuid4

from evidence_cartographer.application.resolution import (
    MatchEvidence,
    ResolutionCandidate,
)
from evidence_cartographer.domain.enums import ResolutionDecision


def test_weak_candidate_defaults_to_human_review() -> None:
    candidate = ResolutionCandidate(
        left_person_id=uuid4(),
        right_person_id=uuid4(),
        evidence=(
            MatchEvidence(
                rule_id="exact_name_and_dates",
                field="normalized_name",
                left_value="claude monet",
                right_value="claude monet",
                is_strong_identifier=False,
            ),
        ),
        confidence=0.85,
    )
    assert candidate.decision is ResolutionDecision.UNREVIEWED
    assert not candidate.can_auto_link
```

- [ ] **Step 2: Run the tests and verify missing application modules**

Run: `uv run pytest tests/application -q`

Expected: FAIL because the application modules do not exist.

- [ ] **Step 3: Implement contract and Gold signal types**

```python
# src/evidence_cartographer/application/contracts.py
from pydantic import BaseModel, ConfigDict, Field

from evidence_cartographer.domain.enums import ContractOutcome, SourceName


class ApplicationModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ContractManifest(ApplicationModel):
    source: SourceName
    version: str
    formats: tuple[str, ...]
    outcomes: tuple[ContractOutcome, ...]


class ValidationMessage(ApplicationModel):
    rule_id: str
    message: str
    field: str | None = None


class ContractResult(ApplicationModel):
    outcome: ContractOutcome
    messages: tuple[ValidationMessage, ...] = ()


class GoldEligibilitySignals(ApplicationModel):
    rights_are_permissive: bool | None
    has_usable_image: bool
    metadata_quality_score: float = Field(ge=0.0, le=1.0)
```

- [ ] **Step 4: Implement the generic pipeline ports**

```python
# src/evidence_cartographer/application/ports.py
from collections.abc import Iterable, Mapping
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict

from evidence_cartographer.application.contracts import (
    ContractResult,
    GoldEligibilitySignals,
)
from evidence_cartographer.domain.enums import IngestionMode, SourceName
from evidence_cartographer.domain.models import Object, SourceRecord


class RawRecord(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    source_record_id: str
    payload: Mapping[str, Any]


class SourceAdapter(Protocol):
    source: SourceName

    def acquire(self, mode: IngestionMode) -> Iterable[RawRecord]: ...


class ContractValidator(Protocol):
    def validate(self, record: RawRecord) -> ContractResult: ...


class BronzeWriter(Protocol):
    def append(self, record: RawRecord, provenance: SourceRecord) -> None: ...


class CanonicalMapper(Protocol):
    def map_object(self, record: RawRecord, provenance: SourceRecord) -> Object: ...


class GoldPublisher(Protocol):
    def publish(self, object_: Object, signals: GoldEligibilitySignals) -> None: ...
```

- [ ] **Step 5: Implement conservative resolution evidence**

```python
# src/evidence_cartographer/application/resolution.py
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, computed_field

from evidence_cartographer.domain.enums import ResolutionDecision


class MatchEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    rule_id: str
    field: str
    left_value: str
    right_value: str
    is_strong_identifier: bool


class ResolutionCandidate(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    left_person_id: UUID
    right_person_id: UUID
    evidence: tuple[MatchEvidence, ...]
    confidence: float = Field(ge=0.0, le=1.0)
    decision: ResolutionDecision = ResolutionDecision.UNREVIEWED
    reviewer_note: str | None = None

    @computed_field
    @property
    def can_auto_link(self) -> bool:
        return bool(self.evidence) and all(
            item.is_strong_identifier for item in self.evidence
        )
```

```python
# src/evidence_cartographer/application/__init__.py
"""Use-case contracts that connect the domain to external adapters."""
```

- [ ] **Step 6: Run application tests and static checks**

Run: `uv run pytest tests/application -q`

Expected: PASS.

Run: `uv run mypy src/evidence_cartographer/domain src/evidence_cartographer/application`

Expected: PASS.

- [ ] **Step 7: Commit application boundaries**

```bash
git add src/evidence_cartographer/application tests/application
git commit -m "feat: define pipeline and resolution contracts"
```

---

### Task 4: Add Met and AIC source descriptors

**Files:**
- Create: `src/evidence_cartographer/sources/__init__.py`
- Create: `src/evidence_cartographer/sources/catalog.py`
- Create: `src/evidence_cartographer/sources/met/__init__.py`
- Create: `src/evidence_cartographer/sources/aic/__init__.py`
- Create: `tests/sources/test_catalog.py`

**Interfaces:**
- Consumes: `SourceName`
- Produces: immutable `SourceDescriptor`
- Produces: `MET_SOURCE` and `AIC_SOURCE` descriptors
- Does not produce network clients, parsers, or mappers

- [ ] **Step 1: Write failing descriptor tests**

```python
# tests/sources/test_catalog.py
from evidence_cartographer.domain.enums import SourceName
from evidence_cartographer.sources.aic import AIC_SOURCE
from evidence_cartographer.sources.met import MET_SOURCE


def test_met_descriptor_separates_bulk_and_api_acquisition() -> None:
    assert MET_SOURCE.source is SourceName.MET
    assert MET_SOURCE.bulk_format == "csv"
    assert MET_SOURCE.supports_incremental_api


def test_aic_descriptor_separates_bulk_and_api_acquisition() -> None:
    assert AIC_SOURCE.source is SourceName.AIC
    assert AIC_SOURCE.bulk_format == "json"
    assert AIC_SOURCE.supports_incremental_api
```

- [ ] **Step 2: Run the tests and verify missing source modules**

Run: `uv run pytest tests/sources/test_catalog.py -q`

Expected: FAIL because source modules do not exist.

- [ ] **Step 3: Implement the descriptor and catalog entries**

```python
# src/evidence_cartographer/sources/catalog.py
from pydantic import BaseModel, ConfigDict

from evidence_cartographer.domain.enums import SourceName


class SourceDescriptor(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    source: SourceName
    display_name: str
    bulk_format: str
    supports_incremental_api: bool
    contract_version: str
```

```python
# src/evidence_cartographer/sources/met/__init__.py
from evidence_cartographer.domain.enums import SourceName
from evidence_cartographer.sources.catalog import SourceDescriptor

MET_SOURCE = SourceDescriptor(
    source=SourceName.MET,
    display_name="The Metropolitan Museum of Art",
    bulk_format="csv",
    supports_incremental_api=True,
    contract_version="1.0.0",
)
```

```python
# src/evidence_cartographer/sources/aic/__init__.py
from evidence_cartographer.domain.enums import SourceName
from evidence_cartographer.sources.catalog import SourceDescriptor

AIC_SOURCE = SourceDescriptor(
    source=SourceName.AIC,
    display_name="Art Institute of Chicago",
    bulk_format="json",
    supports_incremental_api=True,
    contract_version="1.0.0",
)
```

```python
# src/evidence_cartographer/sources/__init__.py
"""Museum-specific source descriptors and future adapters."""
```

- [ ] **Step 4: Run descriptor tests**

Run: `uv run pytest tests/sources/test_catalog.py -q`

Expected: PASS.

- [ ] **Step 5: Commit source package skeletons**

```bash
git add src/evidence_cartographer/sources tests/sources
git commit -m "feat: add Met and AIC source descriptors"
```

---

### Task 5: Add environment settings and structured logging

**Files:**
- Create: `src/evidence_cartographer/infrastructure/__init__.py`
- Create: `src/evidence_cartographer/infrastructure/settings.py`
- Create: `src/evidence_cartographer/infrastructure/logging.py`
- Create: `.env.example`
- Create: `tests/infrastructure/test_settings.py`

**Interfaces:**
- Produces: `Settings` loaded from `EC_` environment variables with `__` nesting
- Produces: `configure_logging(log_level: str) -> None`
- Keeps credentials out of checked-in Python and YAML

- [ ] **Step 1: Write a failing nested-settings test**

```python
# tests/infrastructure/test_settings.py
from evidence_cartographer.infrastructure.settings import Settings


def test_settings_load_nested_environment_values(monkeypatch) -> None:
    monkeypatch.setenv("EC_POSTGRES__PASSWORD", "test-secret")
    monkeypatch.setenv("EC_OBJECT_STORE__ENDPOINT", "minio.internal:9000")

    settings = Settings(_env_file=None)

    assert settings.postgres.password.get_secret_value() == "test-secret"
    assert settings.object_store.endpoint == "minio.internal:9000"
    assert settings.refresh.weekly_full_cron == "0 2 * * 0"
    assert settings.refresh.daily_incremental_cron == "0 3 * * *"
```

- [ ] **Step 2: Run the test and verify missing infrastructure modules**

Run: `uv run pytest tests/infrastructure/test_settings.py -q`

Expected: FAIL because infrastructure modules do not exist.

- [ ] **Step 3: Implement settings models**

```python
# src/evidence_cartographer/infrastructure/settings.py
from pathlib import Path

from pydantic import BaseModel, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class PostgresSettings(BaseModel):
    host: str = "localhost"
    port: int = 5432
    database: str = "evidence_cartographer"
    user: str = "evidence_cartographer"
    password: SecretStr = SecretStr("evidence_cartographer")


class ObjectStoreSettings(BaseModel):
    endpoint: str = "localhost:9000"
    access_key: str = "minioadmin"
    secret_key: SecretStr = SecretStr("minioadmin")
    secure: bool = False
    bronze_bucket: str = "bronze"
    silver_bucket: str = "silver"
    gold_bucket: str = "gold"
    quarantine_bucket: str = "quarantine"
    image_cache_bucket: str = "image-cache"


class DuckDBSettings(BaseModel):
    path: Path = Path("data/evidence_cartographer.duckdb")


class RefreshSettings(BaseModel):
    weekly_full_cron: str = "0 2 * * 0"
    daily_incremental_cron: str = "0 3 * * *"
    timezone: str = "UTC"


class SourceEndpoints(BaseModel):
    met_api_base_url: str = "https://collectionapi.metmuseum.org/public/collection/v1"
    aic_api_base_url: str = "https://api.artic.edu/api/v1"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="EC_",
        env_nested_delimiter="__",
        env_file=".env",
        extra="ignore",
    )

    environment: str = "development"
    log_level: str = "INFO"
    postgres: PostgresSettings = PostgresSettings()
    object_store: ObjectStoreSettings = ObjectStoreSettings()
    duckdb: DuckDBSettings = DuckDBSettings()
    refresh: RefreshSettings = RefreshSettings()
    sources: SourceEndpoints = SourceEndpoints()
```

- [ ] **Step 4: Implement structured logging setup**

```python
# src/evidence_cartographer/infrastructure/logging.py
import logging

import structlog


def configure_logging(log_level: str = "INFO") -> None:
    logging.basicConfig(
        format="%(message)s",
        level=log_level.upper(),
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelNamesMapping()[log_level.upper()]
        ),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
```

```python
# src/evidence_cartographer/infrastructure/__init__.py
"""Configuration and external-system adapter boundaries."""
```

```dotenv
# .env.example
EC_ENVIRONMENT=development
EC_LOG_LEVEL=INFO

EC_POSTGRES__HOST=localhost
EC_POSTGRES__PORT=5432
EC_POSTGRES__DATABASE=evidence_cartographer
EC_POSTGRES__USER=evidence_cartographer
EC_POSTGRES__PASSWORD=evidence_cartographer

EC_OBJECT_STORE__ENDPOINT=localhost:9000
EC_OBJECT_STORE__ACCESS_KEY=minioadmin
EC_OBJECT_STORE__SECRET_KEY=minioadmin
EC_OBJECT_STORE__SECURE=false
EC_OBJECT_STORE__BRONZE_BUCKET=bronze
EC_OBJECT_STORE__SILVER_BUCKET=silver
EC_OBJECT_STORE__GOLD_BUCKET=gold
EC_OBJECT_STORE__QUARANTINE_BUCKET=quarantine
EC_OBJECT_STORE__IMAGE_CACHE_BUCKET=image-cache

EC_DUCKDB__PATH=data/evidence_cartographer.duckdb

EC_REFRESH__WEEKLY_FULL_CRON="0 2 * * 0"
EC_REFRESH__DAILY_INCREMENTAL_CRON="0 3 * * *"
EC_REFRESH__TIMEZONE=UTC

EC_SOURCES__MET_API_BASE_URL=https://collectionapi.metmuseum.org/public/collection/v1
EC_SOURCES__AIC_API_BASE_URL=https://api.artic.edu/api/v1
```

- [ ] **Step 5: Run infrastructure tests and static checks**

Run: `uv run pytest tests/infrastructure/test_settings.py -q`

Expected: PASS.

Run: `uv run ruff check src/evidence_cartographer/infrastructure tests/infrastructure`

Expected: PASS.

- [ ] **Step 6: Commit configuration skeleton**

```bash
git add .env.example src/evidence_cartographer/infrastructure tests/infrastructure
git commit -m "feat: add typed environment configuration"
```

---

### Task 6: Add refresh orchestration boundaries

**Files:**
- Create: `src/evidence_cartographer/orchestration/__init__.py`
- Create: `src/evidence_cartographer/orchestration/flows.py`
- Create: `src/evidence_cartographer/orchestration/schedules.py`
- Create: `tests/orchestration/test_schedules.py`

**Interfaces:**
- Consumes: `SourceName`, `IngestionMode`
- Produces: `WEEKLY_FULL_CRON` and `DAILY_INCREMENTAL_CRON`
- Produces: `build_ingestion_flow(run_pipeline) -> Flow`
- Does not contain source acquisition or transformation logic

- [ ] **Step 1: Write a failing schedule test**

```python
# tests/orchestration/test_schedules.py
from evidence_cartographer.orchestration.schedules import (
    DAILY_INCREMENTAL_CRON,
    WEEKLY_FULL_CRON,
)


def test_refresh_crons_are_distinct() -> None:
    assert WEEKLY_FULL_CRON == "0 2 * * 0"
    assert DAILY_INCREMENTAL_CRON == "0 3 * * *"
    assert WEEKLY_FULL_CRON != DAILY_INCREMENTAL_CRON
```

- [ ] **Step 2: Run the test and verify missing orchestration modules**

Run: `uv run pytest tests/orchestration/test_schedules.py -q`

Expected: FAIL because orchestration modules do not exist.

- [ ] **Step 3: Implement schedule constants and an injected generic flow**

```python
# src/evidence_cartographer/orchestration/schedules.py
WEEKLY_FULL_CRON = "0 2 * * 0"
DAILY_INCREMENTAL_CRON = "0 3 * * *"
DEFAULT_TIMEZONE = "UTC"
```

```python
# src/evidence_cartographer/orchestration/flows.py
from collections.abc import Callable

from prefect import Flow, flow

from evidence_cartographer.domain.enums import IngestionMode, SourceName

PipelineRunner = Callable[[SourceName, IngestionMode], None]


def build_ingestion_flow(run_pipeline: PipelineRunner) -> Flow[..., None]:
    @flow(name="collection-ingestion")
    def ingestion_flow(source: SourceName, mode: IngestionMode) -> None:
        run_pipeline(source, mode)

    return ingestion_flow
```

```python
# src/evidence_cartographer/orchestration/__init__.py
"""Prefect flow construction and refresh schedule defaults."""
```

- [ ] **Step 4: Run orchestration tests with the orchestration extra**

Run: `uv run --extra orchestration pytest tests/orchestration -q`

Expected: PASS.

- [ ] **Step 5: Commit orchestration skeleton**

```bash
git add src/evidence_cartographer/orchestration tests/orchestration
git commit -m "feat: add refresh orchestration boundaries"
```

---

### Task 7: Add source-contract, dbt, and local service configuration

**Files:**
- Create: `contracts/met/v1.yaml`
- Create: `contracts/aic/v1.yaml`
- Create: `dbt/dbt_project.yml`
- Create: `dbt/profiles.yml.example`
- Create: `dbt/models/staging/README.md`
- Create: `dbt/models/intermediate/README.md`
- Create: `dbt/models/marts/README.md`
- Create: `infra/compose.yaml`
- Create: `tests/config/test_manifests.py`

**Interfaces:**
- Consumes: the four stable contract outcome values
- Produces: versioned source manifests without declaring source field mappings
- Produces: dbt layer ownership and environment-driven DuckDB configuration
- Produces: healthy local PostgreSQL and MinIO services

- [ ] **Step 1: Write failing contract-manifest tests**

```python
# tests/config/test_manifests.py
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
```

- [ ] **Step 2: Run the test and verify missing manifests**

Run: `uv run pytest tests/config/test_manifests.py -q`

Expected: FAIL because the contract files do not exist.

- [ ] **Step 3: Add versioned source manifests**

```yaml
# contracts/met/v1.yaml
source: met
version: 1.0.0
bulk_format: csv
incremental_format: json
outcomes:
  - accepted
  - accepted_with_warnings
  - quarantined
  - rejected
provenance:
  required:
    - source_record_id
    - ingestion_run_id
    - observed_at
    - raw_uri
```

```yaml
# contracts/aic/v1.yaml
source: aic
version: 1.0.0
bulk_format: json
incremental_format: json
outcomes:
  - accepted
  - accepted_with_warnings
  - quarantined
  - rejected
provenance:
  required:
    - source_record_id
    - ingestion_run_id
    - observed_at
    - raw_uri
```

- [ ] **Step 4: Add dbt configuration and model-boundary documentation**

```yaml
# dbt/dbt_project.yml
name: evidence_cartographer
version: 1.0.0
config-version: 2
profile: evidence_cartographer
model-paths: ["models"]
target-path: "target"
clean-targets: ["target", "dbt_packages"]

models:
  evidence_cartographer:
    staging:
      +materialized: view
    intermediate:
      +materialized: incremental
    marts:
      +materialized: table
```

```yaml
# dbt/profiles.yml.example
evidence_cartographer:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "{{ env_var('EC_DUCKDB__PATH', '../data/evidence_cartographer.duckdb') }}"
      threads: 4
```

```markdown
<!-- dbt/models/staging/README.md -->
# Staging models

Source-shaped views over Bronze objects. Keep Met and AIC field names isolated
here before canonical normalization.
```

```markdown
<!-- dbt/models/intermediate/README.md -->
# Intermediate models

Silver canonical transformations, validation outputs, and SCD Type 2 history.
Use `valid_from`, `valid_to`, and `is_current` for historical rows.
```

```markdown
<!-- dbt/models/marts/README.md -->
# Mart models

Gold publication models and PostgreSQL-facing exports. Models in this directory
must apply explicit rights, usable-image, and metadata-quality gates.
```

- [ ] **Step 5: Add local PostgreSQL and MinIO services**

```yaml
# infra/compose.yaml
name: evidence-cartographer

services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: ${EC_POSTGRES__DATABASE:-evidence_cartographer}
      POSTGRES_USER: ${EC_POSTGRES__USER:-evidence_cartographer}
      POSTGRES_PASSWORD: ${EC_POSTGRES__PASSWORD:-evidence_cartographer}
    ports:
      - "5432:5432"
    volumes:
      - postgres-data:/var/lib/postgresql/data
    healthcheck:
      test:
        - CMD-SHELL
        - pg_isready -U $${POSTGRES_USER} -d $${POSTGRES_DB}
      interval: 5s
      timeout: 5s
      retries: 10

  minio:
    image: quay.io/minio/minio:RELEASE.2025-04-22T22-12-26Z
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: ${EC_OBJECT_STORE__ACCESS_KEY:-minioadmin}
      MINIO_ROOT_PASSWORD: ${EC_OBJECT_STORE__SECRET_KEY:-minioadmin}
    ports:
      - "9000:9000"
      - "9001:9001"
    volumes:
      - minio-data:/data
    healthcheck:
      test: ["CMD", "mc", "ready", "local"]
      interval: 5s
      timeout: 5s
      retries: 10

volumes:
  postgres-data:
  minio-data:
```

- [ ] **Step 6: Run configuration tests and parsers**

Run: `uv run pytest tests/config/test_manifests.py -q`

Expected: PASS.

Run: `docker compose -f infra/compose.yaml config --quiet`

Expected: exit code 0.

Run: `mkdir -p /tmp/evidence-cartographer-dbt-profile`

Run: `cp dbt/profiles.yml.example /tmp/evidence-cartographer-dbt-profile/profiles.yml`

Run: `uv run --extra transforms dbt parse --project-dir dbt --profiles-dir /tmp/evidence-cartographer-dbt-profile`

Expected: dbt parses successfully with the example environment-driven profile.

- [ ] **Step 7: Commit data-platform configuration**

```bash
git add contracts dbt infra tests/config
git commit -m "build: scaffold contracts dbt and local services"
```

---

### Task 8: Complete documentation, ignores, automation, and verification

**Files:**
- Modify: `.gitignore`
- Modify: `README.md`
- Create: `.pre-commit-config.yaml`
- Create: `.github/workflows/ci.yml`
- Create: `docs/architecture.md`

**Interfaces:**
- Documents: setup, boundaries, common commands, and non-goals
- Produces: CI parity for tests, Ruff, and mypy
- Prevents: credentials, local lake data, dbt outputs, and database files from entering Git

- [ ] **Step 1: Extend repository ignores**

Append these entries to `.gitignore`:

```gitignore
# Evidence Cartographer local state
.env
data/*
!data/.gitkeep
*.duckdb
*.duckdb.wal
dbt/profiles.yml
dbt/target/
dbt/logs/
dbt/dbt_packages/
.prefect/
```

Create an empty `data/.gitkeep`.

- [ ] **Step 2: Add pre-commit configuration**

```yaml
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.9.10
    hooks:
      - id: ruff
        args: [--fix]
      - id: ruff-format
  - repo: local
    hooks:
      - id: mypy
        name: mypy
        entry: uv run mypy src
        language: system
        pass_filenames: false
```

- [ ] **Step 3: Replace the root README**

````markdown
# Evidence Cartographer

Lake-first ETL foundations for complete Met and Art Institute of Chicago
collection metadata.

## Layers

- **Bronze:** immutable, complete source observations.
- **Silver:** all valid normalized records with quality evidence and history.
- **Gold:** records that pass explicit rights, image, and metadata-quality gates.

The repository contains only the initial package and configuration scaffold.
Museum mappings, quality policy, and production ingestion are intentionally not
implemented.

## Setup

```bash
uv sync --all-extras
cp .env.example .env
docker compose -f infra/compose.yaml up -d
```

## Checks

```bash
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run mypy src
docker compose -f infra/compose.yaml config --quiet
```

## Package boundaries

- `domain`: canonical entities and stable value types.
- `application`: pipeline ports, contracts, and resolution evidence.
- `sources`: isolated Met and AIC source descriptors and future adapters.
- `infrastructure`: settings, logging, and future external-system adapters.
- `orchestration`: Prefect flow construction and schedule defaults.

See `docs/architecture.md` for the data-flow and ownership rules.
````

- [ ] **Step 4: Add concise architecture documentation**

```markdown
# Architecture

## Data flow

`acquire → contract → Bronze append → canonical map → Silver quality/history → Gold policy`

Every source observation remains addressable by ingestion run, source identity,
contract version, observation timestamp, raw object URI, and contract outcome.

## Ownership rules

- Domain code does not import source or infrastructure packages.
- Source packages do not define canonical entity variants.
- Application protocols depend on domain types, not concrete storage clients.
- Infrastructure adapters implement application protocols.
- Gold policy consumes explicit rights, image, and quality signals.
- Weak identity evidence creates review candidates and cannot auto-link.

## Image policy

Metadata and source URLs are ingested for all images. Cached image objects have
their own retrieval state and object URI. An external URL is never equivalent
to a permanently stored image.

## History

Raw source observations are append-only. Silver history uses `valid_from`,
`valid_to`, and `is_current`; current rows never erase earlier observations.
```

- [ ] **Step 5: Add CI**

```yaml
# .github/workflows/ci.yml
name: CI

on:
  pull_request:
  push:
    branches: [main]

permissions:
  contents: read

jobs:
  checks:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v6
        with:
          python-version: "3.12"
          enable-cache: true
      - run: uv sync --locked --all-extras
      - run: uv run pytest
      - run: uv run ruff check .
      - run: uv run ruff format --check .
      - run: uv run mypy src
```

- [ ] **Step 6: Run the full verification suite**

Run: `uv run pytest`

Expected: all tests PASS.

Run: `uv run ruff check .`

Expected: PASS with no diagnostics.

Run: `uv run ruff format --check .`

Expected: PASS with no formatting changes required.

Run: `uv run mypy src`

Expected: PASS with no type errors.

Run: `docker compose -f infra/compose.yaml config --quiet`

Expected: exit code 0.

Run: `git status --short`

Expected: only intentional scaffold files are modified or untracked; no `.env`,
local data, DuckDB files, dbt targets, or service volumes appear.

- [ ] **Step 7: Commit final scaffold documentation**

```bash
git add .gitignore .pre-commit-config.yaml .github README.md docs/architecture.md data/.gitkeep
git commit -m "docs: document scaffold workflow and boundaries"
```

---

## Plan Self-Review

- **Spec coverage:** Tasks cover root consolidation, Python 3.12/uv, all
  canonical entities, contract outcomes, source isolation, settings, logging,
  Prefect boundaries, dbt layers, local services, image metadata, history,
  resolution evidence, tests, and documentation.
- **Scope control:** No task implements network access, museum parsing,
  canonical mapping, quality scoring, Gold policy, image downloading, or fuzzy
  resolution.
- **Type consistency:** `SourceName`, `IngestionMode`, `SourceRecord`,
  `GoldEligibilitySignals`, and protocol names are defined before their
  consumers.
- **Verification:** Each behavioral task follows a failing-test-first cycle;
  configuration-only tasks use parser and static validation.
