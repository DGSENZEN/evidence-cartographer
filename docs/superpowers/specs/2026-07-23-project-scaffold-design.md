# Evidence Cartographer Project Scaffold Design

## Purpose

Evidence Cartographer will ingest the complete available metadata catalogs of
The Metropolitan Museum of Art (Met) and the Art Institute of Chicago (AIC).
The initial implementation is deliberately limited to project boilerplate,
package boundaries, configuration, typed interfaces, and small synthetic test
fixtures. Museum-specific mapping, scoring, and ingestion logic remain for the
project owner to implement.

The scaffold targets Python 3.12 and uses `uv` for dependency and environment
management.

## Architecture

The Python codebase uses a domain-core and source-adapter architecture:

- `domain` contains canonical entities, shared value types, identifiers,
  provenance, and contract status values. It has no dependencies on museum
  payload shapes or infrastructure implementations.
- `sources` isolates the Met and AIC integrations. Each source package has
  placeholders for versioned contracts, bulk acquisition, API acquisition,
  and canonical mapping.
- `application` defines use-case boundaries for Bronze ingestion, Silver
  normalization, Gold eligibility, data quality, history, and entity
  resolution.
- `infrastructure` contains configuration-facing adapter skeletons for MinIO,
  PostgreSQL, DuckDB, dbt, Prefect, and structured logging.

Non-Python project areas sit beside the application package:

- `dbt` contains staging, intermediate/SCD Type 2, and Gold model skeletons.
- `contracts` contains versioned, source-specific ingestion-contract
  configuration.
- `infra/compose.yaml` defines local PostgreSQL and MinIO services.
- `tests` mirrors the package boundaries and uses only synthetic fixtures.
- `docs` records architecture, data modeling, and contribution guidance.
- `data` is ignored except for intentionally committed tiny fixtures.

## Medallion Responsibilities

### Bronze

Bronze is immutable and source-shaped. It preserves every acquired Met and AIC
record, acquisition metadata, source URL, retrieval status, and payload
location. Weekly full snapshots and daily API checks both append observations;
neither overwrites earlier source state.

### Silver

Silver contains every valid canonical record, including works without usable
images and works that are not public domain. It includes normalization results,
quality scores, validation evidence, and SCD Type 2 history. A record accepted
with warnings may proceed to Silver with those warnings retained.

### Gold

Gold contains only records that meet all publication gates: sufficiently
permissive rights, a valid usable image, and the configured minimum metadata
quality. Gold policy is represented by an application boundary and dbt model
skeleton; the scaffold does not choose the final rights vocabulary, quality
threshold, or image validation rules.

## Data Flow

Every run receives a unique `IngestionRun` identity and follows this logical
path:

1. A source acquisition adapter retrieves a bulk snapshot or API change.
2. The versioned ingestion contract classifies the record.
3. The raw observation and classification evidence are appended to Bronze.
4. Accepted records and records accepted with warnings enter canonical mapping.
5. Canonical validation and quality assessment produce Silver records and
   `DataQualityResult` evidence.
6. Gold policy evaluates Silver records for publication eligibility.

The four contract outcomes are:

- `accepted`
- `accepted_with_warnings`
- `quarantined`
- `rejected`

Quarantined and rejected records remain traceable through their Bronze
observation and validation evidence. No raw record is silently discarded.
Snapshot and incremental acquisition use the same downstream contracts and
canonical mapping boundaries.

## Storage Responsibilities

- MinIO stores immutable raw payloads and manifests, quarantine artifacts,
  normalized Parquet datasets, Gold lake outputs, and selectively cached image
  objects.
- DuckDB reads and transforms lake files locally.
- dbt Core owns SQL transformation organization for staging, Silver history,
  and Gold publication models.
- PostgreSQL stores servable Gold tables and operational data such as ingestion
  runs, quality evidence, resolution candidates, review decisions, and source
  record indexes.

The scaffold supplies configuration and adapter protocols, not production
repositories or network clients.

## Canonical Entities

The domain package defines typed skeletons for:

- `Museum`
- `Object`
- `Person`
- `Image`
- `Department`
- `Classification`
- `Culture`
- `Place`
- `ObjectPersonRole`
- `ObjectImage`
- `SourceRecord`
- `IngestionRun`
- `DataQualityResult`

Canonical entities carry stable internal identity, source provenance, and
timestamps appropriate to their role. `SourceRecord` preserves the original
source identity, payload location, acquisition context, and original
attribution data. Source-specific fields do not leak into canonical entity
interfaces.

## Image Handling

Image processing is metadata-first. The canonical image model and source
adapters can represent:

- source and retrieval URLs;
- dimensions;
- rights and license assertions;
- IIIF identifiers;
- checksums when supplied or computed;
- availability and retrieval status;
- cached object location when an image has deliberately been downloaded.

An external image URL is never modeled as permanent or equivalent to a cached
object. The initial project does not bulk-download collection images.

## Historical Change Tracking

Source history is append-only: every observed source version retains its
acquisition time and payload reference. Silver history uses SCD Type 2 fields
(`valid_from`, `valid_to`, and `is_current`) so canonical changes do not erase
prior state. The dbt directory includes the appropriate model boundaries but
does not implement source-specific history SQL in the scaffold.

## Entity Resolution

Entity resolution is separated from source normalization. The scaffold defines
evidence and review interfaces for the following staged approach:

1. Deterministic matching through shared authority identifiers such as
   Wikidata, ULAN, or an exact source authority ID.
2. Exact normalized name plus birth/death date matching.
3. Candidate-link generation for uncertain or incomplete matches.
4. Human review decisions with retained confidence evidence.

Weak matches are never automatically merged. No fuzzy auto-merging algorithm
is included. Original attribution text is retained even after a deterministic
link or reviewed merge.

## Configuration and Local Development

Pydantic Settings loads environment-specific configuration. `.env.example`
documents non-secret settings and local service defaults; `.env` remains
ignored. Configuration covers source endpoints, lake buckets and prefixes,
PostgreSQL, DuckDB, Prefect, contract versions, image-cache behavior, and
logging.

`infra/compose.yaml` supplies PostgreSQL and MinIO with health checks and
persistent named volumes. dbt provides a checked-in project file and an example
profile that reads credentials from environment variables. Prefect provides
flow, deployment, and weekly/daily schedule skeletons without embedding
museum-specific ingestion behavior.

## Error Handling and Observability

The Python package provides typed base exceptions for acquisition, contract,
storage, mapping, and quality failures. Structured logging includes source,
ingestion-run identity, contract version, and source-record identity when
available. Retry hooks are exposed at acquisition and orchestration boundaries,
but the scaffold does not assert a production retry or backoff policy.

Data-quality failures use the contract outcome and quality-evidence models
instead of being represented only as thrown exceptions.

## Tooling and Dependencies

Runtime dependencies are limited to the agreed stack and configuration needs:
Polars, DuckDB, dbt Core with the DuckDB adapter, Pydantic, Pydantic Settings,
Pandera, Prefect, PostgreSQL connectivity, MinIO connectivity, and structured
logging support.

Development tooling includes pytest, Ruff, mypy, pre-commit, and coverage
configuration. Dependencies are grouped so developer tooling and orchestration
extras remain distinguishable from the core package.

## Verification Scope

The scaffold includes small tests for boilerplate invariants:

- the four contract outcome values are stable;
- required provenance cannot be omitted from source-record models;
- canonical entity skeletons accept valid synthetic data;
- Gold eligibility input exposes rights, image, and quality signals without
  embedding policy;
- settings load from environment variables;
- source packages expose the expected adapter boundaries.

Tests do not implement or assert real Met/AIC field mappings, production
quality-scoring rules, full ingestion, network access, or image downloading.

## Explicit Non-Goals

This scaffold does not:

- download Met CSV snapshots or AIC JSON dumps;
- call either museum API;
- map production museum records;
- define a final metadata-quality scoring formula;
- establish final Gold rights or image policy;
- perform automatic fuzzy entity merging;
- bulk-download or permanently cache collection images;
- deploy infrastructure to a hosted environment;
- provide an end-user application or API.

## Acceptance Criteria

The scaffold is complete when:

1. The project lives directly at the Git repository root and targets Python
   3.12 under `uv`.
2. The package, source-adapter, dbt, contract, infrastructure, documentation,
   and test boundaries described above exist and are importable where
   applicable.
3. Local service and environment examples are coherent and contain no secrets.
4. Static checks and scaffold-invariant tests can be run from documented
   commands.
5. No museum-specific ETL implementation or speculative business policy is
   hidden inside the boilerplate.
