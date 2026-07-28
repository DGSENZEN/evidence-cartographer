# Met Snapshot Ingestion Design

## Goal

Build the first real source-to-Bronze vertical slice for The Metropolitan
Museum of Art. The slice downloads the complete official Open Access CSV,
validates every row through a versioned Met contract, and commits the unchanged
artifact plus record evidence through the existing immutable Bronze boundary.

The official dataset is the Met's weekly-updated UTF-8 CSV published through
the verified `metmuseum/openaccess` GitHub repository:

`https://github.com/metmuseum/openaccess/raw/refs/heads/master/MetObjects.csv`

The downloader follows the redirect to GitHub's large-file media endpoint. The
URL remains configurable so a source relocation does not require code changes.

## Scope

This slice includes:

- a production streaming HTTP downloader for the real Met snapshot;
- acquisition metadata, size, and SHA-256 capture;
- CSV preflight validation before any Bronze write;
- bounded Polars batch processing for every row;
- a versioned Met CSV contract with all four outcome types;
- complete `SourceRecord` and `ContractResult` evidence generation;
- pre-resolution of the Bronze artifact URI for provenance;
- an application service and thin Prefect flow wrapper;
- deterministic unit tests; and
- a normal pytest integration test that downloads and processes the complete
  current Met snapshot once per test session.

This slice does not include:

- Met Collection API enrichment or incremental checks;
- canonical mapping or Silver writes;
- image downloads;
- live MinIO in automated tests;
- PostgreSQL ingestion-run persistence;
- Gold policy;
- AIC ingestion; or
- a command-line interface.

## Architecture

The data flow is:

`download → CSV preflight → Bronze target resolution → batched evidence → Bronze commit`

### Streaming HTTP boundary

`StreamingHttpClient` is a narrow infrastructure protocol. The production
implementation uses the project's pinned `urllib3` dependency and exposes a
streaming response rather than materialized response bytes.

`MetSnapshotDownloader` owns Met-specific acquisition behavior:

- stream fixed-size chunks into a run-scoped `.part` file;
- compute SHA-256 and byte count during the same pass;
- reject an empty response;
- stop when the existing 5 GiB single-object ceiling is crossed;
- validate `Content-Length` when present;
- capture the requested URL, final URL, ETag, Last-Modified, Content-Type, and
  retrieval time;
- atomically rename `.part` to the completed local artifact only after a
  successful transfer; and
- remove partial files before a retry or on terminal failure.

The downloader follows a bounded redirect chain because the official GitHub
URL resolves to GitHub's large-file media host. It never forwards application
credentials or object-store secrets.

Transport failures, HTTP 408, HTTP 429, and transient 5xx responses are passed
to the existing `RetryDecider`. Other 4xx responses stop immediately.
Acquisition failures remain typed and preserve status/attempt context.

### Met CSV preflight

`MetCsvPreflight` reads only the CSV header before Bronze storage begins. It
accepts a UTF-8 byte-order mark but otherwise requires valid UTF-8 and a
readable CSV header.

The v1 hard-required columns are:

- `Object ID`
- `Object Number`
- `Is Public Domain`
- `Department`
- `Object Name`
- `Title`
- `Artist Display Name`
- `Object Date`
- `Classification`
- `Link Resource`
- `Metadata Date`
- `Repository`

Missing required columns raise `MetCsvSchemaError` and prevent any Bronze
write. Additional columns are allowed and produce one bundle-level
`ValidationMessage` listing the added names. The message is persisted in the
completion manifest rather than duplicated across every row.

### Batched record evidence

`MetCsvEvidenceReader` uses `polars.read_csv_batched` with a configurable batch
size. Schema inference is disabled: source values remain strings or nulls, and
date/boolean coercion happens only inside validation. Pandera checks the
required batch columns and their string/null representation; it does not
normalize the source values.

Each DataFrame batch is converted to records and released before requesting
the next batch. The reader never collects the complete snapshot or evidence
stream.

Duplicate detection retains only a set of positive integer Object IDs. At the
Met's current scale this consumes tens of megabytes rather than materializing
hundreds of megabytes of row payload. An on-disk identity index is deferred
until observed scale justifies it.

### Application service

`MetSnapshotIngestionService` receives:

- the downloader;
- CSV preflight and evidence components;
- a `BronzeArtifactStore`;
- a fixed Met museum UUID supplied by composition;
- the configured Met snapshot URL;
- contract version;
- a clock and run-ID factory; and
- the existing `RetryDecider`.

The service:

1. creates a full-snapshot ingestion context;
2. downloads the artifact into a run-scoped temporary directory;
3. performs preflight validation;
4. creates `BronzeArtifact` with the computed checksum;
5. resolves a `BronzeBundleTarget`;
6. lazily builds record evidence using the target artifact URI;
7. calls `store_bundle`;
8. returns a typed result containing the run identity, receipt, acquisition
   metadata, and outcome counts; and
9. cleans the temporary directory after completion or failure.

The service is ordinary Python and remains directly testable. A thin Prefect
flow wrapper delegates to it and does not contain source or storage logic.

## Bronze Boundary Extension

Evidence requires the final raw artifact URI before `store_bundle` consumes the
evidence iterable. The application boundary therefore adds:

```python
class BronzeBundleTarget(BronzeModel):
    artifact_uri: str
    evidence_manifest_uri: str
    completion_manifest_uri: str


class BronzeArtifactStore(Protocol):
    def target_for(self, artifact: BronzeArtifact) -> BronzeBundleTarget: ...

    def store_bundle(
        self,
        artifact: BronzeArtifact,
        evidence: Iterable[BronzeRecordEvidence],
        *,
        contract_messages: tuple[ValidationMessage, ...] = (),
    ) -> BronzeBundleReceipt: ...
```

`MinioBronzeArtifactStore.target_for` uses the same deterministic key builder
as `store_bundle`. `store_bundle` verifies that it writes those same locations.
The completion manifest gains `contract_messages`, defaulting to an empty
tuple for backward compatibility.

A test-only counting store implements the same protocol, returns deterministic
fake targets, consumes evidence exactly once, and records counts without
retaining all records.

## Met v1 Row Contract

Every readable CSV row becomes exactly one evidence line.

### Accepted

A row is `accepted` when:

- `Object ID` is a positive integer;
- the ID has not appeared earlier in the snapshot; and
- object number, department, object name, title, link resource, metadata date,
  repository, and public-domain flag are present and syntactically valid.

### Accepted with warnings

A row with a valid unique Object ID is `accepted_with_warnings` when one or
more non-identity values are absent or malformed, including:

- blank object number, department, object name, title, link resource,
  metadata date, or repository;
- a blank or unrecognized `Is Public Domain` value; or
- a nonblank metadata date that cannot be parsed as an ISO-style date or
  timestamp.

The contract does not quarantine a work merely because it lacks artist,
classification, object date, public-domain status, or image-related metadata.
Blank artist, classification, and object-date values do not themselves create
warnings because incompleteness is valid in Silver. A blank or unrecognized
public-domain value does create a warning because rights certainty matters to
later Gold eligibility.

### Quarantined

A row is `quarantined` when `Object ID` is blank, nonnumeric, zero, or
negative. Its `SourceRecord.source_record_id` uses a deterministic
run-and-row fallback identity so the evidence remains addressable without
pretending the fallback is a museum authority identifier.

### Rejected

The first row for a positive Object ID is evaluated normally. Every later row
with the same Object ID is `rejected` with a `duplicate_object_id` message.
The duplicate row is still preserved by the unchanged artifact and receives
its own evidence entry.

Malformed CSV that cannot be segmented reliably into rows is an artifact-level
parse failure and stops the run before a completion marker. The contract does
not invent row evidence when record boundaries are unknowable.

## Provenance

Every `SourceRecord` contains:

- the injected Met museum UUID;
- `source=met`;
- the Met Object ID or deterministic invalid-ID fallback;
- contract version `1.0.0`;
- ingestion run ID;
- observation and retrieval timestamps;
- the requested official snapshot URL;
- `retrieval_status=cached`;
- the resolved Bronze artifact URI;
- full-snapshot acquisition context;
- the downloaded artifact SHA-256;
- original artist display text when present; and
- the row's contract outcome.

The unchanged CSV is the complete raw record source. The evidence sidecar does
not duplicate all row fields; it preserves the complete provenance and
contract result for each row.

## Result and Failure Semantics

Row-level warnings, quarantine, and rejection do not fail the run. The Bronze
bundle commits when:

- acquisition completed;
- preflight passed;
- every readable row produced evidence; and
- artifact, evidence, and `_SUCCESS.json` were written successfully.

The run fails without `_SUCCESS.json` for:

- terminal HTTP failure;
- incomplete or oversized download;
- unreadable encoding or CSV structure;
- missing required headers;
- evidence iteration failure; or
- Bronze storage failure.

Typed errors distinguish acquisition, CSV schema, CSV parse, and existing
storage failures. Prefect records the raised error; database-backed
`IngestionRun` lifecycle persistence remains a later slice.

## Testing

### Deterministic tests

Small local fixtures and fake transports cover:

- a successful streamed download;
- redirects and response metadata;
- retryable and terminal HTTP status handling;
- transport retry and partial-file cleanup;
- content-length mismatch, empty response, and size ceiling;
- required-header failure and extra-header warning;
- all four row outcomes;
- string/null preservation;
- bounded batches and one-pass evidence consumption;
- provenance and Bronze target URI;
- bundle-level contract messages;
- service cleanup on success and failure; and
- the thin Prefect delegation boundary.

The existing MinIO adapter tests remain responsible for actual Bronze upload
semantics. This slice does not duplicate those tests with a live object store.

### Live full-snapshot test

The normal pytest suite contains a session-scoped fixture for the official Met
URL. It processes the complete current artifact, not a sample.

The fixture stores the artifact and response metadata under
`.pytest_cache/met/`. On a later test session it performs a conditional request
using ETag and Last-Modified when available:

- HTTP 304 reuses the locally verified cached artifact;
- HTTP 200 replaces the cache through a `.part` file and atomic rename; and
- a missing, corrupt, or checksum-mismatched cache forces a full download.

The live test:

- verifies nonzero size and SHA-256;
- preflights the real header;
- streams every row through the contract;
- uses the counting Bronze store;
- asserts total outcomes equal total rows;
- asserts the total exceeds 400,000 rather than pinning a changing exact count;
- asserts at least one accepted or accepted-with-warnings row; and
- verifies evidence was consumed once without retaining the dataset.

Internet unavailability, upstream schema failure, or upstream download failure
fails the normal test suite by explicit product decision. The test does not
require MinIO.

## Configuration

Add source settings for:

- Met snapshot URL;
- HTTP connect/read timeout;
- download chunk size;
- Polars batch size; and
- optional test cache location override.

Defaults target the official Met source and conservative bounded sizes.
Credentials remain environment-only and unrelated to the public download.

## Success Criteria

The slice is complete when:

- production code can download the official complete CSV;
- the artifact is unchanged and checksum-addressed;
- preflight prevents missing-header uploads;
- every readable row produces exactly one evidence entry;
- outcome totals reconcile to row count;
- bundle-level schema warnings persist once;
- the production service composes with the real Bronze store;
- the Prefect wrapper delegates without embedding business logic;
- the full real snapshot passes through the live test; and
- the full repository verification remains clean.
