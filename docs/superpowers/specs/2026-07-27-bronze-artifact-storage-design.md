# Bronze Artifact Storage Design

## Purpose

This slice establishes the first concrete Bronze persistence boundary for
Evidence Cartographer. It stores complete acquired Met and AIC source artifacts
in MinIO without changing their bytes, alongside record-level provenance and
contract evidence.

The implementation remains infrastructure boilerplate. It does not download
museum data, parse CSV or JSON, bootstrap buckets, normalize records, or run a
live MinIO integration test.

## Scope

The slice will:

- define typed application models for a source artifact, record evidence, and
  a completed Bronze bundle;
- define a `BronzeArtifactStore` application port;
- implement the port with the MinIO Python SDK;
- stream local artifacts and record-evidence manifests up to the documented
  5 GiB conditional single-PUT ceiling;
- use deterministic, partitioned object keys;
- preserve full `SourceRecord` provenance and `ContractResult` messages;
- calculate and record SHA-256 checksums;
- write a completion marker only after all required objects succeed;
- remove `.env.example` from Git while retaining a local ignored `.env`;
- document required environment-variable names in README.

The slice will not:

- implement Met or AIC acquisition clients;
- implement snapshot or API parsers;
- store canonical Silver entities;
- choose quality or Gold publication policy;
- create MinIO buckets;
- start Docker services;
- overwrite or delete existing Bronze objects;
- test against a live object store.

## Bronze Acquisition Bundle

Each ingestion run writes one immutable acquisition bundle containing three
objects:

1. The original source artifact, byte-for-byte unchanged.
2. A streamed NDJSON sidecar containing record provenance and contract results.
3. A small JSON completion manifest written last as `_SUCCESS.json`.

The completion manifest is the commit marker for the bundle. A prefix without
`_SUCCESS.json` is incomplete and must not be treated as a valid Bronze
acquisition.

## Object-Key Layout

Object keys use the following deterministic layout:

```text
{bronze_prefix}/{source}/year=YYYY/month=MM/day=DD/run={run_id}/
  source.{extension}
  records.manifest.jsonl
  _SUCCESS.json
```

The partition date is the artifact retrieval date in UTC. `source` is the
stable `SourceName` value, and `run_id` is the ingestion-run UUID.

The extension is validated as a safe suffix without a path separator. Object
keys are constructed in a focused, infrastructure-independent helper so they
can be tested without MinIO.

## Application Models

### `BronzeArtifact`

Describes the acquired source file:

- source;
- ingestion-run ID;
- retrieval timestamp;
- source URL;
- media type;
- file extension;
- contract version;
- local `Path`;
- optional expected SHA-256 checksum.

The local path belongs to the upload command, not to canonical provenance. It
is never written into the completion manifest.

### `BronzeRecordEvidence`

Contains:

- the complete immutable `SourceRecord`, including source-record identity;
- the complete `ContractResult`.

This retains accepted-with-warning details and quarantine/rejection evidence.
It does not duplicate the raw source payload, which remains in the original
artifact.

### `BronzeBundleReceipt`

Returns:

- artifact object URI, byte size, and SHA-256;
- evidence-manifest object URI, byte size, and SHA-256;
- completion-manifest object URI;
- ingestion-run ID and source.

### `BronzeArtifactStore`

The application port accepts one `BronzeArtifact` plus a lazy
`Iterable[BronzeRecordEvidence]` and returns a `BronzeBundleReceipt`.

The iterable must not be materialized in memory by the storage adapter.

This port replaces the existing per-record `BronzeWriter` protocol. Its
contract evidence moves into the bundle sidecar so the original acquisition
artifact and all derived validation evidence commit together. Callers and
protocol tests will migrate to `BronzeArtifactStore`; two competing Bronze
persistence boundaries will not remain.

## MinIO Adapter

The Bronze store depends on the owned `ConditionalObjectClient` structural
protocol. A `MinioConditionalObjectClient` wraps the real MinIO SDK, while
store tests supply an in-memory fake with the same truthful interface. A raw
`minio.Minio` is deliberately not accepted by the store because the public SDK
does not expose conditional create headers.

The owned wrapper is the only module permitted to use the MinIO SDK's private
request members. Its compatibility contract is exact-pinned to
`minio==7.2.20` and `urllib3==2.7.0` and covered by real-client composition and
request-contract tests.

The adapter:

1. validates that the artifact is a regular file;
2. computes its SHA-256 and byte size by streaming;
3. compares the computed hash with the expected hash when supplied;
4. derives all deterministic object keys;
5. checks that none of the target keys already exists;
6. conditionally creates the original artifact with `If-None-Match: *`;
7. incrementally serializes record evidence as canonical NDJSON;
8. conditionally creates the NDJSON sidecar;
9. constructs a deterministic completion manifest with object locations,
   hashes, sizes, contract version, retrieval metadata, and outcome counts;
10. conditionally creates `_SUCCESS.json` last;
11. returns the typed receipt.

The adapter does not own credentials or read settings directly. Its constructor
receives the owned conditional client, bucket name, Bronze prefix, spool memory
threshold, and optional spool directory from composition code.

### Conditional single-PUT contract

All three bundle payloads use one conditional HTTP PUT. The supported maximum
is 5 GiB (5,368,709,120 bytes) per object; larger artifacts, evidence
manifests, or completion manifests fail with `SinglePutSizeLimitError`.
Multipart upload is intentionally outside this boilerplate slice.

Each request records the expected SHA-256 and byte size in object user metadata.
The wrapper disables urllib3's implicit retries and redirects, then owns a
bounded three-attempt loop. Every attempt rewinds the staged stream and is
signed afresh. HTTP 409 and transient server responses are retried. A 412,
ambiguous transport failure, or ambiguous server response is reconciled with a
HEAD request: matching stored checksum and size means the earlier attempt
committed; mismatching metadata is an object collision; absence permits another
bounded attempt. Typed diagnostics retain HTTP status, S3 code, message, request
ID, and host ID where supplied.

## Serialization

Record evidence is serialized one item per line as UTF-8 JSON. Serialization
uses stable key ordering, compact separators, and Pydantic JSON-mode output so
UUIDs, enums, paths, and timestamps have consistent representations. Every
line ends with a newline.

The completion manifest uses the same canonical JSON settings. It includes
outcome counts for the four contract statuses and a total record count.

The artifact snapshot and NDJSON stream are each spooled through a bounded
temporary file so the object client receives a seekable file-like object and
known content length without retaining a complete payload in memory. The
in-memory threshold and temporary directory are configurable.

The artifact spool is closed immediately after its upload and before evidence
iteration begins. Temporary-disk capacity therefore needs to accommodate the
larger of the artifact or evidence manifest, not both simultaneously. With the
single-PUT contract, allow up to 5 GiB plus filesystem overhead in the selected
spool directory. Compression is deferred until real dataset measurements
justify it.

## Immutability and Failure Semantics

The adapter performs advisory existence checks before uploading and every write
also uses a race-safe conditional create. Any existing target key causes an
`ObjectAlreadyExistsError`; the adapter never overwrites, appends to, or deletes
an object. Bronze "append-only" means that new runs create new immutable bundle
prefixes, not that records are appended to an existing object.

The adapter never deletes objects automatically. If artifact or sidecar upload
succeeds but a later step fails, the prefix remains without `_SUCCESS.json`.
The failed prefix is auditable and cannot be mistaken for a committed bundle.
A retry uses a new ingestion-run ID.

Bucket versioning and object lock remain deployment concerns; race-safe
create-only writes are enforced by this adapter.

## Errors

Storage failures are expressed through focused subclasses of the existing
`StorageError`:

- `ArtifactNotFoundError`;
- `ArtifactIntegrityError`;
- `ArtifactStagingError`;
- `EvidenceStagingError`;
- `SinglePutSizeLimitError`;
- `ObjectAlreadyExistsError`;
- `ManifestSerializationError`;
- `ObjectStoreError`.

MinIO SDK and conditional-transport exceptions are chained into
`ObjectStoreError`, retaining typed conditional diagnostics. Staging,
validation, and serialization errors preserve their original cause. No error
is silently converted into a successful or quarantined record.

## Environment-File Policy

`.env.example` will be deleted from the repository. The real `.env` remains a
local, Git-ignored file and will not be read, displayed, modified, or committed
by this change.

README will document the required environment-variable names without supplying
values or a copyable environment template. Existing `.gitignore` coverage for
root and nested `.env` files remains in place.

Configuration tests will verify:

- `.env.example` is absent;
- `.env` is ignored;
- `.env` is not tracked by Git;
- README names all required credential variables.

## Testing

Store tests use synthetic files and an in-memory fake implementing the owned
conditional-object protocol. Contract tests compose the owned wrapper around a
real MinIO SDK client with a recording HTTP transport; they do not contact a
live service.

Tests cover:

- deterministic object-key layout;
- unsafe extension rejection;
- byte-for-byte artifact preservation;
- artifact and sidecar byte sizes;
- SHA-256 generation and expected-hash validation;
- stable NDJSON serialization;
- preservation of full provenance and contract messages;
- outcome and total counts in `_SUCCESS.json`;
- upload ordering with `_SUCCESS.json` last;
- the 5 GiB ceiling for artifact, evidence, and completion payloads;
- disabled transport retries/redirects and bounded rewind/re-sign retries;
- 409 retry and metadata reconciliation after 412 or ambiguous outcomes;
- typed S3 diagnostics and exact private-API dependency pins;
- lazy evidence iteration;
- existing-object rejection before upload;
- MinIO failure translation;
- absence of `_SUCCESS.json` after any earlier failure;
- typed receipt values;
- configurable spool location and memory threshold;
- typed artifact/evidence staging failures and sequential spool lifetime;
- the environment-file policy above.

The full existing test, Ruff, formatting, mypy, dbt parse, and Compose
configuration suites must remain clean.

## Acceptance Criteria

The slice is complete when:

1. A synthetic source artifact and lazy evidence iterable can be stored through
   `BronzeArtifactStore`.
2. The fake client receives the unchanged artifact, canonical NDJSON evidence,
   and `_SUCCESS.json` in that order.
3. Checksums, sizes, object URIs, source, run ID, contract version, and outcome
   counts are correct.
4. Existing keys, integrity mismatches, serialization failures, and MinIO
   failures produce typed errors.
5. Partial uploads never contain `_SUCCESS.json`.
6. No implementation loads the complete source artifact or complete evidence
   iterable into memory; each individual stored payload is no larger than
   5 GiB.
7. `.env.example` is removed while local `.env` remains ignored and untracked.
8. No acquisition, parsing, mapping, bucket-bootstrap, or live-service behavior
   is introduced.
9. The real MinIO SDK composes only through the owned conditional client, with
   exact compatible dependency pins and retry/reconciliation contract tests.
