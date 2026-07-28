# Architecture

## Data flow

`acquire → contract → Bronze bundle commit → canonical map → Silver quality/history → Gold policy`

Every source observation remains addressable by ingestion run, source identity,
contract version, observation timestamp, source URL, retrieval status and
timestamp, raw object URI, typed acquisition context, and contract outcome.

## Ownership rules

- Domain code does not import source or infrastructure packages.
- Source packages do not define canonical entity variants.
- Application protocols depend on domain types, not concrete storage clients.
- Infrastructure adapters implement application protocols.
- Gold policy consumes explicit rights, image, and quality signals.
- Weak identity evidence creates review candidates and cannot auto-link.

## Image policy

When implemented, ingestion must retain metadata and source URLs for all
images. Cached image objects have their own retrieval state and object URI. An
external URL is never equivalent to a permanently stored image.

## History

Each acquisition run creates a new immutable Bronze bundle containing the
unchanged artifact, record evidence, and a completion marker written last.
Bronze does not append to or overwrite an existing object; a key collision is a
typed failure and a failed partial run remains uncommitted without
`_SUCCESS.json`. Silver history uses `valid_from`, `valid_to`, and `is_current`;
current rows never erase earlier observations.

## Bronze object-store boundary

The Bronze store accepts the owned `ConditionalObjectClient` protocol.
Production composition wraps `minio.Minio` in
`MinioConditionalObjectClient`; the raw SDK client does not satisfy the store
protocol. The wrapper uses `If-None-Match: *`, records expected SHA-256 and size
as object metadata, and is exact-pinned to `minio==7.2.20` and
`urllib3==2.7.0` because conditional headers require verified private SDK
request members.

Conditional uploads are limited to one 5 GiB (5,368,709,120-byte) PUT per
artifact, evidence manifest, or completion manifest. Implicit HTTP retries and
redirects are disabled. The wrapper instead owns three rewind-and-re-sign
attempts, retries HTTP 409 and transient server failures, and reconciles 412 or
ambiguous outcomes against stored checksum and size metadata.

Artifact and evidence payloads use sequential seekable spools. Their memory
threshold and temporary directory are configurable, and the artifact spool is
closed before evidence staging starts. Operators should provision the selected
spool directory for the larger payload—up to 5 GiB plus filesystem
overhead—rather than the sum of both payloads.

## Met full snapshot

The Met full-snapshot service streams the official weekly CSV into a temporary
artifact, performs required-header preflight, resolves its immutable Bronze
target, and emits one contract-evidence record per CSV row through bounded
Polars batches. Prefect delegates to the application service; production
composition supplies urllib3 and the conditional MinIO client.

Missing headers fail before storage. Added headers are persisted once in the
completion manifest. Row warnings, quarantine, and rejection remain committed
to Bronze and do not affect Silver/Gold policy.
