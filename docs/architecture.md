# Architecture

## Data flow

`acquire → contract → Bronze append → canonical map → Silver quality/history → Gold policy`

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

Raw source observations are append-only. Silver history uses `valid_from`,
`valid_to`, and `is_current`; current rows never erase earlier observations.
