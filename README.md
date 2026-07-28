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
```

Create a local `.env` file. It is ignored by Git and must never be committed.
Fill these three required values before starting local services:

- `EC_POSTGRES__PASSWORD`
- `EC_OBJECT_STORE__ACCESS_KEY`
- `EC_OBJECT_STORE__SECRET_KEY`

Additional optional settings are defined by
`src/evidence_cartographer/infrastructure/settings.py`.

```bash
docker compose --env-file .env -f infra/compose.yaml up -d
```

## Checks

```bash
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run mypy src
docker compose --env-file .env -f infra/compose.yaml config --quiet
```

## Package boundaries

- `domain`: canonical entities and stable value types.
- `application`: pipeline ports, contracts, and resolution evidence.
- `sources`: isolated Met and AIC source descriptors and future adapters.
- `infrastructure`: settings, logging, and future external-system adapters.
- `orchestration`: Prefect flow construction and schedule defaults.

See `docs/architecture.md` for the data-flow and ownership rules.
