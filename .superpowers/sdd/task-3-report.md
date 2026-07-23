# Task 3 report: application contracts and resolution evidence

## Scope

Created only the generic application-boundary package, the prescribed tests, and
this task report. No source mappings, publication policy, I/O, fuzzy matching,
or changes to Task 2 domain files were made.

## TDD evidence

### RED

Command (the required sandbox-safe substitution for `uv run pytest
tests/application -q`):

```text
.venv/bin/pytest tests/application -q
```

Result: exit 2, with collection errors for both application test files:

```text
ModuleNotFoundError: No module named 'evidence_cartographer.application'
```

### GREEN

Command:

```text
.venv/bin/pytest tests/application -q
```

Result: exit 0, `3 passed in 0.03s`.

## Verification

- `.venv/bin/pytest -q` (substituting for `uv run pytest -q`): exit 0,
  `6 passed in 0.04s`.
- `.venv/bin/ruff check src tests`: exit 0, `All checks passed!`.
- `.venv/bin/mypy src/evidence_cartographer/domain
  src/evidence_cartographer/application`: exit 0, `Success: no issues found in
  7 source files`.

Mypy initially reported `prop-decorator` for Pydantic's required
`@computed_field`/`@property` pairing. The installed Pydantic implementation
documents this known mypy limitation and prescribes
`# type: ignore[prop-decorator]` on `@computed_field`; the code uses that exact,
line-local suppression. Re-running mypy succeeded.

## Files

- `src/evidence_cartographer/application/__init__.py`
- `src/evidence_cartographer/application/contracts.py`
- `src/evidence_cartographer/application/ports.py`
- `src/evidence_cartographer/application/resolution.py`
- `tests/application/test_contracts.py`
- `tests/application/test_resolution.py`
- `.superpowers/sdd/task-3-report.md`

## Self-review

- All Pydantic data models are frozen and forbid unknown fields.
- `GoldEligibilitySignals` carries evidence only; it exposes no publication
  decision or eligibility policy.
- Pipeline ports are structural protocols and contain no adapter or I/O logic.
- Auto-linking requires non-empty evidence and every evidence item to be a
  strong identifier; weak evidence remains unreviewed by default.
- The changes depend only on the Task 2 domain enums and models.

## Concerns

None. The `prop-decorator` suppression is intentional and is constrained to
the Pydantic-decorated computed property documented above.
