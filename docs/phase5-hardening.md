# Phase 5 — Distribution, documentation, and release controls

Phase 5 makes installation, upgrades, and releases predictable. This page records what changed, how it is verified, and what remains before the `1.0.0` release candidate.

## Type checking

A `[tool.mypy]` configuration was added to `pyproject.toml`:

- `python_version = "3.11"` (lowest supported interpreter)
- `files = ["src/multids"]` (scope)
- `ignore_missing_imports = true` (optional deps are not installed in the type-check environment)
- `check_untyped_defs = true` (bodies of unannotated functions are checked)
- `no_strict_optional = true` (matches runtime `None`-tolerance)
- `disable_error_code = ["import-untyped", "attr-defined", "assignment"]` (suppresses expected noise from optional imports and dynamic attribute access)

The CI `test` job runs `mypy src/multids` after the Ruff lint step. The check must pass with zero errors.

### mypy fixes applied

| File | Issue | Fix |
| --- | --- | --- |
| `connectors/mssql.py` | `connect_pool` assigned `await run_operation(...)` to `self._pool`, which mypy inferred as `None` from `__init__` | Use a local `pool` variable, then assign to `self._pool` |
| `connectors/sql.py` | `AsyncEngine = Any` fallback on ImportError | `# type: ignore[misc]` |
| `connectors/sync/sql.py` | `Engine = Any` fallback on ImportError | `# type: ignore[misc]` |
| `connectors/sync/sql.py` | `text("SELECT 1")` return-value annotation | `# type: ignore[func-returns-value]` |
| `connectors/sql.py` | `connect()` did not return the awaited connection | `return await engine.connect()` |

## Ruff configuration

The deprecated top-level `[tool.ruff]` `select` was migrated to `[tool.ruff.lint]` (already present in `pyproject.toml`). The rule set is `E`, `F`, `W`, `C90`, `N`.

## Dependency fix: greenlet

SQLAlchemy's asyncio module requires `greenlet`. It was listed in the `mysql` extra but never declared as a dependency, so `pip install .[mysql]` did not pull it and CI failed with:

```
ImportError: The SQLAlchemy asyncio module requires that the Python 'greenlet' library is installed.
```

Fix: `greenlet = { version = ">=2.0", optional = true }` added to `[tool.poetry.dependencies]`, and `greenlet` added to the `mysql` and `all` extras.

## Build and artifact verification

```bash
python -m build          # produces dist/multids-0.2.1.tar.gz and .whl
twine check dist/*       # PASSED for all artifacts
```

Verified in the wheel:

- `multids/py.typed` is included (PEP 561 marker).
- `greenlet` appears in `Requires-Dist` for the `mysql` and `all` extras.
- All extras (`ai`, `all`, `local`, `mysql`, `opensearch`, `s3`, `sqlserver`, `sync`) are declared in `Provides-Extra`.

## CI workflow changes

- `ci.yml` `test` job: added `Type-check with mypy` step (`pip install mypy==1.8.0 && mypy src/multids`).
- `real-integration.yml` and `benchmarks.yml`: replaced the withdrawn `minio/minio:latest` Docker image with `pgsty/silo:latest` (drop-in MinIO fork; same entrypoint, env vars, and `/minio/health/live` endpoint).

## Remaining Phase 5 items (pre-1.0)

- [ ] Execute representative README and documentation examples in CI (beyond syntax-only validation).
- [ ] Test minimal installs, each extra, wheels, and source distributions in clean virtual environments.
- [ ] Commit a regenerated `poetry.lock`.
- [ ] Publish a support matrix and compatibility policy; finalize migration / deprecation guidance.
- [ ] Build and publish only from a protected version tag, with artifact checks, changelog verification, and dependency / security scanning (`pip-audit`).
