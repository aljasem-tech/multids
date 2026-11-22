# Contributing to multids

Thanks for taking the time to contribute! This document covers development setup, testing, and the pull request
process.

## Development setup

```bash
git clone https://github.com/aljasem-tech/multids.git
cd multids

# Create a virtual environment (Python 3.11+)
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate

# Install in editable mode with dev dependencies
pip install -e '.[all]'
pip install pytest pytest-asyncio mypy ruff build twine pre-commit
```

Install the pre-commit hooks:

```bash
pre-commit install
```

## Running tests

```bash
# Unit and contract tests (no live services required)
pytest

# Integration tests (require live services; they skip cleanly if unset)
pytest tests/integration
```

## Code quality

The project uses `ruff` for linting, `black` for formatting, `isort` for import sorting, and `mypy` for type
checking. All are enforced by pre-commit and in CI:

```bash
pre-commit run --all-files

# Or run individually:
ruff check .
mypy src/multids
```

## Pull request process

1. Fork the repo and create your branch from `main`.
2. Add tests for any new behaviour.
3. Update documentation (`README.md`, `docs/`, docstrings) when you change public APIs.
4. Ensure `pytest`, `ruff`, and `mypy` all pass.
5. Open a pull request.

## License

By contributing, you agree that your contributions are licensed under the MIT License.
