# Contributing to multids

First off, thanks for taking the time to contribute!

The following is a set of guidelines for contributing to `multids`. These are mostly guidelines, not rules. Use your
best judgment, and feel free to propose changes to this document in a pull request.

## Development Setup

1. **Clone the repository**:
   ```bash
   git clone https://github.com/aljasem-tech/multids.git
   cd multids
   ```

2. **Install Poetry**:
   This project uses [Poetry](https://python-poetry.org/) for dependency management.
   ```bash
   pip install poetry
   ```

3. **Install dependencies**:
   ```bash
   poetry install --all-extras
   ```

4. **Install pre-commit hooks**:
   We use `pre-commit` to ensure code quality.
   ```bash
   poetry run pre-commit install
   ```

## Running Tests

We use `pytest` for testing.

```bash
poetry run pytest
```

To run integration tests (requires local services like OpenSearch):

```bash
poetry run pytest tests/integration
```

## Coding Style

- **Code Formatting**: We use `black` for code formatting.
- **Linting**: We use `ruff` and `isort`.
- **Type Checking**: Type hints are encouraged.

Most of these are checked automatically by `pre-commit`. You can run them manually:

```bash
poetry run pre-commit run --all-files
```

## Pull Request Process

1. Fork the repo and create your branch from `main`.
2. If you've added code that should be tested, add tests.
3. If you've changed APIs, update the documentation.
4. Ensure the test suite passes.
5. Make sure your code lints.
6. Issue that pull request!

## License

By contributing, you agree that your contributions will be licensed under its MIT License.
