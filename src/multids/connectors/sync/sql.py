from itertools import chain, islice
from typing import Any, Iterable, Iterator, Mapping

try:
    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import Engine
except ImportError:  # pragma: no cover - exercised in minimal installations
    Engine = Any
    create_engine = None
    text = None

from ...contracts import BulkWriteResult, WriteResult
from ...error_mapping import map_sqlalchemy_error
from ...errors import ConnectorConnectionError, ConnectorDependencyError
from ...interfaces import SyncConnector, SyncConnectorContext
from ...reliability import RetryConfig, run_operation_sync
from ...telemetry import Telemetry

_BACKEND = "sql"


def _is_connection_error(exc: Exception) -> bool:
    return isinstance(exc, ConnectorConnectionError)


def _affected_rows(result: Any) -> int:
    """Return a statement rowcount, defaulting to 1 when it is unavailable."""
    try:
        rowcount = result.rowcount
        if rowcount is None or rowcount < 0:
            return 1
        return int(rowcount)
    except Exception:
        return 1


class SyncSQLConnectorBase(SyncConnectorContext, SyncConnector):
    """
    Base synchronous SQL connector using SQLAlchemy engine.

    The `url` should be a standard SQLAlchemy URL (e.g. `mysql+pymysql://...`).
    """

    # Identifier quote character; MySQL overrides this with a backtick.
    _quote_char = '"'

    telemetry: Telemetry = Telemetry()

    def __init__(
        self,
        url: str,
        *,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
        max_backoff: float = 30.0,
        telemetry: Telemetry | None = None,
    ):
        if create_engine is None:
            raise ConnectorDependencyError("SQLAlchemy is required; install it with `pip install multids[sync]`")
        self._url = url
        # Engine is created lazily so constructing a connector does not import the
        # optional DBAPI driver until the first real operation (see _get_engine).
        self._engine: Engine | None = None
        self.retry_config = RetryConfig(max_retries, backoff_factor, max_backoff)
        self.telemetry = telemetry or Telemetry()

    def _get_engine(self) -> Engine:
        """Return the engine, creating it (and importing the driver) on first use."""
        if self._engine is None:
            self._engine = create_engine(self._url, future=True)
        return self._engine

    def _quote_identifier(self, name: str) -> str:
        """Quote a single SQL identifier (never interpolate it raw)."""
        if not name:
            raise ValueError("identifier must not be empty")
        quote = self._quote_char
        return f"{quote}{name.replace(quote, quote + quote)}{quote}"

    def _insert_statement(self, table: str, columns: list[str]) -> Any:
        """Build a parameterised INSERT statement for the given columns."""
        column_list = ", ".join(self._quote_identifier(column) for column in columns)
        placeholders = ", ".join(f":{column}" for column in columns)
        return text(f"INSERT INTO {table} ({column_list}) VALUES ({placeholders})")

    def fetch_rows(self, query: str, **params: Any) -> Iterator[dict]:
        """Stream rows from a query as dictionaries."""
        try:
            with self._get_engine().connect() as conn:  # type: ignore
                # stream_results=True might be needed for server-side cursors depending on driver
                result = conn.execution_options(stream_results=True).execute(text(query), params)
                for row in result.mappings():
                    yield dict(row)
        except ConnectorDependencyError:
            raise
        except Exception as exc:
            raise map_sqlalchemy_error(exc, operation="sql.fetch_rows") from exc

    def execute(self, statement: str, **params: Any) -> WriteResult:
        """Execute a statement (INSERT/UPDATE/DELETE) and report affected rows."""

        def run() -> WriteResult:
            try:
                with self._get_engine().begin() as conn:  # type: ignore
                    result = conn.execute(text(statement), params)
                    return WriteResult(items_written=_affected_rows(result))
            except ConnectorDependencyError:
                raise
            except Exception as exc:
                raise map_sqlalchemy_error(exc, operation="sql.execute") from exc

        return run_operation_sync(
            run,
            config=self.retry_config,
            backend=_BACKEND,
            operation_name="sql.execute",
            telemetry=self.telemetry,
            retry_if=_is_connection_error,
        )

    def execute_statement(self, statement: str, **params: Any) -> WriteResult:
        """Execute a statement using the normalized write-result contract."""
        return self.execute(statement, **params)

    stream_records = fetch_rows

    def execute_many(self, statement: str, params_iter: Iterable[dict]) -> WriteResult:
        """Execute the same statement multiple times and report total affected rows."""

        def run() -> int:
            total = 0
            try:
                with self._get_engine().begin() as conn:  # type: ignore
                    for params in params_iter:
                        result = conn.execute(text(statement), params)
                        total += _affected_rows(result)
            except ConnectorDependencyError:
                raise
            except Exception as exc:
                raise map_sqlalchemy_error(exc, operation="sql.execute_many") from exc
            return total

        total = run_operation_sync(
            run,
            config=self.retry_config,
            backend=_BACKEND,
            operation_name="sql.execute_many",
            telemetry=self.telemetry,
            retry=False,
        )
        return WriteResult(items_written=total)

    def bulk_insert(
        self,
        table: str,
        rows: Iterable[Mapping[str, Any]],
        chunk_size: int = 1000,
    ) -> BulkWriteResult:
        """Insert mapping rows in batches and return a backend-neutral summary."""
        if chunk_size <= 0:
            raise ValueError("chunk_size must be greater than zero")

        rows_iter = iter(rows)
        try:
            first_row = next(rows_iter)
        except StopIteration:
            return BulkWriteResult(items_written=0)

        columns = list(first_row.keys())
        rows_iter = chain([first_row], rows_iter)
        stmt = self._insert_statement(table, columns)

        def run() -> int:
            written = 0
            try:
                with self._get_engine().begin() as conn:  # type: ignore
                    while True:
                        chunk = list(islice(rows_iter, chunk_size))
                        if not chunk:
                            break
                        conn.execute(stmt, chunk)
                        written += len(chunk)
            except ConnectorDependencyError:
                raise
            except Exception as exc:
                raise map_sqlalchemy_error(exc, operation="sql.bulk_insert") from exc
            return written

        # Bulk inserts are not idempotent; do not retry (would duplicate rows).
        written = run_operation_sync(
            run,
            config=self.retry_config,
            backend=_BACKEND,
            operation_name="sql.bulk_insert",
            telemetry=self.telemetry,
            resource=table,
            retry=False,
        )
        return BulkWriteResult(items_written=written)

    def write_records(
        self, target: str, records: Iterable[Mapping[str, Any]], *, batch_size: int = 1_000
    ) -> BulkWriteResult:
        """Insert mapping records under the backend-neutral record-writer contract."""
        if batch_size <= 0:
            raise ValueError("batch_size must be greater than zero")

        stmt: Any = None

        def run() -> int:
            nonlocal stmt
            stmt = None
            written = 0
            batch: list[dict[str, Any]] = []
            try:
                with self._get_engine().begin() as conn:  # type: ignore
                    for record in records:
                        if stmt is None:
                            stmt = self._insert_statement(target, list(record.keys()))
                        batch.append(dict(record))
                        if len(batch) >= batch_size:
                            conn.execute(stmt, batch)
                            written += len(batch)
                            batch = []
                    if batch and stmt is not None:
                        conn.execute(stmt, batch)
                        written += len(batch)
            except ConnectorDependencyError:
                raise
            except Exception as exc:
                raise map_sqlalchemy_error(exc, operation="sql.write_records") from exc
            return written

        # Record inserts are not idempotent; do not retry (would duplicate rows).
        written = run_operation_sync(
            run,
            config=self.retry_config,
            backend=_BACKEND,
            operation_name="sql.write_records",
            telemetry=self.telemetry,
            resource=target,
            retry=False,
        )
        return BulkWriteResult(items_written=written)

    def close(self) -> None:
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None

    def ping(self) -> bool:
        """Check connection by running a simple query."""
        try:
            with self._get_engine().connect() as conn:  # type: ignore
                conn.execute(text("SELECT 1"))
            return True
        except Exception:
            return False


class SyncMySQLConnector(SyncSQLConnectorBase):
    """
    Synchronous MySQL connector.
    """

    _quote_char = "`"
