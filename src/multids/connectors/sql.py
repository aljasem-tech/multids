from __future__ import annotations

import asyncio
from itertools import chain, islice
from typing import Any, AsyncIterator, Iterable, Mapping, TypeVar

from ..contracts import BulkWriteResult, WriteResult
from ..error_mapping import map_sqlalchemy_error
from ..errors import ConnectorConnectionError, ConnectorDependencyError
from ..interfaces import AsyncConnectorContext, Connector
from ..reliability import RetryConfig, run_operation
from ..telemetry import Telemetry

try:
    from sqlalchemy import text
except ImportError:  # pragma: no cover - exercised in minimal installations
    text = None

try:
    from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
except ImportError:  # pragma: no cover - exercised in minimal installations
    AsyncEngine = Any
    create_async_engine = None

T = TypeVar("T")

_BACKEND = "sql"


def _is_connection_error(exc: Exception) -> bool:
    return isinstance(exc, ConnectorConnectionError)


async def _aiter(records: Iterable[T] | AsyncIterator[T]) -> AsyncIterator[T]:
    """Normalize a sync or async record source into an async iterator."""
    if hasattr(records, "__aiter__"):
        async for record in records:  # type: ignore[union-attr]
            yield record
    else:
        for record in records:  # type: ignore[union-attr]
            yield record


def _affected_rows(result: Any) -> int:
    """Return a statement rowcount, defaulting to 1 when it is unavailable."""
    try:
        rowcount = result.rowcount
    except Exception:
        return 1
    if rowcount is None or rowcount < 0:
        return 1
    return int(rowcount)


class SQLConnectorBase(AsyncConnectorContext, Connector):
    """
    Base SQL connector using SQLAlchemy async engine.

    The `url` should be an async-enabled SQLAlchemy URL (for MySQL: `mysql+asyncmy://...`).
    """

    # Identifier quote character; MySQL overrides this with a backtick.
    _quote_char = '"'

    # Class-level default so instances created without __init__ still emit telemetry.
    telemetry: Telemetry = Telemetry()

    def __init__(
        self,
        url: str,
        *,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
        max_backoff: float = 30.0,
        timeout: float | None = 30.0,
        telemetry: Telemetry | None = None,
    ):
        self._url = url
        # Engine is created lazily so constructing a connector does not import the
        # optional DBAPI driver until the first real operation (see _get_engine).
        self._engine = None
        self.retry_config = RetryConfig(max_retries, backoff_factor, max_backoff, timeout)
        self.telemetry = telemetry or Telemetry()

    def _get_engine(self) -> AsyncEngine:
        """Return the async engine, creating it (and importing the driver) on first use."""
        if self._engine is None:
            if create_async_engine is None:
                raise ConnectorDependencyError(
                    "SQLAlchemy asyncio support is required; install it with `pip install multids[mysql]` "
                    "(requires SQLAlchemy and greenlet)"
                )
            self._engine = create_async_engine(self._url, future=True)
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

    async def fetch_rows(self, query: str, /, **params: Any) -> AsyncIterator[dict]:
        """Stream rows from a query as dictionaries (uses SQLAlchemy streaming API)."""
        engine = self._get_engine()

        async def connect() -> Any:
            try:
                return await engine.connect().__aenter__()
            except ConnectorDependencyError:
                raise
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                raise map_sqlalchemy_error(exc, operation="sql.fetch_rows.connect") from exc

        conn = await run_operation(
            connect,
            config=self.retry_config,
            backend=_BACKEND,
            operation_name="sql.fetch_rows.connect",
            telemetry=self.telemetry,
        )
        try:

            async def stream() -> Any:
                try:
                    return await conn.stream(text(query), params)
                except ConnectorDependencyError:
                    raise
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    raise map_sqlalchemy_error(exc, operation="sql.fetch_rows.stream") from exc

            result = await run_operation(
                stream,
                config=self.retry_config,
                backend=_BACKEND,
                operation_name="sql.fetch_rows.stream",
                telemetry=self.telemetry,
            )
            async for row in result.mappings():
                yield dict(row)
        finally:
            await conn.close()

    async def execute(self, statement: str, /, **params: Any) -> WriteResult:
        """Execute a statement (INSERT/UPDATE/DELETE) and report affected rows."""

        async def run() -> WriteResult:
            try:
                async with self._get_engine().begin() as conn:  # type: ignore
                    result = await conn.execute(text(statement), params)
                    return WriteResult(items_written=_affected_rows(result))
            except ConnectorDependencyError:
                raise
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                raise map_sqlalchemy_error(exc, operation="sql.execute") from exc

        return await run_operation(
            run,
            config=self.retry_config,
            backend=_BACKEND,
            operation_name="sql.execute",
            telemetry=self.telemetry,
            retry_if=_is_connection_error,
        )

    async def execute_statement(self, statement: str, /, **params: Any) -> WriteResult:
        """Execute a statement using the normalized write-result contract."""
        return await self.execute(statement, **params)

    stream_records = fetch_rows

    async def execute_many(self, statement: str, params_iter: Iterable[dict]) -> WriteResult:
        """
        Execute the same statement multiple times with different parameters.

        The parameter iterator is consumed once, so this operation is not retried
        (a retry would see an exhausted iterator). Drivers or more advanced
        techniques (COPY, bulk insert) may be added for performance.
        """
        total = 0

        async def run() -> int:
            nonlocal total
            total = 0
            try:
                async with self._get_engine().begin() as conn:  # type: ignore
                    for params in params_iter:
                        result = await conn.execute(text(statement), params)
                        total += _affected_rows(result)
            except ConnectorDependencyError:
                raise
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                raise map_sqlalchemy_error(exc, operation="sql.execute_many") from exc
            return total

        await run_operation(
            run,
            config=self.retry_config,
            backend=_BACKEND,
            operation_name="sql.execute_many",
            telemetry=self.telemetry,
            retry=False,
        )
        return WriteResult(items_written=total)

    async def bulk_insert(
        self,
        table: str,
        rows: Iterable[Mapping[str, Any]],
        chunk_size: int = 1000,
    ) -> BulkWriteResult:
        """Insert mapping rows in batches and return a backend-neutral summary."""
        if chunk_size <= 0:
            raise ValueError("chunk_size must be greater than zero")

        iterator = iter(rows)
        try:
            first_row = next(iterator)
        except StopIteration:
            return BulkWriteResult(items_written=0)

        columns = list(first_row.keys())
        rows_iter = chain([first_row], iterator)
        stmt = self._insert_statement(table, columns)

        written = 0

        async def run() -> int:
            nonlocal written
            written = 0
            try:
                async with self._get_engine().begin() as conn:  # type: ignore
                    while True:
                        chunk = list(islice(rows_iter, chunk_size))
                        if not chunk:
                            break
                        await conn.execute(stmt, chunk)
                        written += len(chunk)
            except ConnectorDependencyError:
                raise
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                raise map_sqlalchemy_error(exc, operation="sql.bulk_insert") from exc
            return written

        # Bulk inserts are not idempotent; do not retry (would duplicate rows).
        await run_operation(
            run,
            config=self.retry_config,
            backend=_BACKEND,
            operation_name="sql.bulk_insert",
            telemetry=self.telemetry,
            resource=table,
            retry=False,
        )
        return BulkWriteResult(items_written=written)

    async def write_records(
        self,
        target: str,
        records: Iterable[Mapping[str, Any]] | AsyncIterator[Mapping[str, Any]],
        *,
        batch_size: int = 1_000,
    ) -> BulkWriteResult:
        """Insert mapping records under the backend-neutral record-writer contract."""
        if batch_size <= 0:
            raise ValueError("batch_size must be greater than zero")

        stmt: Any = None
        written = 0
        batch: list[dict[str, Any]] = []

        async def run() -> int:
            nonlocal stmt, written, batch
            written = 0
            batch = []
            stmt = None
            try:
                async with self._get_engine().begin() as conn:  # type: ignore
                    async for record in _aiter(records):
                        if stmt is None:
                            stmt = self._insert_statement(target, list(record.keys()))
                        batch.append(dict(record))
                        if len(batch) >= batch_size:
                            await conn.execute(stmt, batch)
                            written += len(batch)
                            batch = []
                    if batch and stmt is not None:
                        await conn.execute(stmt, batch)
                        written += len(batch)
            except ConnectorDependencyError:
                raise
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                raise map_sqlalchemy_error(exc, operation="sql.write_records") from exc

            return written

        # Record inserts are not idempotent; do not retry (would duplicate rows).
        await run_operation(
            run,
            config=self.retry_config,
            backend=_BACKEND,
            operation_name="sql.write_records",
            telemetry=self.telemetry,
            resource=target,
            retry=False,
        )
        return BulkWriteResult(items_written=written)

    async def close(self) -> None:
        if self._engine is not None:
            await self._engine.dispose()
            self._engine = None

    async def ping(self) -> bool:
        """Check connection by running a simple query."""
        try:
            async with self._get_engine().connect() as conn:  # type: ignore
                await conn.execute(text("SELECT 1"))
            return True
        except Exception:
            return False


class MySQLConnector(SQLConnectorBase):
    """
    MySQL connector convenience wrapper.

    Example URL: `mysql+asyncmy://user:password@host:3306/dbname`
    """

    _quote_char = "`"
