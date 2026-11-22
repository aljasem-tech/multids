from __future__ import annotations

import asyncio
from typing import Any, AsyncIterator, Iterable, Mapping

from ..contracts import BulkWriteResult, WriteResult
from ..error_mapping import map_postgres_error
from ..errors import ConnectorConnectionError, ConnectorDependencyError
from ..interfaces import AsyncConnectorContext, Connector
from ..reliability import RetryConfig, run_operation
from ..telemetry import Telemetry

try:
    import asyncpg
except ImportError:  # pragma: no cover - exercised in minimal installations
    asyncpg = None

_BACKEND = "postgres"


def _is_connection_error(exc: Exception) -> bool:
    return isinstance(exc, ConnectorConnectionError)


async def _aiter(records: Iterable[Any] | AsyncIterator[Any]) -> AsyncIterator[Any]:
    """Normalize a sync or async record source into an async iterator."""
    if hasattr(records, "__aiter__"):
        async for record in records:  # type: ignore[union-attr]
            yield record
    else:
        for record in records:  # type: ignore[union-attr]
            yield record


def _affected_rows(result: str) -> int:
    """Parse asyncpg's command-tag string (e.g. 'INSERT 0 5') into a row count."""
    if not result:
        return 0
    parts = result.split()
    if parts and parts[-1].isdigit():
        return int(parts[-1])
    return 1


class PostgreSQLConnector(AsyncConnectorContext, Connector):
    """Async PostgreSQL connector using the asyncpg driver directly.

    Supports standard record operations (``fetch_rows``, ``execute``,
    ``bulk_insert``, ``write_records``) plus a native COPY-style bulk insert
    via :meth:`copy_records_to_table` for high-throughput ingestion.

    Parameters are positional (``$1``, ``$2``, ...) as expected by asyncpg.
    Pass them as keyword arguments in the same order they appear in the query.

    Example::

        conn = PostgreSQLConnector("postgresql://user:pass@localhost:5432/db")
        rows = [{"id": 1, "name": "alice"}]
        await conn.bulk_insert("users", rows)          # uses COPY internally
        async for row in conn.fetch_rows("SELECT * FROM users WHERE id > $1", min_id=0):
            print(row)
        await conn.close()
    """

    telemetry: Telemetry = Telemetry()

    def __init__(
        self,
        dsn: str,
        *,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
        max_backoff: float = 30.0,
        timeout: float | None = 30.0,
        telemetry: Telemetry | None = None,
    ):
        self._dsn = dsn
        self._pool: Any = None
        self.retry_config = RetryConfig(max_retries, backoff_factor, max_backoff, timeout)
        self.telemetry = telemetry or Telemetry()

    async def _get_pool(self) -> Any:
        """Return the asyncpg pool, creating it (and importing asyncpg) on first use."""
        if self._pool is None:
            if asyncpg is None:
                raise ConnectorDependencyError("asyncpg is required; install it with `pip install multids[postgres]`")
            self._pool = await asyncpg.create_pool(self._dsn)
        return self._pool

    async def fetch_rows(self, query: str, /, **params: Any) -> AsyncIterator[dict]:
        """Stream rows from a query as dictionaries.

        asyncpg uses ``$1``, ``$2`` ... placeholders. Pass parameters as
        keyword arguments in the same order they appear in the query.
        """

        async def run() -> list:
            try:
                pool = await self._get_pool()
                async with pool.acquire() as conn:
                    return await conn.fetch(query, *params.values())
            except ConnectorDependencyError:
                raise
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                raise map_postgres_error(exc, operation="postgres.fetch_rows") from exc

        rows = await run_operation(
            run,
            config=self.retry_config,
            backend=_BACKEND,
            operation_name="postgres.fetch_rows",
            telemetry=self.telemetry,
            retry_if=_is_connection_error,
        )
        for row in rows:
            yield dict(row)

    async def execute(self, statement: str, /, **params: Any) -> WriteResult:
        """Execute a statement (INSERT/UPDATE/DELETE/DDL) and report affected rows."""

        async def run() -> WriteResult:
            try:
                pool = await self._get_pool()
                async with pool.acquire() as conn:
                    async with conn.transaction():
                        result = await conn.execute(statement, *params.values())
                    return WriteResult(items_written=_affected_rows(result))
            except ConnectorDependencyError:
                raise
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                raise map_postgres_error(exc, operation="postgres.execute") from exc

        return await run_operation(
            run,
            config=self.retry_config,
            backend=_BACKEND,
            operation_name="postgres.execute",
            telemetry=self.telemetry,
            retry_if=_is_connection_error,
        )

    async def bulk_insert(
        self,
        table: str,
        rows: Iterable[Mapping[str, Any]],
        chunk_size: int = 1000,
    ) -> BulkWriteResult:
        """Insert mapping rows using the native COPY protocol for speed.

        Falls back to a multi-row INSERT if ``copy_records_to_table`` is
        unavailable. The ``chunk_size`` parameter is accepted for API
        compatibility but COPY handles all rows in a single operation.
        """
        return await self.copy_records_to_table(table, rows)

    async def copy_records_to_table(
        self,
        table: str,
        records: Iterable[Mapping[str, Any]] | AsyncIterator[Mapping[str, Any]],
        *,
        columns: list[str] | None = None,
    ) -> BulkWriteResult:
        """Bulk-insert records using PostgreSQL's native COPY protocol.

        This is the fastest way to load large datasets into PostgreSQL.
        Records are collected into memory, so for extremely large datasets
        consider streaming via ``write_records`` with a smaller batch size.

        Parameters
        ----------
        table:
            Target table name (optionally schema-qualified, e.g.
            ``"myschema.users"``).
        records:
            Iterable or async iterator of mapping records.
        columns:
            Column names to insert. If ``None``, inferred from the first
            record's keys.
        """

        async def run() -> int:
            records_list: list[dict] = []
            async for record in _aiter(records):
                records_list.append(dict(record))
            if not records_list:
                return 0

            cols = columns or list(records_list[0].keys())
            tuples = [tuple(r.get(col) for col in cols) for r in records_list]

            try:
                pool = await self._get_pool()
                async with pool.acquire() as conn:
                    await conn.copy_records_to_table(table, records=tuples, columns=cols)
                return len(records_list)
            except ConnectorDependencyError:
                raise
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                raise map_postgres_error(exc, operation="postgres.copy_records_to_table") from exc

        # COPY is not idempotent; do not retry (would duplicate rows).
        written = await run_operation(
            run,
            config=self.retry_config,
            backend=_BACKEND,
            operation_name="postgres.copy_records_to_table",
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
        """Insert mapping records under the backend-neutral record-writer contract.

        Uses the COPY protocol via :meth:`copy_records_to_table` for
        performance. The ``batch_size`` parameter is accepted for API
        compatibility.
        """
        if batch_size <= 0:
            raise ValueError("batch_size must be greater than zero")
        return await self.copy_records_to_table(target, records)

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def ping(self) -> bool:
        """Check connection by running a simple query."""
        try:
            pool = await self._get_pool()
            async with pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            return True
        except Exception:
            return False
