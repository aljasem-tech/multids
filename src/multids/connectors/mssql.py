from __future__ import annotations

import asyncio
from typing import Any, AsyncIterator, Dict, Iterable, List, Mapping, Optional, Sequence

from ..contracts import BulkWriteResult, WriteResult
from ..error_mapping import map_pyodbc_error
from ..errors import ConnectorConnectionError, ConnectorDependencyError
from ..interfaces import AsyncConnectorContext, Connector
from ..reliability import RetryConfig, run_operation
from ..telemetry import Telemetry

_BACKEND = "mssql"


def _is_connection_error(exc: Exception) -> bool:
    return isinstance(exc, ConnectorConnectionError)


class MSSQLConnector(AsyncConnectorContext, Connector):
    """
    Async SQL Server connector using aioodbc.

    Notes:
    - Requires an ODBC driver for SQL Server on the host (e.g. Microsoft ODBC Driver for SQL Server).
    - Install `aioodbc` as an extra (e.g. `pip install multids[sqlserver]`).
    """

    telemetry: Telemetry = Telemetry()

    def __init__(
        self,
        dsn: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
        max_backoff: float = 30.0,
        timeout: float | None = 30.0,
        telemetry: Telemetry | None = None,
    ):
        self._dsn = dsn
        self._user = user
        self._password = password
        self._host = host
        self._port = port
        self._database = database
        self._loop = loop
        self._pool = None
        self.retry_config = RetryConfig(max_retries, backoff_factor, max_backoff, timeout)
        self.telemetry = telemetry or Telemetry()

        # lazy import
        try:
            import aioodbc as _aioodbc

            self._aioodbc = _aioodbc
        except Exception:
            self._aioodbc = None

    async def connect_pool(self, minsize: int = 1, maxsize: int = 10) -> None:
        if self._aioodbc is None:
            raise ConnectorDependencyError("aioodbc is required; install multids[sqlserver]")

        conn_str = self._dsn
        if conn_str is None:
            # build a simple DSN if not provided
            parts = []
            if self._host:
                parts.append(f"Server={self._host}")
            if self._port:
                parts.append(f"Port={self._port}")
            if self._database:
                parts.append(f"Database={self._database}")
            if self._user:
                parts.append(f"UID={self._user}")
            if self._password:
                parts.append(f"PWD={self._password}")
            conn_str = ";".join(parts)

        def run() -> Any:
            return self._aioodbc.create_pool(dsn=conn_str, minsize=minsize, maxsize=maxsize, loop=self._loop)

        self._pool = await run_operation(
            run,
            config=self.retry_config,
            backend=_BACKEND,
            operation_name="mssql.connect_pool",
            telemetry=self.telemetry,
        )

    async def close(self) -> None:
        if self._pool is not None:
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None

    async def ping(self) -> bool:
        """Check connection by running a simple query."""
        try:
            if self._pool is None:
                await self.connect_pool()
            async with self._pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT 1")
            return True
        except Exception:
            return False

    async def execute(self, sql: str, params: Optional[Sequence[Any]] = None) -> WriteResult:
        if self._pool is None:
            await self.connect_pool()

        async def run() -> WriteResult:
            try:
                async with self._pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute(sql, params or [])
                        rowcount = getattr(cur, "rowcount", -1)
                        return WriteResult(items_written=rowcount if rowcount and rowcount > 0 else 1)
            except ConnectorDependencyError:
                raise
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                raise map_pyodbc_error(exc, operation="mssql.execute") from exc

        return await run_operation(
            run,
            config=self.retry_config,
            backend=_BACKEND,
            operation_name="mssql.execute",
            telemetry=self.telemetry,
            retry_if=_is_connection_error,
        )

    async def execute_statement(self, statement: str, params: Optional[Sequence[Any]] = None) -> WriteResult:
        return await self.execute(statement, params)

    async def fetch_rows(self, sql: str, params: Optional[Sequence[Any]] = None) -> List[Dict[str, Any]]:
        if self._pool is None:
            await self.connect_pool()

        async def run() -> List[Dict[str, Any]]:
            try:
                async with self._pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute(sql, params or [])
                        cols = [c[0] for c in cur.description]
                        rows = []
                        async for r in cur:
                            rows.append({k: v for k, v in zip(cols, r)})
                        return rows
            except ConnectorDependencyError:
                raise
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                raise map_pyodbc_error(exc, operation="mssql.fetch_rows") from exc

        return await run_operation(
            run,
            config=self.retry_config,
            backend=_BACKEND,
            operation_name="mssql.fetch_rows",
            telemetry=self.telemetry,
            retry_if=_is_connection_error,
        )

    async def fetch_iter(self, sql: str, params: Optional[Sequence[Any]] = None) -> AsyncIterator[Dict[str, Any]]:
        if self._pool is None:
            await self.connect_pool()
        try:
            async with self._pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(sql, params or [])
                    cols = [c[0] for c in cur.description]
                    async for r in cur:
                        yield {k: v for k, v in zip(cols, r)}
        except ConnectorDependencyError:
            raise
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise map_pyodbc_error(exc, operation="mssql.fetch_iter") from exc

    stream_records = fetch_iter

    async def bulk_insert(
        self, table: str, columns: Sequence[str], rows: Sequence[Sequence[Any]], batch_size: int = 1000
    ) -> None:
        """
        Bulk insert rows into a table using executemany in batches.

        Note: For very large bulk loads consider using bcp / BULK INSERT or SSIS outside of Python.
        """
        if self._pool is None:
            await self.connect_pool()
        placeholders = ",".join(["?" for _ in columns])
        cols = ",".join(columns)
        sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"

        async def run() -> None:
            try:
                async with self._pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        for i in range(0, len(rows), batch_size):
                            batch = rows[i : i + batch_size]
                            await cur.executemany(sql, batch)
            except ConnectorDependencyError:
                raise
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                raise map_pyodbc_error(exc, operation="mssql.bulk_insert") from exc

        # Bulk inserts are not idempotent; do not retry (would duplicate rows).
        await run_operation(
            run,
            config=self.retry_config,
            backend=_BACKEND,
            operation_name="mssql.bulk_insert",
            telemetry=self.telemetry,
            resource=table,
            retry=False,
        )

    async def write_records(
        self, target: str, records: Iterable[Mapping[str, Any]], *, batch_size: int = 1_000
    ) -> BulkWriteResult:
        rows = [dict(record) for record in records]
        if not rows:
            return BulkWriteResult(items_written=0)
        columns = list(rows[0])
        values = [tuple(row[column] for column in columns) for row in rows]
        await self.bulk_insert(target, columns, values, batch_size=batch_size)
        return BulkWriteResult(items_written=len(rows))
