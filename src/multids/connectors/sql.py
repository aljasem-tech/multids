from __future__ import annotations

from typing import Any, AsyncIterator, Iterable

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from ..interfaces import Connector


class SQLConnectorBase(Connector):
    """
    Base SQL connector using SQLAlchemy async engine.

    The `url` should be an async-enabled SQLAlchemy URL (for MySQL: `mysql+asyncmy://...`).
    """

    def __init__(self, url: str):
        self._url = url
        self._engine: AsyncEngine = create_async_engine(self._url, future=True)

    async def fetch_rows(self, query: str, /, **params: Any) -> AsyncIterator[dict]:
        """Stream rows from a query as dictionaries (uses SQLAlchemy streaming API)."""
        async with self._engine.connect() as conn:  # type: ignore
            result = await conn.stream(text(query), params)
            async for row in result.mappings():
                yield dict(row)

    async def execute(self, statement: str, /, **params: Any) -> None:
        """Execute a statement (INSERT/UPDATE/DELETE)."""
        async with self._engine.begin() as conn:  # type: ignore
            await conn.execute(text(statement), params)

    async def execute_many(self, statement: str, params_iter: Iterable[dict]) -> None:
        """
        Execute the same statement multiple times with different parameters.

        This implementation iterates and executes repeatedly inside a transaction.
        Drivers or more advanced techniques (COPY, bulk insert) may be added for performance.
        """
        async with self._engine.begin() as conn:  # type: ignore
            for params in params_iter:
                await conn.execute(text(statement), params)

    async def close(self) -> None:
        await self._engine.dispose()

    async def ping(self) -> bool:
        """Check connection by running a simple query."""
        try:
            async with self._engine.connect() as conn:  # type: ignore
                await conn.execute(text("SELECT 1"))
            return True
        except Exception:
            return False


class MySQLConnector(SQLConnectorBase):
    """
    MySQL connector convenience wrapper.

    Example URL: `mysql+asyncmy://user:password@host:3306/dbname`
    """

    def __init__(self, url: str):
        super().__init__(url)

    async def bulk_insert(
        self,
        table: str,
        rows: Iterable[dict],
        chunk_size: int = 1000,
    ) -> None:
        """
        Optimized bulk insert using SQLAlchemy's batch execution.

        Args:
            table: Name of the table to insert into.
            rows: Iterable of dictionaries, where keys match column names.
            chunk_size: Number of rows to insert in a single batch.
        """
        # Convert to iterator to handle both lists and generators
        rows_iter = iter(rows)

        # Peek at the first row to determine columns without consuming it
        try:
            first_row = next(rows_iter)
        except StopIteration:
            return  # Empty iterable

        cols = list(first_row.keys())
        # Reconstruct the iterable with the first row back in front
        # (This is a simple way; for very large iterables, using itertools.chain is cleaner)
        from itertools import chain, islice

        rows_iter = chain([first_row], rows_iter)

        col_list = ", ".join([f"`{c}`" for c in cols])
        placeholders = ", ".join([f":{c}" for c in cols])
        stmt = text(f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})")

        async with self._engine.begin() as conn:  # type: ignore
            while True:
                chunk = list(islice(rows_iter, chunk_size))
                if not chunk:
                    break
                await conn.execute(stmt, chunk)
