from itertools import islice
from typing import Any, Iterable, Iterator

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from ...interfaces import SyncConnector


class SyncSQLConnectorBase(SyncConnector):
    """
    Base synchronous SQL connector using SQLAlchemy engine.

    The `url` should be a standard SQLAlchemy URL (e.g. `mysql+pymysql://...`).
    """

    def __init__(self, url: str):
        self._url = url
        self._engine: Engine = create_engine(self._url, future=True)

    def fetch_rows(self, query: str, **params: Any) -> Iterator[dict]:
        """Stream rows from a query as dictionaries."""
        with self._engine.connect() as conn:  # type: ignore
            # stream_results=True might be needed for server-side cursors depending on driver
            # For strict streaming, execution options might be needed.
            # Here we follow basic SQLAlchemy usage.
            result = conn.execution_options(stream_results=True).execute(text(query), params)
            for row in result.mappings():
                yield dict(row)

    def execute(self, statement: str, **params: Any) -> None:
        """Execute a statement (INSERT/UPDATE/DELETE)."""
        with self._engine.begin() as conn:  # type: ignore
            conn.execute(text(statement), params)

    def execute_many(self, statement: str, params_iter: Iterable[dict]) -> None:
        """
        Execute the same statement multiple times.
        """
        with self._engine.begin() as conn:  # type: ignore
            for params in params_iter:
                conn.execute(text(statement), params)

    def close(self) -> None:
        self._engine.dispose()

    def ping(self) -> bool:
        """Check connection by running a simple query."""
        try:
            with self._engine.connect() as conn:  # type: ignore
                conn.execute(text("SELECT 1"))
            return True
        except Exception:
            return False


class SyncMySQLConnector(SyncSQLConnectorBase):
    """
    Synchronous MySQL connector.
    """

    def __init__(self, url: str):
        super().__init__(url)

    def bulk_insert(
        self,
        table: str,
        rows: Iterable[dict],
        chunk_size: int = 1000,
    ) -> None:
        """
        Optimized bulk insert using SQLAlchemy's batch execution.
        """
        rows_iter = iter(rows)

        try:
            first_row = next(rows_iter)
        except StopIteration:
            return

        cols = list(first_row.keys())
        from itertools import chain

        rows_iter = chain([first_row], rows_iter)

        col_list = ", ".join([f"`{c}`" for c in cols])
        placeholders = ", ".join([f":{c}" for c in cols])
        stmt = text(f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})")

        with self._engine.begin() as conn:  # type: ignore
            while True:
                chunk = list(islice(rows_iter, chunk_size))
                if not chunk:
                    break
                conn.execute(stmt, chunk)
