from __future__ import annotations

import io
from typing import Any, Iterable, Iterator, Mapping

from ...contracts import BulkWriteResult, WriteResult
from ...error_mapping import map_postgres_error
from ...errors import ConnectorDependencyError
from ...interfaces import SyncConnector, SyncConnectorContext
from ...reliability import RetryConfig, run_operation_sync
from ...telemetry import Telemetry

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:  # pragma: no cover - exercised in minimal installations
    psycopg2 = None
    execute_values = None

_BACKEND = "postgres"


def _is_connection_error(exc: Exception) -> bool:
    from ...errors import ConnectorConnectionError

    return isinstance(exc, ConnectorConnectionError)


def _affected_rows(rowcount: int) -> int:
    if rowcount is None or rowcount < 0:
        return 1
    return int(rowcount)


class SyncPostgreSQLConnector(SyncConnectorContext, SyncConnector):
    """Synchronous PostgreSQL connector using psycopg2 directly.

    Supports standard record operations plus a native COPY-style bulk insert
    via :meth:`copy_records_to_table`.

    Parameters use ``%s`` placeholders (psycopg2 style). Pass them as a
    tuple or dict.

    Example::

        conn = SyncPostgreSQLConnector("postgresql://user:pass@localhost:5432/db")
        result = conn.execute("INSERT INTO users (name) VALUES (%s)", ("alice",))
        rows = list(conn.fetch_rows("SELECT * FROM users WHERE id > %s", (0,)))
        conn.close()
    """

    telemetry: Telemetry = Telemetry()

    def __init__(
        self,
        dsn: str,
        *,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
        max_backoff: float = 30.0,
        telemetry: Telemetry | None = None,
    ):
        if psycopg2 is None:
            raise ConnectorDependencyError("psycopg2 is required; install it with `pip install multids[postgres]`")
        self._dsn = dsn
        self._conn: Any = None
        self.retry_config = RetryConfig(max_retries, backoff_factor, max_backoff)
        self.telemetry = telemetry or Telemetry()

    def _get_conn(self) -> Any:
        """Return the psycopg2 connection, creating it on first use."""
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(self._dsn)
            self._conn.autocommit = False
        return self._conn

    def fetch_rows(self, query: str, params: Any = None) -> Iterator[dict]:
        """Stream rows from a query as dictionaries.

        Parameters use ``%s`` placeholders. Pass a tuple or dict.
        """

        def run() -> list[dict]:
            try:
                conn = self._get_conn()
                with conn.cursor() as cur:
                    cur.execute(query, params)
                    if cur.description is None:
                        return []
                    cols = [d[0] for d in cur.description]
                    return [dict(zip(cols, row)) for row in cur.fetchall()]
            except ConnectorDependencyError:
                raise
            except Exception as exc:
                raise map_postgres_error(exc, operation="postgres.fetch_rows") from exc

        rows = run_operation_sync(
            run,
            config=self.retry_config,
            backend=_BACKEND,
            operation_name="postgres.fetch_rows",
            telemetry=self.telemetry,
            retry_if=_is_connection_error,
        )
        yield from rows

    def execute(self, statement: str, params: Any = None) -> WriteResult:
        """Execute a statement (INSERT/UPDATE/DELETE/DDL) and report affected rows."""

        def run() -> WriteResult:
            try:
                conn = self._get_conn()
                with conn.cursor() as cur:
                    cur.execute(statement, params)
                    count = _affected_rows(cur.rowcount)
                conn.commit()
                return WriteResult(items_written=count)
            except ConnectorDependencyError:
                raise
            except Exception as exc:
                conn = self._conn
                if conn is not None and not conn.closed:
                    conn.rollback()
                raise map_postgres_error(exc, operation="postgres.execute") from exc

        return run_operation_sync(
            run,
            config=self.retry_config,
            backend=_BACKEND,
            operation_name="postgres.execute",
            telemetry=self.telemetry,
            retry_if=_is_connection_error,
        )

    def bulk_insert(
        self,
        table: str,
        rows: Iterable[Mapping[str, Any]],
        chunk_size: int = 1000,
    ) -> BulkWriteResult:
        """Insert mapping rows using COPY for speed."""
        return self.copy_records_to_table(table, rows)

    def copy_records_to_table(
        self,
        table: str,
        records: Iterable[Mapping[str, Any]],
        *,
        columns: list[str] | None = None,
    ) -> BulkWriteResult:
        """Bulk-insert records using PostgreSQL's native COPY protocol.

        This is the fastest way to load large datasets into PostgreSQL.
        """

        def run() -> int:
            records_list = [dict(r) for r in records]
            if not records_list:
                return 0

            cols = columns or list(records_list[0].keys())

            # Format rows for COPY: tab-separated, \N for NULL, escape backslash.
            buffer = io.StringIO()
            for r in records_list:
                values = []
                for col in cols:
                    val = r.get(col)
                    if val is None:
                        values.append("\\N")
                    else:
                        s = str(val)
                        s = s.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")
                        values.append(s)
                buffer.write("\t".join(values) + "\n")
            buffer.seek(0)

            try:
                conn = self._get_conn()
                col_list = ", ".join(cols)
                with conn.cursor() as cur:
                    cur.copy_expert(
                        f"COPY {table} ({col_list}) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N')",
                        buffer,
                    )
                conn.commit()
                return len(records_list)
            except ConnectorDependencyError:
                raise
            except Exception as exc:
                conn = self._conn
                if conn is not None and not conn.closed:
                    conn.rollback()
                raise map_postgres_error(exc, operation="postgres.copy_records_to_table") from exc

        # COPY is not idempotent; do not retry (would duplicate rows).
        written = run_operation_sync(
            run,
            config=self.retry_config,
            backend=_BACKEND,
            operation_name="postgres.copy_records_to_table",
            telemetry=self.telemetry,
            resource=table,
            retry=False,
        )
        return BulkWriteResult(items_written=written)

    def write_records(
        self,
        target: str,
        records: Iterable[Mapping[str, Any]],
        *,
        batch_size: int = 1_000,
    ) -> BulkWriteResult:
        """Insert mapping records under the backend-neutral record-writer contract.

        Uses COPY via :meth:`copy_records_to_table` for performance.
        """
        if batch_size <= 0:
            raise ValueError("batch_size must be greater than zero")
        return self.copy_records_to_table(target, records)

    def close(self) -> None:
        if self._conn is not None and not self._conn.closed:
            self._conn.close()
            self._conn = None

    def ping(self) -> bool:
        """Check connection by running a simple query."""
        try:
            conn = self._get_conn()
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            return True
        except Exception:
            return False
