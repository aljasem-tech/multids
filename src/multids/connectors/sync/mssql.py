from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence

from ...contracts import BulkWriteResult, WriteResult
from ...error_mapping import map_pyodbc_error
from ...errors import ConnectorConnectionError, ConnectorDependencyError
from ...interfaces import SyncConnector, SyncConnectorContext
from ...reliability import RetryConfig, run_operation_sync
from ...telemetry import Telemetry

try:
    import pyodbc
except ImportError:
    pyodbc = None

_BACKEND = "mssql"


def _is_connection_error(exc: Exception) -> bool:
    return isinstance(exc, ConnectorConnectionError)


class SyncMSSQLConnector(SyncConnectorContext, SyncConnector):
    """
    Synchronous SQL Server connector using pyodbc.

    Requires an ODBC driver for SQL Server installed on the system.
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
        max_retries: int = 3,
        backoff_factor: float = 0.5,
        max_backoff: float = 30.0,
        telemetry: Telemetry | None = None,
    ):
        self._dsn = dsn
        self._user = user
        self._password = password
        self._host = host
        self._port = port
        self._database = database
        self._conn = None
        self.retry_config = RetryConfig(max_retries, backoff_factor, max_backoff)
        self.telemetry = telemetry or Telemetry()

        if pyodbc is None:
            raise ConnectorDependencyError(
                "pyodbc is required for SyncMSSQLConnector. Install with `pip install multids[sync]`"
            )

    def connect(self) -> None:  # noqa: C901
        if self._conn is not None:
            return

        conn_str = self._dsn
        if conn_str is None:
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

            # Usually pyodbc needs a Driver spec too unless DSN is configured.
            # Assume user knows what they are doing or DSN is provided.
            conn_str = ";".join(parts)

        def run() -> Any:
            try:
                return pyodbc.connect(conn_str)
            except ConnectorDependencyError:
                raise
            except Exception as exc:
                raise map_pyodbc_error(exc, operation="mssql.connect") from exc

        self._conn = run_operation_sync(
            run,
            config=self.retry_config,
            backend=_BACKEND,
            operation_name="mssql.connect",
            telemetry=self.telemetry,
            retry_if=_is_connection_error,
        )

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def ping(self) -> bool:
        try:
            if self._conn is None:
                self.connect()
            cursor = self._conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            return True
        except Exception:
            return False

    def execute(self, sql: str, params: Optional[Sequence[Any]] = None) -> None:
        if self._conn is None:
            self.connect()

        def run() -> None:
            try:
                cursor = self._conn.cursor()
                cursor.execute(sql, params or [])
                self._conn.commit()
            except ConnectorDependencyError:
                raise
            except Exception as exc:
                raise map_pyodbc_error(exc, operation="mssql.execute") from exc

        run_operation_sync(
            run,
            config=self.retry_config,
            backend=_BACKEND,
            operation_name="mssql.execute",
            telemetry=self.telemetry,
            retry_if=_is_connection_error,
        )

    def execute_statement(self, statement: str, params: Optional[Sequence[Any]] = None) -> WriteResult:
        self.execute(statement, params)
        return WriteResult(items_written=1)

    def fetch_rows(self, sql: str, params: Optional[Sequence[Any]] = None) -> List[Dict[str, Any]]:
        return list(self.fetch_iter(sql, params))

    def fetch_iter(self, sql: str, params: Optional[Sequence[Any]] = None) -> Iterator[Dict[str, Any]]:
        if self._conn is None:
            self.connect()
        try:
            cursor = self._conn.cursor()
            cursor.execute(sql, params or [])

            cols = [c[0] for c in cursor.description]
            for row in cursor:
                yield {k: v for k, v in zip(cols, row)}
        except ConnectorDependencyError:
            raise
        except Exception as exc:
            raise map_pyodbc_error(exc, operation="mssql.fetch_iter") from exc

    stream_records = fetch_iter

    def bulk_insert(
        self, table: str, columns: Sequence[str], rows: Sequence[Sequence[Any]], batch_size: int = 1000
    ) -> None:
        """
        Bulk insert rows using executemany.
        """
        if self._conn is None:
            self.connect()

        placeholders = ",".join(["?" for _ in columns])
        cols = ",".join(columns)
        sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"

        def run() -> None:
            try:
                cursor = self._conn.cursor()

                # pyodbc's executemany is reasonably fast if fast_executemany is enabled
                # which depends on the driver, but typically used.
                try:
                    cursor.fast_executemany = True
                except Exception:
                    pass

                for i in range(0, len(rows), batch_size):
                    batch = rows[i : i + batch_size]
                    cursor.executemany(sql, batch)
                    self._conn.commit()
            except ConnectorDependencyError:
                raise
            except Exception as exc:
                raise map_pyodbc_error(exc, operation="mssql.bulk_insert") from exc

        # Bulk inserts are not idempotent; do not retry (would duplicate rows).
        run_operation_sync(
            run,
            config=self.retry_config,
            backend=_BACKEND,
            operation_name="mssql.bulk_insert",
            telemetry=self.telemetry,
            resource=table,
            retry=False,
        )

    def write_records(
        self, target: str, records: Iterable[Mapping[str, Any]], *, batch_size: int = 1_000
    ) -> BulkWriteResult:
        rows = [dict(record) for record in records]
        if not rows:
            return BulkWriteResult(items_written=0)
        columns = list(rows[0])
        values = [tuple(row[column] for column in columns) for row in rows]
        self.bulk_insert(target, columns, values, batch_size=batch_size)
        return BulkWriteResult(items_written=len(rows))
