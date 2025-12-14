from typing import Any, Dict, Iterator, List, Optional, Sequence

from ...interfaces import SyncConnector

try:
    import pyodbc
except ImportError:
    pyodbc = None


class SyncMSSQLConnector(SyncConnector):
    """
    Synchronous SQL Server connector using pyodbc.

    Requires an ODBC driver for SQL Server installed on the system.
    """

    def __init__(
        self,
        dsn: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
    ):
        self._dsn = dsn
        self._user = user
        self._password = password
        self._host = host
        self._port = port
        self._database = database
        self._conn = None

        if pyodbc is None:
            raise ImportError("pyodbc is required for SyncMSSQLConnector. Install with `pip install multids[sync]`")

    def connect(self) -> None:
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

            # Usually pyodbc needs a Driver spec too unless DSN is configured
            # Assume user knows what they are doing or DSN is provided
            # If not, let's default to a common driver if none provided?
            # Better to let user provide full string or set parts.
            conn_str = ";".join(parts)

        # Basic retries or connection logic could go here
        self._conn = pyodbc.connect(conn_str)

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
        cursor = self._conn.cursor()
        cursor.execute(sql, params or [])
        self._conn.commit()

    def fetch_rows(self, sql: str, params: Optional[Sequence[Any]] = None) -> List[Dict[str, Any]]:
        return list(self.fetch_iter(sql, params))

    def fetch_iter(self, sql: str, params: Optional[Sequence[Any]] = None) -> Iterator[Dict[str, Any]]:
        if self._conn is None:
            self.connect()
        cursor = self._conn.cursor()
        cursor.execute(sql, params or [])

        cols = [c[0] for c in cursor.description]
        for row in cursor:
            yield {k: v for k, v in zip(cols, row)}

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
