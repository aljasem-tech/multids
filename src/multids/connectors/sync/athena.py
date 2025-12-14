import time
from typing import Any, Dict, Iterator, List, Optional

try:
    import boto3
except ImportError:
    boto3 = None

from ...interfaces import SyncConnector


class SyncAthenaConnector(SyncConnector):
    """
    Synchronous Athena connector using boto3.
    """

    def __init__(
        self,
        region_name: Optional[str] = None,
        workgroup: Optional[str] = None,
        poll_interval: float = 1.0,
        max_wait: float = 300.0,
    ):
        if boto3 is None:
            raise ImportError("boto3 is required for SyncAthenaConnector. Install with `pip install multids[sync]`")

        self.region_name = region_name
        self.workgroup = workgroup
        self.poll_interval = poll_interval
        self.max_wait = max_wait
        self._session = boto3.Session(region_name=region_name)
        self._client = self._session.client("athena")

    def close(self) -> None:
        if self._client:
            self._client.close()

    def ping(self) -> bool:
        try:
            self._client.list_work_groups(MaxResults=1)
            return True
        except Exception:
            return False

    def start_query(
        self,
        sql: str,
        database: Optional[str] = None,
        output_location: Optional[str] = None,
        encryption_configuration: Optional[Dict] = None,
    ) -> str:
        """Start an Athena query execution and return the execution id."""
        params: Dict[str, Any] = {"QueryString": sql}
        if database:
            params["QueryExecutionContext"] = {"Database": database}
        cfg = {}
        if output_location:
            cfg["OutputLocation"] = output_location
        if encryption_configuration:
            cfg["EncryptionConfiguration"] = encryption_configuration
        if cfg:
            params["ResultConfiguration"] = cfg
        if self.workgroup:
            params["WorkGroup"] = self.workgroup

        resp = self._client.start_query_execution(**params)
        return resp["QueryExecutionId"]

    def wait_query(self, query_execution_id: str, timeout: Optional[float] = None) -> Dict[str, Any]:
        """Poll Athena until the query reaches a terminal state."""
        timeout = timeout if timeout is not None else self.max_wait
        elapsed = 0.0
        while True:
            resp = self._client.get_query_execution(QueryExecutionId=query_execution_id)
            state = resp.get("QueryExecution", {}).get("Status", {}).get("State")
            if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
                return resp

            if elapsed >= timeout:
                try:
                    self._client.stop_query_execution(QueryExecutionId=query_execution_id)
                except Exception:
                    pass
                raise TimeoutError(f"Query {query_execution_id} timed out after {timeout}s")

            time.sleep(self.poll_interval)
            elapsed += self.poll_interval

    def get_results(self, query_execution_id: str) -> Iterator[Dict[str, Any]]:
        """Yield rows from `get_query_results` as dictionaries keyed by column name."""
        paginator = self._client.get_paginator("get_query_results")

        columns: List[str] = []

        # We need to handle the header row carefully across pages
        # Actually, get_query_results paginator usually returns header in first page only?
        # boto3 docs vary, but let's assume standard behavior.

        for page in paginator.paginate(QueryExecutionId=query_execution_id):
            result_set = page.get("ResultSet", {})
            metadata = result_set.get("ResultSetMetadata", {})
            col_info = metadata.get("ColumnInfo", [])

            if not columns and col_info:
                columns = [cinfo.get("Name") for cinfo in col_info]

            rows = result_set.get("Rows", [])
            if not rows:
                continue

            # Standard Athena API returns header as the first row of the first page
            first_row_data = rows[0].get("Data", [])
            header_detected = (
                bool(columns)
                and bool(first_row_data)
                and all("VarCharValue" in cell for cell in first_row_data)
                # Safeguard: if first row matches column names
                and first_row_data[0].get("VarCharValue") == columns[0]
            )

            start = 1 if header_detected else 0
            for r in rows[start:]:
                data = r.get("Data", [])
                values = [d.get("VarCharValue") for d in data]
                if columns:
                    yield dict(zip(columns, values))
                else:
                    yield {str(i): v for i, v in enumerate(values)}

    def query_stream(self, sql: str, database: Optional[str] = None) -> Iterator[Dict[str, Any]]:
        """Convenience: run SQL and stream results."""
        qid = self.start_query(sql, database=database)
        exec_resp = self.wait_query(qid)
        state = exec_resp.get("QueryExecution", {}).get("Status", {}).get("State")
        if state != "SUCCEEDED":
            reason = exec_resp.get("QueryExecution", {}).get("Status", {}).get("StateChangeReason")
            raise RuntimeError(f"Query failed: {reason}")

        yield from self.get_results(qid)

    def fetch_all(self, sql: str, database: Optional[str] = None) -> List[Dict[str, Any]]:
        return list(self.query_stream(sql, database=database))
