from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
from typing import Any, AsyncIterator, Dict, List, Optional

from ..interfaces import Connector


class AthenaError(Exception):
    pass


class AthenaConnector(Connector):
    """
    Async helper for running Athena queries and streaming results.

    Minimal, pragmatic implementation that uses `aioboto3` when available.
    Features:
    - start_query: start an Athena query and return execution id
    - wait_query: poll for completion with timeout
    - get_results: paginate `get_query_results` and yield rows as dicts
    - query_stream: convenience to run a SQL and stream results

    Notes:
    - Reuse the connector instance to reuse the underlying boto3 session/client.
    - Always implementation `close()` when done (or usage as context manager if we added one,
    but explicit close is fine).
    """

    def __init__(
        self,
        region_name: Optional[str] = None,
        workgroup: Optional[str] = None,
        poll_interval: float = 1.0,
        max_wait: float = 300.0,
    ):
        self.region_name = region_name
        self.workgroup = workgroup
        self.poll_interval = poll_interval
        self.max_wait = max_wait
        self._session: Optional[Any] = None
        self._client: Optional[Any] = None
        self._exit_stack: Optional[Any] = None

        try:
            import aioboto3

            self._aioboto3 = aioboto3
        except Exception as e:
            self._aioboto3 = None
            raise AthenaError("aioboto3 is required for AthenaConnector") from e

    async def _get_client(self):
        """Get or create the cached aioboto3 client."""
        if self._client is not None:
            return self._client

        if self._aioboto3 is None:
            raise AthenaError("aioboto3 is required for AthenaConnector")

        self._session = self._aioboto3.Session()
        self._exit_stack = AsyncExitStack()  # type: ignore
        # Enter the client context and keep it open until close() is called
        client_cm = self._session.client("athena", region_name=self.region_name)
        self._client = await self._exit_stack.enter_async_context(client_cm)
        return self._client

    async def close(self) -> None:
        """Close the underlying client and session."""
        if self._exit_stack:
            await self._exit_stack.aclose()
            self._exit_stack = None
            self._client = None
            self._session = None

    async def ping(self) -> bool:
        """Check connectivity by listing workgroups (lightweight)."""
        try:
            client = await self._get_client()
            await client.list_work_groups(MaxResults=1)
            return True
        except Exception:
            return False

    async def start_query(
        self,
        sql: str,
        database: Optional[str] = None,
        output_location: Optional[str] = None,
        encryption_configuration: Optional[Dict] = None,
    ) -> str:
        """Start an Athena query execution and return the execution id."""
        client = await self._get_client()
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

        resp = await client.start_query_execution(**params)
        return resp["QueryExecutionId"]

    async def wait_query(self, query_execution_id: str, timeout: Optional[float] = None) -> Dict[str, Any]:
        """Poll Athena until the query reaches a terminal state."""
        timeout = timeout if timeout is not None else self.max_wait
        client = await self._get_client()
        elapsed = 0.0
        while True:
            resp = await client.get_query_execution(QueryExecutionId=query_execution_id)
            state = resp.get("QueryExecution", {}).get("Status", {}).get("State")
            if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
                return resp
            if elapsed >= timeout:
                # attempt to stop the query
                try:
                    await client.stop_query_execution(QueryExecutionId=query_execution_id)
                except Exception:
                    pass
                raise asyncio.TimeoutError(f"Query {query_execution_id} timed out after {timeout}s")
            await asyncio.sleep(self.poll_interval)
            elapsed += self.poll_interval

    async def get_results(self, query_execution_id: str) -> AsyncIterator[Dict[str, Any]]:
        """Yield rows from `get_query_results` as dictionaries keyed by column name."""
        client = await self._get_client()
        paginator_token = None
        columns: List[str] = []
        while True:
            if paginator_token:
                resp = await client.get_query_results(QueryExecutionId=query_execution_id, NextToken=paginator_token)
            else:
                resp = await client.get_query_results(QueryExecutionId=query_execution_id)

            result_set = resp.get("ResultSet", {})
            metadata = result_set.get("ResultSetMetadata", {})
            col_info = metadata.get("ColumnInfo", [])
            if not columns and col_info:
                columns = [cinfo.get("Name") for cinfo in col_info]

            rows = result_set.get("Rows", [])
            # skip header row if present
            first_row_data = rows[0].get("Data", []) if rows else []
            header_detected = (
                bool(columns)
                and bool(first_row_data)
                and all("VarCharValue" in cell for cell in first_row_data)
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

            paginator_token = resp.get("NextToken")
            if not paginator_token:
                break

    async def query_stream(self, sql: str, database: Optional[str] = None) -> AsyncIterator[Dict[str, Any]]:
        """Convenience: run SQL and stream results."""
        qid = await self.start_query(sql, database=database)
        exec_resp = await self.wait_query(qid)
        state = exec_resp.get("QueryExecution", {}).get("Status", {}).get("State")
        if state != "SUCCEEDED":
            reason = exec_resp.get("QueryExecution", {}).get("Status", {}).get("StateChangeReason")
            raise AthenaError(f"Query failed: {reason}")
        async for row in self._aiter_results(qid):
            yield row

    async def _aiter_results(self, qid: str) -> AsyncIterator[Dict[str, Any]]:
        async for r in self.get_results(qid):
            yield r

    async def fetch_all(self, sql: str, database: Optional[str] = None) -> List[Dict[str, Any]]:
        rows = []
        async for r in self.query_stream(sql, database=database):
            rows.append(r)
        return rows

    async def to_pandas(self, sql: str, database: Optional[str] = None):
        try:
            import pandas as pd  # type: ignore

        except Exception as e:  # pragma: no cover - optional
            raise AthenaError("pandas is required for to_pandas()") from e

        rows = await self.fetch_all(sql, database=database)
        return pd.DataFrame(rows)
