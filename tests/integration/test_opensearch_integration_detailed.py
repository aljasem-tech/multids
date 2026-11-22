"""Detailed opt-in integration coverage for a real OpenSearch service.

Set ``OPENSEARCH_URL`` (and optionally ``OPENSEARCH_USERNAME`` /
``OPENSEARCH_PASSWORD`` or ``OPENSEARCH_API_KEY``).

Scenarios: authentication, bulk indexing with Unicode, scroll pagination, large
bulk loads, cleanup and failure recovery.
"""

from __future__ import annotations

import os
import uuid

import pytest

from multids.connectors.opensearch import OpenSearchConnector
from multids.errors import ConnectorError

pytestmark = [pytest.mark.real_integration, pytest.mark.asyncio]


def _connector() -> OpenSearchConnector | None:
    url = os.environ.get("OPENSEARCH_URL")
    if not url:
        return None
    username = os.environ.get("OPENSEARCH_USERNAME")
    password = os.environ.get("OPENSEARCH_PASSWORD")
    api_key = os.environ.get("OPENSEARCH_API_KEY")
    if api_key:
        return OpenSearchConnector(url, api_key=api_key, timeout=60.0)
    if username and password:
        return OpenSearchConnector(url, basic_auth=(username, password), timeout=60.0)
    return OpenSearchConnector(url, timeout=60.0)


async def test_opensearch_auth_ping():
    connector = _connector()
    if connector is None:
        pytest.skip("Set OPENSEARCH_URL to run real OpenSearch integration tests")
    try:
        assert await connector.ping() is True
    finally:
        await connector.close()


async def test_opensearch_bulk_unicode_and_scroll():
    connector = _connector()
    if connector is None:
        pytest.skip("OPENSEARCH_URL not set")

    index = f"multids-it-{uuid.uuid4().hex[:12]}"
    try:
        await connector._request("PUT", f"/{index}", json={"settings": {"number_of_shards": 1}})

        docs = [{"id": i, "name": f"héllo-{i}", "grp": i % 3} for i in range(50)]
        results = await connector.bulk_index(index, docs, chunk_size=10, refresh=True)
        assert isinstance(results, list) and results

        # Scroll pagination yields every document exactly once.
        hits = [hit async for hit in connector.scroll(index, {"query": {"match_all": {}}}, scroll="1m")]
        assert len(hits) == 50
        names = {hit.get("_source", {}).get("name") for hit in hits}
        assert "héllo-0" in names
    finally:
        try:
            await connector._request("DELETE", f"/{index}")
        except Exception:
            pass
        await connector.close()


async def test_opensearch_large_bulk_and_cleanup():
    connector = _connector()
    if connector is None:
        pytest.skip("OPENSEARCH_URL not set")

    index = f"multids-it-{uuid.uuid4().hex[:12]}"
    try:
        await connector._request("PUT", f"/{index}", json={"settings": {"number_of_shards": 1}})
        docs = ({"id": i, "payload": "x" * 64} for i in range(2000))
        await connector.bulk_index(index, docs, chunk_size=500, refresh=True)
        response = await connector.search(index, {"query": {"match_all": {}}}, size=0)
        total = response.get("hits", {}).get("total", {})
        count = total.get("value") if isinstance(total, dict) else total
        assert count == 2000
    finally:
        try:
            await connector._request("DELETE", f"/{index}")
        except Exception:
            pass
        await connector.close()


async def test_opensearch_failure_recovery_raises_typed_error():
    connector = _connector()
    if connector is None:
        pytest.skip("OPENSEARCH_URL not set")

    missing = f"multids-it-missing-{uuid.uuid4().hex[:12]}"
    try:
        with pytest.raises(ConnectorError):
            await connector.search(missing, {"query": {"match_all": {}}})
    finally:
        await connector.close()
