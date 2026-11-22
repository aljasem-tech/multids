import os

import pytest

from multids.connectors.sync.opensearch import SyncOpenSearchConnector

try:
    import httpx
except Exception:
    httpx = None  # type: ignore[assignment]


@pytest.mark.asyncio
@pytest.mark.integration
async def test_basic_index_search():
    url = os.environ.get("OPENSEARCH_URL")
    if not url:
        pytest.skip("OPENSEARCH_URL not set; skipping integration test")
    if httpx is None:
        pytest.skip("httpx not installed; install extras to run integration tests")

    async with httpx.AsyncClient(base_url=url, timeout=30.0) as client:
        # create index
        r = await client.put("/test-integ", json={"settings": {"number_of_shards": 1}})
        r.raise_for_status()

        # index a document
        r = await client.post("/test-integ/_doc/1", json={"name": "alice"})
        r.raise_for_status()

        # refresh
        r = await client.post("/test-integ/_refresh")
        r.raise_for_status()

        # search
        r = await client.post("/test-integ/_search", json={"query": {"match_all": {}}})
        r.raise_for_status()
        data = r.json()
        hits = data.get("hits", {}).get("hits", [])
        assert any(h.get("_source", {}).get("name") == "alice" for h in hits)

        # cleanup
        await client.delete("/test-integ")


@pytest.mark.integration
def test_sync_bulk_index_and_search():
    url = os.environ.get("OPENSEARCH_URL")
    if not url:
        pytest.skip("OPENSEARCH_URL not set; skipping integration test")
    if httpx is None:
        pytest.skip("httpx not installed; install extras to run integration tests")

    index = "sync-test-integ"
    connector = SyncOpenSearchConnector(url, timeout=30.0)
    try:
        # create index via raw httpx
        with httpx.Client(base_url=url, timeout=30.0) as client:
            client.put(f"/{index}", json={"settings": {"number_of_shards": 1}})

        docs = [{"id": i, "name": f"name-{i}"} for i in range(100)]
        result = connector.bulk_index(index, docs, chunk_size=50, refresh=True)
        assert isinstance(result, list)

        # search
        resp = connector.search(index, {"query": {"match_all": {}}}, size=100)
        hits = resp.get("hits", {}).get("hits", [])
        assert len(hits) == 100

        # write_records contract
        write_result = connector.write_records(index, [{"id": 1000, "name": "extra"}], batch_size=10)
        assert write_result.items_written == 1
    finally:
        connector.close()
        with httpx.Client(base_url=url, timeout=30.0) as client:
            client.delete(f"/{index}")
