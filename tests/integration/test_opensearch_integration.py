import os

import pytest

httpx = None

try:
    import httpx
except Exception:
    httpx = None


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
