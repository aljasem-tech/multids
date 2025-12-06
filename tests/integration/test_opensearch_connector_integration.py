import os

import pytest

try:
    from multids.connectors.opensearch import OpenSearchConnector
except Exception:
    OpenSearchConnector = None


@pytest.mark.asyncio
@pytest.mark.integration
async def test_bulk_index_and_search_integration():
    url = os.environ.get("OPENSEARCH_URL")
    if not url:
        pytest.skip("OPENSEARCH_URL not set; skipping integration test")
    if OpenSearchConnector is None:
        pytest.skip("OpenSearchConnector not available; ensure package is installed with opensearch extras")

    oc = OpenSearchConnector(url)

    # ensure clean index
    try:
        await oc._request("DELETE", "/integ-connector")
    except Exception:
        pass

    # create index
    await oc._request("PUT", "/integ-connector", json={"settings": {"number_of_shards": 1}})

    # bulk index with sync iterable
    docs = [{"id": f"d{i}", "name": f"name-{i}", "grp": "g1"} for i in range(6)]
    res = await oc.bulk_index("integ-connector", docs, chunk_size=2, refresh=False)
    assert isinstance(res, list)

    # refresh and assert via search
    await oc._request("POST", "/integ-connector/_refresh")
    r = await oc.search("integ-connector", {"query": {"match_all": {}}}, size=10)
    assert r.get("hits", {}).get("total") is not None or len(r.get("hits", {}).get("hits", [])) >= 6

    # bulk index with async generator and id_field/routing
    async def agen():
        for i in range(3):
            yield {"id": f"g{i}", "name": f"gen-{i}", "grp": "g2"}

    res2 = await oc.bulk_index("integ-connector", agen(), chunk_size=1)
    assert isinstance(res2, list)

    await oc._request("POST", "/integ-connector/_refresh")
    r2 = await oc.search("integ-connector", {"query": {"term": {"grp.keyword": {"value": "g2"}}}}, size=10)
    hits = r2.get("hits", {}).get("hits", [])
    assert any(h.get("_source", {}).get("name", "").startswith("gen-") for h in hits)

    # cleanup
    await oc._request("DELETE", "/integ-connector")
    await oc.close()
