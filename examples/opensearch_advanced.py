import asyncio
import json

from multids.connectors.opensearch import OpenSearchConnector


async def main():
    oc = OpenSearchConnector("http://localhost:9200")

    # Create an index with mappings
    mappings = {
        "mappings": {
            "properties": {
                "name": {"type": "text"},
                "age": {"type": "integer"},
                "joined": {"type": "date"},
                "tags": {"type": "keyword"},
            }
        }
    }

    # create index (idempotent for demo)
    try:
        await oc._request("PUT", "/users", json=mappings)
    except Exception:
        # ignore if already exists
        pass

    # Create an ingest pipeline to add a timestamp and lowercase a field
    pipeline = {
        "description": "Add ingest timestamp and lowercase name",
        "processors": [
            {"set": {"field": "ingest_ts", "value": "{{_ingest.timestamp}}"}},
            {"lowercase": {"field": "name"}},
        ],
    }

    await oc._request("PUT", "/_ingest/pipeline/add_meta", json=pipeline)

    # Index documents through the pipeline
    docs = [
        {"id": "u1", "name": "Alice", "age": 30, "tags": ["admin", "team-a"]},
        {"id": "u2", "name": "Bob", "age": 25, "tags": ["team-b"]},
    ]

    for d in docs:
        await oc._request("PUT", f"/users/_doc/{d['id']}?pipeline=add_meta", json=d)

    # Refresh and search
    await oc._request("POST", "/users/_refresh")
    r = await oc._request("POST", "/users/_search", json={"query": {"match_all": {}}})
    print(json.dumps(r.json(), indent=2))

    await oc.close()


if __name__ == "__main__":
    asyncio.run(main())
