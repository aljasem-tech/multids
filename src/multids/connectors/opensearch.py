from __future__ import annotations

import asyncio
import json
from typing import Any, AsyncIterator, Dict, Iterable, List, Optional, cast

from ..interfaces import Connector


class OpenSearchConnector(Connector):
    """
    Async OpenSearch connector using httpx.

    Features:
    - `index_doc` to index a single document
    - `bulk_index` to index many docs (sync or async iterable) using the bulk API
    - `search` and `scroll` helpers for paging large result sets
    - `ping` health check and `close`

    Authentication: supports API Key via `api_key` or HTTP Basic auth using `basic_auth`.
    """

    def __init__(
        self,
        base_url: str,
        api_key: Optional[str] = None,
        basic_auth: Optional[tuple[str, str]] = None,
        timeout: float = 30.0,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
    ):
        self._base = base_url.rstrip("/")
        # Lazy import httpx to avoid importing during test collection in constrained envs.
        try:
            import httpx as _httpx

            self._httpx = _httpx
            self._client = _httpx.AsyncClient(base_url=self._base, timeout=timeout)
        except Exception:
            self._httpx = None
            self._client = None
        self._api_key = api_key
        self._basic_auth = basic_auth
        self._max_retries = max_retries
        self._backoff_factor = backoff_factor

    def _auth_headers(self) -> Dict[str, str]:
        headers: Dict[str, str] = {}
        if self._api_key:
            headers["Authorization"] = f"ApiKey {self._api_key}"
        return headers

    def _auth_kwargs(self) -> Dict[str, Any]:
        kwargs: Dict[str, Any] = {}
        if self._basic_auth is not None:
            kwargs["auth"] = self._basic_auth
        return kwargs

    async def _request(self, method: str, path: str, **kwargs):
        """
        Internal request with simple retry/backoff for transient errors.

        Retries on `httpx.RequestError` and 5xx responses.
        """
        attempt = 0
        while True:
            try:
                if self._client is None:
                    raise RuntimeError("httpx not available in this environment")
                resp = await self._client.request(method, path, **kwargs)
            except Exception:
                attempt += 1
                if attempt > self._max_retries:
                    raise
                backoff = self._backoff_factor * (2 ** (attempt - 1))
                await asyncio.sleep(backoff)
                continue

            # if server error, retry
            if 500 <= resp.status_code < 600:
                attempt += 1
                if attempt > self._max_retries:
                    resp.raise_for_status()
                backoff = self._backoff_factor * (2 ** (attempt - 1))
                await asyncio.sleep(backoff)
                continue

            return resp

    @staticmethod
    def build_bulk_ndjson(
        docs: Iterable[Dict[str, Any]] | AsyncIterator[Dict[str, Any]],
        index: str,
        id_field: Optional[str] = None,
        routing_field: Optional[str] = None,
        chunk_size: int = 500,
    ) -> AsyncIterator[str]:
        """
        Build NDJSON chunks for the bulk API from docs.

        Accepts sync iterable or async iterator. Yields NDJSON chunk strings.
        """

        async def _async_from_sync(sync_iter: Iterable[Dict[str, Any]]):
            for d in sync_iter:
                yield d

        async def _gen(async_iter: AsyncIterator[Dict[str, Any]]):
            out: List[str] = []
            cnt = 0
            async for doc in async_iter:
                action: Dict[str, Any] = {"index": {"_index": index}}
                if id_field and id_field in doc:
                    action["index"]["_id"] = doc[id_field]
                if routing_field and routing_field in doc:
                    action["index"]["routing"] = doc[routing_field]

                out.append(json.dumps(action, ensure_ascii=False))
                out.append(json.dumps(doc, ensure_ascii=False))
                cnt += 1
                if cnt >= chunk_size:
                    yield "\n".join(out) + "\n"
                    out = []
                    cnt = 0
            if out:
                yield "\n".join(out) + "\n"

        # normalize
        if hasattr(docs, "__aiter__"):
            return _gen(cast(AsyncIterator[Dict[str, Any]], docs))
        else:
            return _gen(_async_from_sync(cast(Iterable[Dict[str, Any]], docs)))

    async def ping(self) -> bool:
        try:
            r = await self._client.get("/")
            return r.status_code == 200
        except Exception:
            return False

    async def index_doc(self, index: str, doc: Dict[str, Any], id: Optional[str] = None) -> Dict[str, Any]:
        """Index a single document. If `id` is provided, uses PUT to set the document id."""
        headers = self._auth_headers()
        # Use explicit json dumps with ensure_ascii=False for unicode support
        headers.setdefault("Content-Type", "application/json")
        body = json.dumps(doc, ensure_ascii=False)

        if id is None:
            r = await self._client.post(f"/{index}/_doc", content=body, headers=headers, **self._auth_kwargs())
        else:
            r = await self._client.put(f"/{index}/_doc/{id}", content=body, headers=headers, **self._auth_kwargs())
        r.raise_for_status()
        return r.json()

    async def bulk_index(
        self,
        index: str,
        docs: Iterable[Dict[str, Any]] | AsyncIterator[Dict[str, Any]],
        chunk_size: int = 500,
        refresh: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Bulk index documents using the `_bulk` API.

        Accepts either a synchronous iterable or an async iterator of documents.
        Returns list of responses from each bulk request.
        """
        headers = self._auth_headers()
        headers.setdefault("Content-Type", "application/x-ndjson")

        async def _iter_ndjson(async_iter: AsyncIterator[Dict[str, Any]]):
            chunk = []
            async for d in async_iter:
                action = {"index": {"_index": index}}
                chunk.append(json.dumps(action, ensure_ascii=False))
                chunk.append(json.dumps(d, ensure_ascii=False))
                if len(chunk) >= chunk_size * 2:
                    yield "\n".join(chunk) + "\n"
                    chunk = []
            if chunk:
                yield "\n".join(chunk) + "\n"

        async def _async_from_sync_iterable(sync_iterable: Iterable[Dict[str, Any]]):
            for d in sync_iterable:
                yield d

        results: List[Dict[str, Any]] = []

        # Normalize to async iterator
        async_iter: AsyncIterator[Dict[str, Any]]
        if hasattr(docs, "__aiter__"):
            async_iter = cast(AsyncIterator[Dict[str, Any]], docs)
        else:
            async_iter = _async_from_sync_iterable(cast(Iterable[Dict[str, Any]], docs))

        # stream NDJSON chunks and POST to _bulk
        # use build_bulk_ndjson to support metadata fields
        async for ndchunk in self.build_bulk_ndjson(
            async_iter, index, id_field=None, routing_field=None, chunk_size=chunk_size
        ):
            r = await self._request(
                "POST",
                f"/_bulk?refresh={str(refresh).lower()}",
                content=ndchunk,
                headers=headers,
                **self._auth_kwargs(),
            )
            r.raise_for_status()
            results.append(r.json())

        return results

    async def search(self, index: str, query: Dict[str, Any], size: int = 10, from_: int = 0) -> Dict[str, Any]:
        body = dict(query)
        body.setdefault("size", size)
        body.setdefault("from", from_)
        headers = self._auth_headers()
        r = await self._client.post(f"/{index}/_search", json=body, headers=headers, **self._auth_kwargs())
        r.raise_for_status()
        return r.json()

    async def scroll(self, index: str, query: Dict[str, Any], scroll: str = "1m") -> AsyncIterator[Dict[str, Any]]:
        """Scroll over search results. Yields individual hits (dicts)."""
        headers = self._auth_headers()
        body = dict(query)
        body.setdefault("size", 1000)
        r = await self._client.post(
            f"/{index}/_search?scroll={scroll}", json=body, headers=headers, **self._auth_kwargs()
        )
        r.raise_for_status()
        data = r.json()
        scroll_id = data.get("_scroll_id")
        hits = data.get("hits", {}).get("hits", [])
        for h in hits:
            yield h

        try:
            while True:
                if not scroll_id:
                    break
                r = await self._client.post(
                    "/_search/scroll",
                    json={"scroll": scroll, "scroll_id": scroll_id},
                    headers=headers,
                    **self._auth_kwargs(),
                )
                r.raise_for_status()
                data = r.json()
                scroll_id = data.get("_scroll_id")
                hits = data.get("hits", {}).get("hits", [])
                if not hits:
                    break
                for h in hits:
                    yield h
        finally:
            if scroll_id:
                # best-effort clear scroll
                try:
                    await self._client.request(
                        "DELETE",
                        "/_search/scroll",
                        json={"scroll_id": [scroll_id]},
                        headers=headers,
                        **self._auth_kwargs(),
                    )
                except Exception:
                    pass

    async def close(self) -> None:
        await self._client.aclose()
