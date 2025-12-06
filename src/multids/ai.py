from __future__ import annotations

from typing import Any, Dict, Protocol


class AIClientInterface(Protocol):
    async def generate(self, prompt: str, **kwargs: Any) -> Dict[str, Any]:
        """Generate a response for the given prompt."""


class OpenAIClient:
    def __init__(self, client):
        # client: an `openai` async-compatible client or wrapper
        self._client = client

    async def generate(self, prompt: str, **kwargs: Any) -> Dict[str, Any]:
        # Implementation will depend on the chosen AI provider.
        return await self._client.create(prompt=prompt, **kwargs)
