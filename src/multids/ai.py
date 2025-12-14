from __future__ import annotations

from typing import Any, Dict, Protocol


class AIClientInterface(Protocol):
    async def generate(self, prompt: str, **kwargs: Any) -> Dict[str, Any]:
        """Generate a response for the given prompt."""


class OpenAIClient:
    def __init__(self, client: Any):
        """
        Initialize with an async openai client (e.g. AsyncOpenAI).

        Using Any for client to genericize and avoid hard dependency if openai not installed,
        but runtime it should be `openai.AsyncClient`.
        """
        self._client = client

    async def generate(self, prompt: str, **kwargs: Any) -> Dict[str, Any]:
        """
        Generate text using OpenAI ChatCompletion.

        Args:
            prompt: The text prompt.
            **kwargs: Extra arguments to pass to chat.completions.create (model, temperature, etc).

        Returns:
            Dict containing the raw response or at least 'content'.
        """
        # Default model if not provided
        model = kwargs.pop("model", "gpt-3.5-turbo")

        resp = await self._client.chat.completions.create(
            messages=[{"role": "user", "content": prompt}], model=model, **kwargs
        )
        # Simplify return to be dict-like or just the object
        # Returning a simplified dict for easier usage
        content = resp.choices[0].message.content
        return {"content": content, "raw": resp}
