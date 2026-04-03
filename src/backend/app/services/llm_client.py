from __future__ import annotations

import httpx

from app.config import settings


class LLMClient:
    def __init__(self) -> None:
        self.enabled = settings.llm_enabled

    async def complete_text(
        self,
        prompt: str,
        model: str | None = None,
        temperature: float = 0.2,
        top_p: float | None = None,
        system_prompt: str | None = None,
    ) -> str | None:
        results = await self.complete_texts(
            prompt=prompt,
            model=model,
            temperature=temperature,
            top_p=top_p,
            n=1,
            system_prompt=system_prompt,
        )
        return results[0] if results else None

    async def complete_texts(
        self,
        prompt: str,
        model: str | None = None,
        temperature: float = 0.2,
        top_p: float | None = None,
        n: int = 1,
        system_prompt: str | None = None,
    ) -> list[str]:
        if not self.enabled:
            return []
        target_model = (model or settings.generation_model).strip()
        if not target_model:
            return []
        headers = {"Authorization": f"Bearer {settings.api_key}"}
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})
        async with httpx.AsyncClient(base_url=settings.api_base_url, timeout=settings.timeout_seconds) as client:
            payload = {
                "model": target_model,
                "messages": messages,
                "temperature": temperature,
                "top_p": top_p if top_p is not None else 0.8,
                "n": n,
                "max_tokens": settings.max_completion_tokens,
            }
            try:
                response = await client.post("/chat/completions", headers=headers, json=payload)
                response.raise_for_status()
            except Exception:
                return []
            contents = self._usable_contents(response.json())
            if contents:
                return contents[:n]
        return []

    def _usable_contents(self, body: dict[str, object]) -> list[str]:
        contents: list[str] = []
        for choice in body.get("choices", []):
            if not isinstance(choice, dict):
                continue
            if choice.get("error"):
                continue
            message = choice.get("message")
            if not isinstance(message, dict):
                continue
            content = message.get("content")
            if isinstance(content, str) and content.strip():
                contents.append(content)
        return contents


llm_client = LLMClient()
