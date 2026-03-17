"""OpenAI client wrapper."""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, Optional

from openai import OpenAI, OpenAIError

from ..config.settings import Settings
from ..shared.exceptions import ExternalServiceError


class OpenAIClient:
    def __init__(
        self,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
        timeout_seconds: Optional[int] = None,
        retry_attempts: Optional[int] = None,
    ):
        settings = Settings()
        key = api_key or settings.openai_api_key
        if not key:
            raise ValueError("OPENAI_API_KEY is required.")

        self.client = OpenAI(api_key=key, timeout=timeout_seconds or settings.request_timeout_seconds)
        self.model = model or settings.openai_model
        self.temperature = float(temperature if temperature is not None else settings.openai_temperature)
        self.max_tokens = int(max_tokens if max_tokens is not None else settings.openai_max_tokens)
        self.retry_attempts = retry_attempts or settings.http_retry_count
        self.retry_backoff = settings.http_retry_backoff
        self.logger = logging.getLogger(self.__class__.__name__)

    def generate(
        self,
        system_prompt: str,
        user_prompt: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> str:
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": self._build_user_content(user_prompt, context)},
        ]
        response = self.chat_completion(messages=messages)
        return response.get("content", "")

    def chat_completion(self, messages: list[Dict[str, Any]], **kwargs: Any) -> Dict[str, Any]:
        request_payload: Dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
        }
        request_payload.update(kwargs)

        for attempt in range(1, self.retry_attempts + 1):
            self.logger.debug("OpenAI request start: model=%s attempt=%s", self.model, attempt)
            try:
                result = self.client.chat.completions.create(**request_payload)
                choice = result.choices[0]
                self.logger.info("OpenAI request success: model=%s id=%s", self.model, result.id)
                return {
                    "id": result.id,
                    "model": result.model,
                    "content": getattr(choice.message, "content", "") or "",
                    "usage": result.usage.model_dump() if result.usage else None,
                }
            except (OpenAIError, ValueError, KeyError, IndexError) as exc:
                self.logger.warning("OpenAI request failed: attempt=%s error=%s", attempt, exc)
                if attempt >= self.retry_attempts:
                    raise ExternalServiceError(f"OpenAI request failed: {exc}") from exc
                time.sleep(min(self.retry_backoff * 2 ** (attempt - 1), self.retry_backoff * 5))

        raise ExternalServiceError("OpenAI request failed after retries")

    @staticmethod
    def _build_user_content(user_prompt: str, context: Optional[Dict[str, Any]]) -> str:
        if not context:
            return user_prompt
        return f"{user_prompt}\n\nContext:\n{context}"
