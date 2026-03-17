"""High-level generation orchestration wrapper."""

from __future__ import annotations

from typing import Any, Mapping, Optional, Sequence

from ...clients.openai_client import OpenAIClient
from .pipeline import GenerationPipeline, GeneratedArticle
from ...shared.models import GenerationInput


class TextGenerator:
    """Compatibility wrapper around [GenerationPipeline]."""

    def __init__(self, openai_client: OpenAIClient):
        self.client = openai_client

    def generate(
        self,
        request: GenerationInput,
        places: Sequence[Mapping[str, Any]],
        scenario: str = "solo_travel",
        duration_days: Optional[int] = None,
        budget_level: Optional[str] = None,
        tone: str = "friendly",
        **extra: Any,
    ) -> GeneratedArticle:
        pipeline = GenerationPipeline(
            openai_client=self.client,
            scenario=scenario,
            locale="ko",
        )
        return pipeline.generate_article(
            places=places,
            region=request.region,
            duration_days=duration_days,
            budget_level=budget_level,
            tone=tone,
            extra_context={"extra": extra, **request.context} if request.context else {"extra": extra},
        )
