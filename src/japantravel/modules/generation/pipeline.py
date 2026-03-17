"""Generation pipeline for Korean travel recommendation drafts.

The pipeline accepts selected place candidates and produces an article draft with
sections:
- title
- summary
- intro
- place sections
- route suggestion
- checklist
- FAQ
- conclusion

It is intentionally a skeleton focused on orchestration and extensibility.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Dict, List, Mapping, Optional, Sequence

from ...clients.openai_client import OpenAIClient
from ...shared.exceptions import ExternalServiceError
from . import prompt_templates as templates


@dataclass
class PlaceSection:
    """A single place-focused section in the final article."""

    place_id: str
    title: str
    body: str
    image_urls: list[str]
    maps_url: str
    map_embed_url: str


@dataclass
class GeneratedArticle:
    """Structured Korean draft output."""

    title: str
    summary: str
    intro: str
    place_sections: List[PlaceSection]
    route_suggestion: str
    checklist: List[str]
    faq: List[str]
    conclusion: str

    def to_payload(self) -> Dict[str, Any]:
        return {
            "title": self.title,
            "summary": self.summary,
            "intro": self.intro,
            "place_sections": [
                {"place_id": section.place_id, "title": section.title, "body": section.body}
                if not (section.image_urls or section.maps_url or section.map_embed_url)
                else {
                    "place_id": section.place_id,
                    "title": section.title,
                    "body": section.body,
                    "image_urls": section.image_urls,
                    "maps_url": section.maps_url,
                    "map_embed_url": section.map_embed_url,
                }
                for section in self.place_sections
            ],
            "route_suggestion": self.route_suggestion,
            "checklist": self.checklist,
            "faq": self.faq,
            "conclusion": self.conclusion,
        }


class GenerationPipeline:
    """Build a Korean travel recommendation draft from selected places."""

    def __init__(
        self,
        openai_client: OpenAIClient,
        scenario: str = "solo_travel",
        locale: str = "ko",
        max_sections: int = 6,
        section_retry: int = 1,
    ):
        self.client = openai_client
        self.scenario = scenario
        self.locale = locale
        self.max_sections = max_sections
        self.section_retry = section_retry

    def generate_article(
        self,
        places: Sequence[Mapping[str, Any]],
        region: str,
        duration_days: Optional[int] = None,
        budget_level: str | None = None,
        tone: str = "friendly",
        extra_context: Optional[Mapping[str, Any]] = None,
    ) -> GeneratedArticle:
        """Orchestrate the whole pipeline and return structured draft output."""

        selected = list(places)[: self.max_sections]
        draft_context = self._build_context(
            region=region,
            duration_days=duration_days,
            budget_level=budget_level,
            tone=tone,
            extra_context=extra_context,
        )

        title = self._generate_title(selected=selected, context=draft_context)
        summary = self._generate_summary(selected=selected, context=draft_context)
        intro = self._generate_intro(selected=selected, context=draft_context)
        place_sections = self._generate_place_sections(selected=selected, context=draft_context)
        route_suggestion = self._generate_route_suggestion(selected=selected, context=draft_context)
        checklist = self._generate_checklist(selected=selected, context=draft_context)
        faq = self._generate_faq(selected=selected, context=draft_context)
        conclusion = self._generate_conclusion(selected=selected, context=draft_context)

        return GeneratedArticle(
            title=title,
            summary=summary,
            intro=intro,
            place_sections=place_sections,
            route_suggestion=route_suggestion,
            checklist=checklist,
            faq=faq,
            conclusion=conclusion,
        )

    def _build_context(
        self,
        region: str,
        duration_days: Optional[int],
        budget_level: Optional[str],
        tone: str,
        extra_context: Optional[Mapping[str, Any]],
    ) -> Dict[str, Any]:
        context = {
            "region": region,
            "scenario": self.scenario,
            "locale": self.locale,
            "duration_days": duration_days,
            "budget_level": budget_level,
            "tone": tone,
        }
        if extra_context:
            context.update(extra_context)
        return context

    def _generate_title(self, selected: Sequence[Mapping[str, Any]], context: Mapping[str, Any]) -> str:
        user_prompt = templates.TITLE_PROMPT.format(
            region=context["region"],
            scenario=context["scenario"],
            place_count=len(selected),
            names=self._place_names(selected),
        )
        raw_title = self.client.generate(system_prompt=templates.SYSTEM, user_prompt=user_prompt, context=context)
        return self._normalize_title(raw_title)

    @staticmethod
    def _normalize_title(raw_title: str, max_len: int = 48) -> str:
        if not raw_title:
            return "여행 추천 글"

        for line in str(raw_title).splitlines():
            line = line.strip()
            if not line:
                continue
            raw = re.sub(r"^\s*#{1,6}\s+", "", line)
            raw = raw.replace("**", "").replace("`", "").strip()
            if raw:
                title = raw
                break
        else:
            title = str(raw_title).strip()

        if len(title) > max_len:
            title = title[: max_len - 1].rstrip()
            return f"{title}…"
        return title

    def _generate_summary(self, selected: Sequence[Mapping[str, Any]], context: Mapping[str, Any]) -> str:
        user_prompt = templates.SUMMARY_PROMPT.format(
            region=context["region"],
            scenario=context["scenario"],
            place_count=len(selected),
        )
        return self.client.generate(system_prompt=templates.SYSTEM, user_prompt=user_prompt, context={"selected_places": selected, **context})

    def _generate_intro(self, selected: Sequence[Mapping[str, Any]], context: Mapping[str, Any]) -> str:
        user_prompt = templates.INTRO_PROMPT.format(region=context["region"], scenario=context["scenario"])
        return self.client.generate(system_prompt=templates.SYSTEM, user_prompt=user_prompt, context={"selected_places": selected, **context})

    def _generate_place_sections(
        self,
        selected: Sequence[Mapping[str, Any]],
        context: Mapping[str, Any],
    ) -> List[PlaceSection]:
        sections: List[PlaceSection] = []
        for place in selected:
            if place.get("name") is None:
                continue
            name = str(place.get("name", ""))
            rating = self._to_display_rating(place)
            review_count = self._to_int(place.get("review_count", 0))
            section_prompt = templates.PLACE_SECTION_PROMPT.format(
                name=name,
                scenario=context["scenario"],
            )
            content = self._retry_generate(
                system_prompt=templates.SYSTEM_PLACE_SECTION,
                user_prompt=section_prompt,
                context={"place": place, "display_rating": rating, "review_count": review_count, **context},
            )
            title = self._build_section_title(name=name, rating=rating, review_count=review_count)
            sections.append(
                PlaceSection(
                    place_id=str(place.get("id", place.get("place_id", ""))),
                    title=title,
                    body=content,
                    image_urls=self._collect_image_urls(place),
                    maps_url=str(place.get("maps_url", "") or ""),
                    map_embed_url=str(place.get("maps_embed_url", "") or ""),
                )
            )
        return sections

    @staticmethod
    def _build_section_title(name: str, rating: str, review_count: int) -> str:
        badge = "🌟"
        if review_count > 3000:
            badge = "🔥"
        if rating and rating != "0.0":
            return f"{badge} {name} ({rating}, 리뷰 {review_count}개)"
        if review_count:
            return f"{badge} {name} (리뷰 {review_count}개)"
        return f"{badge} {name} (새로운 후보)"

    @staticmethod
    def _to_display_rating(place: Mapping[str, Any]) -> str:
        raw = place.get("rating")
        try:
            value = float(raw)
            return f"{value:.1f} / 5.0"
        except (TypeError, ValueError):
            return ""

    @staticmethod
    def _to_int(value: Any) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _collect_image_urls(place: Mapping[str, Any]) -> List[str]:
        raw: list[str] = []
        for key in ("image_urls", "images", "photos", "image", "photo", "photoUrl", "imageUrl"):
            value = place.get(key)
            if isinstance(value, str):
                raw.append(value)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, str):
                        raw.append(item)
                    elif isinstance(item, Mapping) and isinstance(item.get("url"), str):
                        raw.append(item.get("url", ""))
        return list(dict.fromkeys([item.strip() for item in raw if isinstance(item, str) and item.strip()]))

    def _generate_route_suggestion(
        self,
        selected: Sequence[Mapping[str, Any]],
        context: Mapping[str, Any],
    ) -> str:
        user_prompt = templates.ROUTE_PROMPT.format(
            region=context["region"],
            scenario=context["scenario"],
            duration=context.get("duration_days") or 1,
        )
        return self.client.generate(
            system_prompt=templates.SYSTEM,
            user_prompt=user_prompt,
            context={"selected_places": selected, **context},
        )

    def _generate_checklist(
        self,
        selected: Sequence[Mapping[str, Any]],
        context: Mapping[str, Any],
    ) -> List[str]:
        user_prompt = templates.CHECKLIST_PROMPT.format(
            scenario=context["scenario"],
            region=context["region"],
        )
        raw = self.client.generate(
            system_prompt=templates.SYSTEM,
            user_prompt=user_prompt,
            context={"selected_places": selected, **context},
        )
        return self._split_bullets(raw)

    def _generate_faq(
        self,
        selected: Sequence[Mapping[str, Any]],
        context: Mapping[str, Any],
    ) -> List[str]:
        user_prompt = templates.FAQ_PROMPT.format(
            region=context["region"],
            scenario=context["scenario"],
        )
        raw = self.client.generate(
            system_prompt=templates.SYSTEM,
            user_prompt=user_prompt,
            context={"selected_places": selected, **context},
        )
        return self._split_qa(raw)

    def _generate_conclusion(self, selected: Sequence[Mapping[str, Any]], context: Mapping[str, Any]) -> str:
        user_prompt = templates.CONCLUSION_PROMPT.format(
            scenario=context["scenario"],
            region=context["region"],
        )
        return self.client.generate(
            system_prompt=templates.SYSTEM,
            user_prompt=user_prompt,
            context={"selected_places": selected, **context},
        )

    def _retry_generate(self, system_prompt: str, user_prompt: str, context: Mapping[str, Any]) -> str:
        # Retry only around generation logic where transient model errors may occur
        last_exc: Exception | None = None
        for _ in range(max(1, self.section_retry)):
            try:
                return self.client.generate(system_prompt=system_prompt, user_prompt=user_prompt, context=dict(context))
            except ExternalServiceError as exc:
                last_exc = exc
        if last_exc is not None:
            raise last_exc
        return ""

    @staticmethod
    def _split_bullets(raw: str) -> List[str]:
        lines = [line.strip() for line in raw.splitlines() if line.strip()]
        bullets: List[str] = []
        for line in lines:
            normalized = line.lstrip("- •*").strip()
            if not normalized:
                continue
            if normalized.startswith("Q") and "." in normalized:
                continue
            bullets.append(normalized)
        return bullets[:12]

    @staticmethod
    def _split_qa(raw: str) -> List[str]:
        if not raw:
            return []

        text = str(raw).strip()
        lines = []
        for line in text.splitlines():
            line = line.strip()
            if line:
                lines.append(line)
        if not lines:
            return []

        normalized = "\n".join(lines)

        qa_pattern = re.compile(
            r"(?is)(?:^|\n)\s*(?:-|\d+[\.\)]\s*)?\s*Q\s*[:.]?\s*(.+?)\s*(?:\n|$)\s*A\s*:\s*(.+?)(?=(?:\n|^)\s*(?:-|\d+[\.\)]\s*)?\s*Q\s*[:.]|\Z)",
            re.MULTILINE,
        )
        faq: List[str] = []
        for match in qa_pattern.finditer(normalized):
            question = re.sub(r"\s+", " ", (match.group(1) or "").strip())
            answer = re.sub(r"\s+", " ", (match.group(2) or "").strip())
            if question and answer:
                faq.append(f"Q: {question} A: {answer}")

        if not faq:
            # Fallback: keep existing behavior for non-ideal outputs
            bullets: List[str] = []
            for line in lines:
                normalized_line = line.lstrip("- •*").strip()
                if normalized_line:
                    bullets.append(normalized_line)
            return bullets[:10]

        return faq[:10]

    @staticmethod
    def _place_names(selected: Sequence[Mapping[str, Any]]) -> str:
        names = [str(place.get("name", "")).strip() for place in selected]
        names = [name for name in names if name]
        return ", ".join(names)
