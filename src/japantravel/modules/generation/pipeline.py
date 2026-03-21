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
from .seo import (
    build_meta_description,
    build_primary_keyword,
    build_secondary_topics,
    normalize_generated_text,
)


@dataclass
class PlaceSection:
    """A single place-focused section in the final article."""

    place_id: str
    place_name: str
    title: str
    body: str
    image_urls: list[str]
    maps_url: str
    map_embed_url: str


@dataclass
class SeoMetadata:
    """Lightweight SEO metadata stored alongside a generated article."""

    primary_keyword: str
    secondary_topics: List[str]
    meta_description: str

    def to_payload(self) -> Dict[str, Any]:
        return {
            "primary_keyword": self.primary_keyword,
            "secondary_topics": list(self.secondary_topics),
            "meta_description": self.meta_description,
        }


@dataclass
class GeneratedArticle:
    """Structured Korean draft output."""

    title: str
    summary: str
    intro: str
    place_sections: List[PlaceSection]
    route_suggestion: str
    checklist: List[str]
    faq: List[Dict[str, str]]
    conclusion: str
    seo: SeoMetadata

    def to_payload(self) -> Dict[str, Any]:
        return {
            "title": self.title,
            "summary": self.summary,
            "intro": self.intro,
            "place_sections": [
                {
                    "place_id": section.place_id,
                    "place_name": section.place_name,
                    "title": section.title,
                    "body": section.body,
                }
                if not (section.image_urls or section.maps_url or section.map_embed_url)
                else {
                    "place_id": section.place_id,
                    "place_name": section.place_name,
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
            "seo": self.seo.to_payload(),
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
        seo = SeoMetadata(
            primary_keyword=build_primary_keyword(
                title=title,
                region=str(draft_context.get("region", "")),
                scenario=str(draft_context.get("scenario", self.scenario)),
            ),
            secondary_topics=build_secondary_topics([section.place_name for section in place_sections]),
            meta_description=build_meta_description(
                title=title,
                summary=summary,
                intro=intro,
                region=str(draft_context.get("region", "")),
            ),
        )

        return GeneratedArticle(
            title=title,
            summary=summary,
            intro=intro,
            place_sections=place_sections,
            route_suggestion=route_suggestion,
            checklist=checklist,
            faq=faq,
            conclusion=conclusion,
            seo=seo,
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
        prompt_values = self._prompt_values(context)
        user_prompt = templates.TITLE_PROMPT.format(
            region=context["region"],
            scenario=prompt_values["scenario"],
            audience=prompt_values["audience"],
            duration_days=prompt_values["duration_days"],
            content_angle=prompt_values["content_angle"],
            title_family=prompt_values["title_family"],
            title_hook=prompt_values["title_hook"],
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
            raw = re.sub(r"^\s*(제목|타이틀|title)\s*[:：\-]\s*", "", raw, flags=re.IGNORECASE)
            raw = raw.strip("\"'[]() ")
            if raw:
                title = raw
                break
        else:
            title = str(raw_title).strip()

        title = re.sub(r"^\s*(제목|타이틀|title)\s*[:：\-]\s*", "", title, flags=re.IGNORECASE)
        title = title.strip("\"'[]() ")
        title = re.sub(r"\s{2,}", " ", title)

        if len(title) > max_len:
            title = title[: max_len - 1].rstrip()
            return f"{title}…"
        return title

    def _generate_summary(self, selected: Sequence[Mapping[str, Any]], context: Mapping[str, Any]) -> str:
        prompt_values = self._prompt_values(context)
        user_prompt = templates.SUMMARY_PROMPT.format(
            region=context["region"],
            scenario=prompt_values["scenario"],
            audience=prompt_values["audience"],
            content_angle=prompt_values["content_angle"],
            place_count=len(selected),
        )
        raw = self.client.generate(
            system_prompt=templates.SYSTEM,
            user_prompt=user_prompt,
            context={"selected_places": selected, **context},
        )
        return normalize_generated_text(raw, drop_heading_lines=True)

    def _generate_intro(self, selected: Sequence[Mapping[str, Any]], context: Mapping[str, Any]) -> str:
        prompt_values = self._prompt_values(context)
        user_prompt = templates.INTRO_PROMPT.format(
            region=context["region"],
            scenario=prompt_values["scenario"],
            audience=prompt_values["audience"],
            duration_days=prompt_values["duration_days"],
            content_angle=prompt_values["content_angle"],
        )
        raw = self.client.generate(
            system_prompt=templates.SYSTEM,
            user_prompt=user_prompt,
            context={"selected_places": selected, **context},
        )
        return normalize_generated_text(raw, drop_heading_lines=True)

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
            prompt_values = self._prompt_values(context)
            section_prompt = templates.PLACE_SECTION_PROMPT.format(
                name=name,
                scenario=prompt_values["scenario"],
                content_angle=prompt_values["content_angle"],
                audience=prompt_values["audience"],
            )
            content = self._retry_generate(
                system_prompt=templates.SYSTEM_PLACE_SECTION,
                user_prompt=section_prompt,
                context={"place": place, "display_rating": rating, "review_count": review_count, **context},
            )
            title = self._build_section_title(name=name)
            sections.append(
                PlaceSection(
                    place_id=str(place.get("id", place.get("place_id", ""))),
                    place_name=name,
                    title=title,
                    body=normalize_generated_text(content, drop_heading_lines=True),
                    image_urls=self._collect_image_urls(place),
                    maps_url=str(place.get("maps_url", "") or ""),
                    map_embed_url=str(place.get("maps_embed_url", "") or ""),
                )
            )
        return sections

    @staticmethod
    def _build_section_title(name: str) -> str:
        cleaned = normalize_generated_text(name, drop_heading_lines=True)
        return cleaned or "추천 장소"

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
        prompt_values = self._prompt_values(context)
        user_prompt = templates.ROUTE_PROMPT.format(
            region=context["region"],
            scenario=prompt_values["scenario"],
            audience=prompt_values["audience"],
            content_angle=prompt_values["content_angle"],
            duration=prompt_values["duration_days"],
        )
        raw = self.client.generate(
            system_prompt=templates.SYSTEM,
            user_prompt=user_prompt,
            context={"selected_places": selected, **context},
        )
        return self._normalize_route_suggestion(raw, selected=selected)

    def _generate_checklist(
        self,
        selected: Sequence[Mapping[str, Any]],
        context: Mapping[str, Any],
    ) -> List[str]:
        prompt_values = self._prompt_values(context)
        user_prompt = templates.CHECKLIST_PROMPT.format(
            scenario=prompt_values["scenario"],
            region=context["region"],
            audience=prompt_values["audience"],
            duration_days=prompt_values["duration_days"],
            content_angle=prompt_values["content_angle"],
        )
        raw = self.client.generate(
            system_prompt=templates.SYSTEM,
            user_prompt=user_prompt,
            context={"selected_places": selected, **context},
        )
        return self._normalize_checklist(
            self._split_bullets(raw),
            scenario=str(context.get("scenario", self.scenario)),
        )

    def _generate_faq(
        self,
        selected: Sequence[Mapping[str, Any]],
        context: Mapping[str, Any],
    ) -> List[Dict[str, str]]:
        prompt_values = self._prompt_values(context)
        user_prompt = templates.FAQ_PROMPT.format(
            region=context["region"],
            scenario=prompt_values["scenario"],
            audience=prompt_values["audience"],
            content_angle=prompt_values["content_angle"],
        )
        raw = self.client.generate(
            system_prompt=templates.SYSTEM,
            user_prompt=user_prompt,
            context={"selected_places": selected, **context},
        )
        return self._split_qa(raw)

    def _generate_conclusion(self, selected: Sequence[Mapping[str, Any]], context: Mapping[str, Any]) -> str:
        prompt_values = self._prompt_values(context)
        user_prompt = templates.CONCLUSION_PROMPT.format(
            scenario=prompt_values["scenario"],
            region=context["region"],
            audience=prompt_values["audience"],
            content_angle=prompt_values["content_angle"],
        )
        raw = self.client.generate(
            system_prompt=templates.SYSTEM,
            user_prompt=user_prompt,
            context={"selected_places": selected, **context},
        )
        return normalize_generated_text(raw, drop_heading_lines=True)

    def _prompt_values(self, context: Mapping[str, Any]) -> Dict[str, Any]:
        scenario = self._scenario_label(context.get("scenario", self.scenario))
        duration_days = self._to_int(context.get("duration_days", 1)) or 1
        return {
            "scenario": scenario,
            "duration_days": duration_days,
            "content_angle": self._to_prompt_text(
                context.get("content_angle_label") or context.get("content_angle"),
                fallback="지역 대표 포인트 중심",
            ),
            "audience": self._to_prompt_text(
                context.get("audience_label") or context.get("audience"),
                fallback="한국어 여행 독자",
            ),
            "title_family": self._to_prompt_text(
                context.get("title_family_label") or context.get("title_family"),
                fallback="정보형 제목",
            ),
            "title_hook": self._to_prompt_text(
                context.get("title_hook"),
                fallback="현실적인 동선과 포인트",
            ),
        }

    @staticmethod
    def _to_prompt_text(value: Any, fallback: str) -> str:
        cleaned = normalize_generated_text(value, drop_heading_lines=True)
        return cleaned or fallback

    @staticmethod
    def _scenario_label(value: Any) -> str:
        mapping = {
            "solo_travel": "혼자 여행",
            "rainy_day": "비 오는 날 여행",
            "parents_trip": "부모님과 함께하는 여행",
        }
        raw = str(value or "").strip()
        return mapping.get(raw, raw.replace("_", " ") or "여행")

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
    def _normalize_route_suggestion(raw: str, selected: Sequence[Mapping[str, Any]]) -> str:
        cleaned = normalize_generated_text(raw, drop_heading_lines=True)
        cleaned = re.sub(r"\r\n?", "\n", cleaned)
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
        if re.search(r"(?m)^\s*1\.\s+", cleaned) and re.search(r"(?m)^\s*2\.\s+", cleaned) and re.search(r"(?m)^\s*3\.\s+", cleaned):
            return cleaned

        paragraphs = [part.strip() for part in re.split(r"\n\s*\n", cleaned) if part.strip()]
        if not paragraphs:
            place_names = [str(place.get("name", "")).strip() for place in selected if str(place.get("name", "")).strip()]
            start_name = place_names[0] if place_names else "첫 장소"
            mid_names = ", ".join(place_names[1:3]) if len(place_names) > 1 else "주요 장소"
            end_name = place_names[-1] if place_names else "마지막 장소"
            paragraphs = [
                f"{start_name}부터 시작하는 흐름이 무난합니다. 출발 전 첫 이동편과 운영시간을 먼저 확인하세요.",
                f"{mid_names} 순서로 이동하면 되돌아가는 구간을 줄이기 좋습니다. 구간 이동은 대중교통 기준으로 짧게 끊어 가는 편이 안전합니다.",
                f"{end_name} 방문 뒤에는 복귀 동선을 먼저 확정하세요. 막차나 배편 시간은 당일 다시 확인하는 편이 좋습니다.",
            ]

        while len(paragraphs) < 3:
            paragraphs.append(paragraphs[-1] if paragraphs else "현장 상황에 따라 순서를 조정하세요.")

        return (
            f"1. 시작 루트\n{paragraphs[0]}\n\n"
            f"2. 이동 동선\n{paragraphs[1]}\n\n"
            f"3. 마무리 루트\n{paragraphs[2]}"
        )

    def _normalize_checklist(self, items: Sequence[str], scenario: str) -> List[str]:
        normalized: List[str] = []
        for item in items:
            text = re.sub(r"^\s*(?:-|\*|•|\[ \]|\d+[\.\)])\s*", "", str(item or "").strip())
            text = re.sub(r"\s+", " ", text).strip()
            if text and text not in normalized:
                normalized.append(text)

        for fallback in self._default_checklist_items(scenario):
            if fallback not in normalized:
                normalized.append(fallback)

        return normalized[:8]

    @staticmethod
    def _default_checklist_items(scenario: str) -> List[str]:
        base_items = [
            "운영시간과 당일 휴무 여부를 다시 확인하기",
            "버스, 배편, 막차 시간표를 출발 전에 체크하기",
            "현금 또는 카드 결제 가능 여부를 미리 확인하기",
            "바람, 비, 체감온도까지 포함한 날씨를 확인하기",
            "오프라인 지도와 데이터 신호 상태를 대비해 두기",
            "걷는 거리와 신발 컨디션을 출발 전에 점검하기",
            "입장 제한, 촬영 제한, 출입 가능 시간대를 확인하기",
            "변수 발생 시 들를 대체 장소 한 곳을 메모해 두기",
        ]
        scenario_items = {
            "solo_travel": [
                "야간 복귀 동선과 주변 조명 상태를 확인하기",
                "혼자 식사 가능한 시간대와 대기 시간을 체크하기",
            ],
            "rainy_day": [
                "우천 시 실내 대체 코스를 함께 정리해 두기",
                "우산보다 우비가 편한지 이동 동선을 기준으로 판단하기",
            ],
            "parents_trip": [
                "계단, 경사, 화장실 접근성을 먼저 확인하기",
                "장시간 대기 없이 쉴 수 있는 포인트를 확보하기",
            ],
        }
        return list(dict.fromkeys(scenario_items.get(scenario, []) + base_items))

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
    def _split_qa(raw: str) -> List[Dict[str, str]]:
        if not raw:
            return []

        normalized = str(raw).replace("<br>", "\n").replace("<br/>", "\n").strip()
        normalized = re.sub(r"\r\n?", "\n", normalized)
        normalized = normalized.replace("\n\n", "\n")

        pairs = GenerationPipeline._extract_qa_pairs(normalized)
        if pairs:
            return pairs[:10]

        lines = [line.strip() for line in normalized.splitlines() if line.strip()]
        if not lines:
            return []

        fallback: List[Dict[str, str]] = []
        current_question = ""
        current_answer_parts: List[str] = []
        for line in lines:
            normalized_line = line.lstrip("- •*").strip()
            if not normalized_line:
                continue

            m = re.match(r"(?is)Q\s*[:.]?\s*(.+?)\s*A\s*[:.]?\s*(.+)", normalized_line)
            if m:
                question = re.sub(r"\s+", " ", (m.group(1) or "").strip())
                answer = re.sub(r"\s+", " ", (m.group(2) or "").strip())
                if question and answer:
                    if current_question and current_answer_parts:
                        fallback.append(
                            {
                                "question": current_question,
                                "answer": re.sub(r"\s+", " ", " ".join(current_answer_parts)).strip(),
                            }
                        )
                    current_question = ""
                    current_answer_parts = []
                    fallback.append({"question": question, "answer": answer})
                    continue

            q_match = re.match(r"(?is)^Q\s*[:.]?\s*(.+)$", normalized_line)
            if q_match:
                if current_question and current_answer_parts:
                    fallback.append(
                        {
                            "question": current_question,
                            "answer": re.sub(r"\s+", " ", " ".join(current_answer_parts)).strip(),
                        }
                    )
                current_question = re.sub(r"\s+", " ", (q_match.group(1) or "").strip())
                current_answer_parts = []
                continue

            a_match = re.match(r"(?is)^A\s*[:.]?\s*(.+)$", normalized_line)
            if a_match:
                answer_line = re.sub(r"\s+", " ", (a_match.group(1) or "").strip())
                if current_question and answer_line:
                    current_answer_parts.append(answer_line)
                continue

            if current_question and current_answer_parts:
                current_answer_parts.append(re.sub(r"\s+", " ", normalized_line))
            elif current_question:
                current_question = re.sub(r"\s+", " ", f"{current_question} {normalized_line}").strip()

        if current_question and current_answer_parts:
            fallback.append(
                {
                    "question": current_question,
                    "answer": re.sub(r"\s+", " ", " ".join(current_answer_parts)).strip(),
                }
            )

        return fallback[:10]

    @staticmethod
    def _extract_qa_pairs(normalized: str) -> List[Dict[str, str]]:
        qa_pattern = re.compile(
            r"(?is)(?:^|\n)\s*(?:-\s*|\d+[\.\)]\s*)?\s*Q\s*[:.]?\s*(.+?)\s*(?:\n|$)\s*A\s*[:.]?\s*(.+?)(?=(?:\n|^)\s*(?:-|\d+[\.\)]\s*)?\s*Q\s*[:.]|\Z)",
            re.MULTILINE,
        )
        faq: List[Dict[str, str]] = []
        for match in qa_pattern.finditer(normalized):
            question = re.sub(r"\s+", " ", (match.group(1) or "").strip())
            answer = re.sub(r"\s+", " ", (match.group(2) or "").strip())
            if question and answer:
                faq.append({"question": question, "answer": answer})

        return faq

    @staticmethod
    def _place_names(selected: Sequence[Mapping[str, Any]]) -> str:
        names = [str(place.get("name", "")).strip() for place in selected]
        names = [name for name in names if name]
        return ", ".join(names)
