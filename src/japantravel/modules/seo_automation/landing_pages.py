"""Build deterministic programmatic SEO page payloads from place data."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence
import re

from ..generation.seo import (
    build_keyword_list,
    build_meta_description,
    build_title_tag,
    infer_content_category,
    infer_schema_type,
    to_plain_text,
)
from .planner import SeoKeywordTarget


@dataclass
class ProgrammaticSeoPage:
    target: SeoKeywordTarget
    payload: dict[str, Any]

    def to_payload(self) -> dict[str, Any]:
        return dict(self.payload)


def select_places_for_keyword(
    target: SeoKeywordTarget,
    places: Sequence[Mapping[str, Any]],
    max_items: int = 6,
) -> list[dict[str, Any]]:
    detail_tokens = _tokenize(target.detail_name)
    region_tokens = _tokenize(target.region_name)
    category_tokens = _tokenize(target.category_name)

    scored: list[tuple[float, dict[str, Any]]] = []
    for raw in places:
        item = dict(raw)
        haystack = " ".join(
            [
                to_plain_text(item.get("name")),
                to_plain_text(item.get("address")),
                to_plain_text(item.get("city")),
                to_plain_text(item.get("country")),
                to_plain_text(item.get("category")),
                " ".join(to_plain_text(value) for value in item.get("subcategories", []) if to_plain_text(value)),
            ]
        ).lower()
        score = 0.0
        if any(token in haystack for token in region_tokens):
            score += 3.0
        if any(token in haystack for token in detail_tokens):
            score += 5.0
        if any(token in haystack for token in category_tokens):
            score += 4.0
        score += min(float(item.get("rating") or 0.0), 5.0)
        score += min(int(item.get("review_count") or 0), 3000) / 1000.0
        scored.append((score, item))

    scored.sort(key=lambda pair: pair[0], reverse=True)
    return [item for _, item in scored[: max(max_items, 1)]]


def build_programmatic_page_payload(
    target: SeoKeywordTarget,
    places: Sequence[Mapping[str, Any]],
    internal_links: Mapping[str, Sequence[Mapping[str, Any]]] | None = None,
) -> ProgrammaticSeoPage:
    selected = select_places_for_keyword(target, places)
    content_category = target.category_name or infer_content_category([item.get("category") for item in selected if isinstance(item, Mapping)])
    primary_keyword = target.keyword
    place_sections = [_place_section(item, target.region_name, content_category, primary_keyword) for item in selected[:6]]
    summary = _build_summary(target, selected, content_category)
    intro = _build_intro(target, selected, content_category)
    tips = _build_visit_tips(target, selected)
    checklist = _build_checklist(target, selected)
    faq = _build_faq(target, content_category)
    conclusion = _build_conclusion(target, selected)
    keywords = build_keyword_list(
        primary_keyword=primary_keyword,
        secondary_topics=[item.get("place_name", "") for item in place_sections],
        region=target.region_name,
        place_name=target.detail_name,
        content_category=content_category,
    )
    payload = {
        "title": primary_keyword,
        "summary": summary,
        "intro": intro,
        "region": target.region_name,
        "scenario": "programmatic_seo",
        "place_sections": place_sections,
        "route_suggestion": tips,
        "checklist": checklist,
        "faq": faq,
        "conclusion": conclusion,
        "place_snapshots": list(selected),
        "internal_links": dict(internal_links or {}),
        "seo": {
            "primary_keyword": primary_keyword,
            "secondary_topics": [item.get("place_name", "") for item in place_sections if item.get("place_name")],
            "meta_description": build_meta_description(
                title=primary_keyword,
                summary=summary,
                intro=intro,
                region=target.region_name,
            ),
            "title_tag": target.title_tag or build_title_tag(target.region_name, target.leaf_name),
            "keywords": keywords,
            "canonical_path": target.canonical_path,
            "schema_type": target.schema_type or infer_schema_type([content_category]),
            "content_category": content_category,
        },
    }
    return ProgrammaticSeoPage(target=target, payload=payload)


def _place_section(place: Mapping[str, Any], region_name: str, category_name: str, primary_keyword: str) -> dict[str, Any]:
    name = to_plain_text(place.get("name")) or "추천 장소"
    address = to_plain_text(place.get("address"))
    rating = float(place.get("rating") or 0.0)
    review_count = int(place.get("review_count") or 0)
    category = to_plain_text(place.get("category")) or category_name or "장소"
    body_parts = [
        f"{name}는 {region_name}에서 {category_name or category} 정보를 찾을 때 함께 보기 좋은 후보입니다.",
    ]
    if address:
        body_parts.append(f"위치는 {address} 기준으로 확인할 수 있어 동선 정리에 도움이 됩니다.")
    if rating > 0:
        body_parts.append(f"평점은 {rating:.1f}점, 리뷰 수는 {review_count:,}건 수준이라 현장 반응을 가늠하기 좋습니다.")
    body_parts.append("방문 전 운영시간, 휴무일, 예약 가능 여부는 최신 공지를 다시 확인하는 편이 안전합니다.")
    return {
        "place_id": to_plain_text(place.get("place_id") or place.get("source_id") or place.get("id")),
        "place_name": name,
        "title": name,
        "body": "\n\n".join(body_parts),
        "image_urls": list(place.get("image_urls") or [])[:2],
        "maps_url": to_plain_text(place.get("maps_url")),
        "map_embed_url": "",
        "address": address,
        "category": category,
        "primary_keyword": primary_keyword,
    }


def _build_summary(target: SeoKeywordTarget, selected: Sequence[Mapping[str, Any]], category_name: str) -> str:
    if selected:
        names = ", ".join(to_plain_text(item.get("name")) for item in selected[:3] if to_plain_text(item.get("name")))
        return (
            f"{target.keyword} 정보를 빠르게 정리할 수 있도록 {target.region_name} 기준 후보를 선별했습니다. "
            f"{names}처럼 실제로 비교하기 쉬운 장소를 중심으로 기본 정보와 방문 포인트를 묶었습니다."
        )
    return f"{target.keyword} 정보를 찾는 여행자를 위해 {target.region_name} {category_name} 후보와 방문 팁을 정리했습니다."


def _build_intro(target: SeoKeywordTarget, selected: Sequence[Mapping[str, Any]], category_name: str) -> str:
    candidate_count = len(selected)
    return (
        f"{target.keyword}처럼 지역과 세부 주제가 함께 들어간 검색은 현장에서 바로 비교할 수 있는 정리형 정보가 중요합니다. "
        f"이 페이지는 {target.region_name}에서 {category_name or '장소'} 후보 {candidate_count}곳을 기준으로 기본 정보, 추천 이유, 방문 전 체크 포인트를 한 번에 볼 수 있게 구성했습니다. "
        f"짧은 일정에서도 바로 참고할 수 있도록 내부 링크와 FAQ까지 함께 정리했습니다."
    )


def _build_visit_tips(target: SeoKeywordTarget, selected: Sequence[Mapping[str, Any]]) -> str:
    if not selected:
        return f"{target.keyword}를 찾을 때는 운영시간, 휴무일, 예약 가능 여부를 먼저 확인하세요."
    names = [to_plain_text(item.get("name")) for item in selected[:3] if to_plain_text(item.get("name"))]
    sequence = " → ".join(names)
    return (
        f"방문 순서는 {sequence}처럼 가까운 후보를 묶어서 보는 편이 효율적입니다. "
        f"{target.region_name}은 시간대에 따라 혼잡도가 크게 달라질 수 있으므로 오전/오후로 나눠 동선을 잡는 편이 무난합니다. "
        "비 오는 날, 휴무일, 마지막 입장 시간 같은 변수는 출발 전에 다시 확인하세요."
    )


def _build_checklist(target: SeoKeywordTarget, selected: Sequence[Mapping[str, Any]]) -> list[str]:
    items = [
        f"{target.region_name} 이동 동선과 가장 가까운 역을 먼저 확인하기",
        "운영시간과 정기 휴무일을 방문 전 다시 확인하기",
        "예약 가능 여부와 대기 시간을 미리 체크하기",
        "현금/카드/QR 결제 가능 여부 확인하기",
        "비 오는 날 대체 동선을 하나 더 준비하기",
    ]
    if selected:
        items.append(f"{to_plain_text(selected[0].get('name'))} 주변의 다른 후보도 함께 비교하기")
    return items[:8]


def _build_faq(target: SeoKeywordTarget, category_name: str) -> list[dict[str, str]]:
    questions = [
        ("언제 가는 편이 좋나요?", f"{target.region_name} 일정과 혼잡도에 따라 다르지만, 피크 시간대를 피해서 비교하면 선택이 수월합니다."),
        ("예약이 필요한가요?", "인기 장소는 예약이나 대기 가능성을 함께 확인하는 편이 안전합니다."),
        ("대중교통으로 이동하기 쉬운가요?", f"{target.region_name}의 주요 역과 버스 정류장 기준으로 접근성을 먼저 체크하면 동선이 훨씬 단순해집니다."),
        ("비 오는 날에도 괜찮나요?", f"{category_name or '장소'} 특성에 따라 다르므로 실내/실외 여부와 우천 시 대체 후보를 함께 보는 편이 좋습니다."),
        ("짧은 일정에도 볼 수 있나요?", "핵심 후보 2~3곳만 먼저 비교하면 반나절 일정에도 충분히 활용할 수 있습니다."),
    ]
    return [{"question": question, "answer": answer} for question, answer in questions[:5]]


def _build_conclusion(target: SeoKeywordTarget, selected: Sequence[Mapping[str, Any]]) -> str:
    if selected:
        first = to_plain_text(selected[0].get("name"))
        return (
            f"{target.keyword} 정보를 찾을 때는 대표 후보 하나만 보기보다 {first}처럼 비교 기준이 분명한 장소를 함께 보는 편이 더 안정적입니다. "
            "방문 직전 최신 운영 정보만 다시 확인하면 검색에서 바로 행동으로 이어지기 쉬운 페이지가 됩니다."
        )
    return f"{target.keyword} 페이지는 검색 의도에 맞는 기본 정보와 방문 팁을 빠르게 확인할 수 있도록 구성했습니다."


def _tokenize(value: str) -> set[str]:
    return {token for token in re.split(r"[\s,/]+", to_plain_text(value).lower()) if token}
