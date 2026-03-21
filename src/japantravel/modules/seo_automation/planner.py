"""Programmatic SEO keyword planning helpers."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Iterable, Sequence

from ..generation.seo import (
    build_canonical_path,
    build_keyword_list,
    build_title_tag,
    infer_content_category,
    infer_schema_type,
    slugify_path_segment,
    to_plain_text,
)

REGION_SLUGS: dict[str, str] = {
    "일본": "japan",
    "japan": "japan",
    "도쿄": "tokyo",
    "tokyo": "tokyo",
    "오사카": "osaka",
    "osaka": "osaka",
    "교토": "kyoto",
    "kyoto": "kyoto",
    "후쿠오카": "fukuoka",
    "fukuoka": "fukuoka",
    "오키나와": "okinawa",
    "okinawa": "okinawa",
    "홋카이도": "hokkaido",
    "hokkaido": "hokkaido",
}

DETAIL_SLUGS: dict[str, str] = {
    "신주쿠": "shinjuku",
    "시부야": "shibuya",
    "아사쿠사": "asakusa",
    "우에노": "ueno",
    "긴자": "ginza",
    "오다이바": "odaiba",
    "난바": "namba",
    "우메다": "umeda",
    "도톤보리": "dotonbori",
    "기온": "gion",
    "아라시야마": "arashiyama",
    "하카타": "hakata",
    "텐진": "tenjin",
    "삿포로": "sapporo",
    "나하": "naha",
    "bunkyo": "bunkyo",
    "adachi": "adachi",
    "toshima": "toshima",
}

CATEGORY_LABELS: dict[str, str] = {
    "카페": "카페",
    "cafe": "카페",
    "맛집": "맛집",
    "restaurant": "맛집",
    "restaurants": "맛집",
    "관광지": "관광지",
    "attraction": "관광지",
    "attractions": "관광지",
    "숙소": "숙소",
    "hotel": "숙소",
    "hotels": "숙소",
    "료칸": "숙소",
    "온천": "온천",
    "market": "시장",
    "시장": "시장",
}

CATEGORY_SLUGS: dict[str, str] = {
    "카페": "cafe",
    "맛집": "restaurants",
    "관광지": "attractions",
    "숙소": "hotels",
    "온천": "onsen",
    "시장": "market",
}

CORE_REGION_DETAILS: dict[str, tuple[str, ...]] = {
    "도쿄": ("신주쿠", "시부야", "아사쿠사", "우에노"),
    "오사카": ("난바", "우메다", "도톤보리"),
    "교토": ("기온", "아라시야마"),
    "후쿠오카": ("하카타", "텐진"),
    "홋카이도": ("삿포로",),
    "오키나와": ("나하",),
}

CORE_CATEGORIES: tuple[str, ...] = ("카페", "맛집", "관광지", "숙소")


@dataclass(frozen=True)
class SeoKeywordTarget:
    keyword: str
    country_name: str
    country_slug: str
    region_name: str
    region_slug: str
    detail_name: str
    leaf_name: str
    leaf_slug: str
    category_name: str
    title_tag: str
    canonical_path: str
    schema_type: str
    keywords: list[str]

    def to_payload(self) -> dict[str, str | list[str]]:
        return {
            "keyword": self.keyword,
            "country_name": self.country_name,
            "country_slug": self.country_slug,
            "region_name": self.region_name,
            "region_slug": self.region_slug,
            "detail_name": self.detail_name,
            "leaf_name": self.leaf_name,
            "leaf_slug": self.leaf_slug,
            "category_name": self.category_name,
            "title_tag": self.title_tag,
            "canonical_path": self.canonical_path,
            "schema_type": self.schema_type,
            "keywords": list(self.keywords),
        }


def build_keyword_target(keyword: str, country_name: str = "일본", country_slug: str = "japan") -> SeoKeywordTarget:
    normalized = to_plain_text(keyword)
    if not normalized:
        raise ValueError("keyword must not be empty")

    tokens = [token for token in re.split(r"\s+", normalized) if token]
    region_name = tokens[0]
    category_name = _detect_category(tokens)
    detail_tokens = tokens[1:]
    if category_name and detail_tokens:
        trailing = detail_tokens[-1]
        if _normalize_category(trailing) == category_name:
            detail_tokens = detail_tokens[:-1]
    detail_name = " ".join(detail_tokens).strip()
    region_slug = _slug_for_region(region_name)
    leaf_name = " ".join(part for part in (detail_name, category_name) if part).strip() or normalized
    leaf_slug = _slug_for_leaf(detail_name, category_name or infer_content_category([normalized]))
    title_tag = build_title_tag(region=region_name, place_name=leaf_name)
    canonical_path = build_canonical_path(country_slug, region_slug or region_name, leaf_slug or leaf_name)
    schema_type = infer_schema_type([category_name, normalized])
    keywords = build_keyword_list(
        primary_keyword=normalized,
        secondary_topics=[detail_name, category_name],
        region=region_name,
        place_name=detail_name,
        content_category=category_name,
    )

    return SeoKeywordTarget(
        keyword=normalized,
        country_name=country_name,
        country_slug=country_slug,
        region_name=region_name,
        region_slug=region_slug or slugify_path_segment(region_name),
        detail_name=detail_name,
        leaf_name=leaf_name,
        leaf_slug=leaf_slug,
        category_name=category_name,
        title_tag=title_tag,
        canonical_path=canonical_path,
        schema_type=schema_type,
        keywords=keywords,
    )


def expand_core_keyword_targets(
    regions: dict[str, Sequence[str]] | None = None,
    categories: Sequence[str] | None = None,
    limit: int | None = None,
) -> list[SeoKeywordTarget]:
    targets: list[SeoKeywordTarget] = []
    matrix = regions or CORE_REGION_DETAILS
    category_values = tuple(categories or CORE_CATEGORIES)
    for region_name, details in matrix.items():
        if not details:
            for category_name in category_values:
                targets.append(build_keyword_target(f"{region_name} {category_name}"))
                if limit is not None and len(targets) >= limit:
                    return targets
            continue
        for detail_name in details:
            for category_name in category_values:
                targets.append(build_keyword_target(f"{region_name} {detail_name} {category_name}"))
                if limit is not None and len(targets) >= limit:
                    return targets
    return targets


def build_keyword_targets(values: Iterable[str]) -> list[SeoKeywordTarget]:
    targets: list[SeoKeywordTarget] = []
    seen: set[str] = set()
    for value in values:
        normalized = to_plain_text(value)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        targets.append(build_keyword_target(normalized))
    return targets


def _detect_category(tokens: Sequence[str]) -> str:
    for token in reversed(tokens):
        normalized = _normalize_category(token)
        if normalized:
            return normalized
    return ""


def _normalize_category(value: str) -> str:
    normalized = CATEGORY_LABELS.get(to_plain_text(value).lower())
    if normalized:
        return normalized
    cleaned = to_plain_text(value)
    return cleaned if cleaned in CATEGORY_SLUGS else ""


def _slug_for_region(region_name: str) -> str:
    normalized = to_plain_text(region_name).lower()
    return REGION_SLUGS.get(normalized, slugify_path_segment(region_name))


def _slug_for_leaf(detail_name: str, category_name: str) -> str:
    parts: list[str] = []
    for raw in (detail_name,):
        value = to_plain_text(raw)
        if not value:
            continue
        lookup = DETAIL_SLUGS.get(value.lower()) or DETAIL_SLUGS.get(value) or slugify_path_segment(value)
        if lookup:
            parts.append(lookup)
    category_slug = CATEGORY_SLUGS.get(category_name, slugify_path_segment(category_name))
    if category_slug:
        parts.append(category_slug)
    return "-".join(part for part in parts if part) or slugify_path_segment(detail_name or category_name or "travel-guide")
