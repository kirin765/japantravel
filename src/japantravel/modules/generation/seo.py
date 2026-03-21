"""SEO and text-normalization helpers for generated travel content."""

from __future__ import annotations

import html
import unicodedata
import re
from typing import Any, Sequence

HTML_TAG_RE = re.compile(r"<[^>]+>")
IMAGE_MD_RE = re.compile(r"!\[[^\]]*\]\([^)]+\)")
LINK_MD_RE = re.compile(r"\[([^\]]+)\]\([^)]+\)")
WHITESPACE_RE = re.compile(r"\s+")

SCHEMA_TYPE_MAP = {
    "restaurant": "Restaurant",
    "맛집": "Restaurant",
    "food": "Restaurant",
    "cafe": "Restaurant",
    "카페": "Restaurant",
    "tourist_attraction": "TouristAttraction",
    "attraction": "TouristAttraction",
    "attractions": "TouristAttraction",
    "관광지": "TouristAttraction",
    "museum": "TouristAttraction",
    "park": "TouristAttraction",
    "숙소": "Place",
    "hotel": "Place",
    "hotels": "Place",
    "ryokan": "Place",
    "료칸": "Place",
}

CONTENT_CATEGORY_MAP = {
    "restaurant": "맛집",
    "맛집": "맛집",
    "food": "맛집",
    "카페": "카페",
    "cafe": "카페",
    "coffee": "카페",
    "tourist_attraction": "관광지",
    "attraction": "관광지",
    "attractions": "관광지",
    "관광지": "관광지",
    "museum": "관광지",
    "park": "관광지",
    "hotel": "숙소",
    "hotels": "숙소",
    "숙소": "숙소",
    "료칸": "숙소",
}

SLUG_TRANSLATIONS = {
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
    "신주쿠": "shinjuku",
    "난바": "namba",
    "기온": "gion",
    "카페": "cafe",
    "맛집": "restaurants",
    "관광지": "attractions",
    "숙소": "hotels",
}

SCENARIO_KEYWORDS = {
    "solo_travel": "혼자 여행",
    "rainy_day": "비 오는 날 여행",
    "parents_trip": "부모님 여행",
}


def normalize_generated_text(text: Any, drop_heading_lines: bool = True) -> str:
    normalized = html.unescape(str(text or ""))
    normalized = re.sub(r"<br\s*/?>", "\n", normalized, flags=re.I)
    normalized = HTML_TAG_RE.sub(" ", normalized)
    normalized = IMAGE_MD_RE.sub("", normalized)
    normalized = LINK_MD_RE.sub(r"\1", normalized)
    normalized = normalized.replace("**", "").replace("__", "").replace("`", "")
    normalized = normalized.replace("\r\n", "\n").replace("\r", "\n")

    lines: list[str] = []
    for raw_line in normalized.splitlines():
        line = raw_line.strip()
        if not line:
            if lines and lines[-1] != "":
                lines.append("")
            continue
        if drop_heading_lines and raw_line.lstrip().startswith("#"):
            continue
        line = re.sub(r"^\s*#{1,6}\s*", "", line)
        line = re.sub(r"^\s*(제목|타이틀|title)\s*[:：\-]\s*", "", line, flags=re.I)
        line = WHITESPACE_RE.sub(" ", line).strip()
        if line:
            lines.append(line)

    result = "\n".join(lines).strip()
    return re.sub(r"\n{3,}", "\n\n", result)


def to_plain_text(text: Any, max_length: int | None = None) -> str:
    normalized = normalize_generated_text(text, drop_heading_lines=False)
    normalized = WHITESPACE_RE.sub(" ", normalized).strip()
    if max_length is None or len(normalized) <= max_length:
        return normalized

    trimmed = normalized[: max_length - 1].rstrip(" ,.;:)]")
    if " " in trimmed:
        trimmed = trimmed.rsplit(" ", 1)[0]
    return f"{trimmed}…"


def build_primary_keyword(title: str, region: str = "", scenario: str = "", max_length: int = 38) -> str:
    title_text = to_plain_text(title)
    region_text = to_plain_text(region)
    scenario_text = SCENARIO_KEYWORDS.get(str(scenario or "").strip(), "여행")

    if region_text:
        keyword = f"{region_text} {scenario_text}".strip()
        if "추천" not in keyword:
            if "여행" in keyword:
                keyword = f"{keyword} 추천"
            else:
                keyword = f"{keyword} 여행 추천"
    else:
        keyword = title_text or "일본 여행 추천"

    if len(keyword) <= max_length:
        return keyword

    shortened = keyword[:max_length].rstrip()
    if " " in shortened:
        shortened = shortened.rsplit(" ", 1)[0]
    if len(shortened) >= 6:
        return shortened
    return to_plain_text(title_text, max_length=max_length) or "일본 여행 추천"


def build_secondary_topics(place_names: Sequence[str], max_items: int = 4) -> list[str]:
    topics: list[str] = []
    for place_name in place_names:
        cleaned = to_plain_text(place_name)
        if cleaned and cleaned not in topics:
            topics.append(cleaned)
        if len(topics) >= max_items:
            break
    return topics


def build_meta_description(
    title: str,
    summary: str = "",
    intro: str = "",
    region: str = "",
    max_length: int = 155,
) -> str:
    candidates = [
        to_plain_text(summary),
        to_plain_text(intro),
        to_plain_text(title),
    ]
    for candidate in candidates:
        if candidate:
            return to_plain_text(candidate, max_length=max_length)

    fallback_region = to_plain_text(region)
    fallback = f"{fallback_region} 여행 정보를 정리한 가이드입니다." if fallback_region else "일본 여행 정보를 정리한 가이드입니다."
    return to_plain_text(fallback, max_length=max_length)


def build_image_alt_text(primary_keyword: str, place_name: str = "", region: str = "", max_length: int = 80) -> str:
    keyword = to_plain_text(primary_keyword)
    place = to_plain_text(place_name)
    region_text = to_plain_text(region)

    if place and place in keyword:
        return to_plain_text(keyword, max_length=max_length)

    parts: list[str] = []
    if keyword:
        parts.append(keyword)
    if place:
        parts.append(place)
    elif region_text:
        parts.append(f"{region_text} 여행 사진")

    if not parts:
        parts = ["일본 여행 이미지"]
    return to_plain_text(" ".join(parts), max_length=max_length)


def build_featured_media_alt_text(primary_keyword: str, place_names: Sequence[str], region: str = "") -> str:
    for place_name in place_names:
        cleaned = to_plain_text(place_name)
        if cleaned:
            return build_image_alt_text(primary_keyword, cleaned, region=region)
    return build_image_alt_text(primary_keyword, region=region)


def build_title_tag(region: str, place_name: str = "", suffix: str = "일본 여행 가이드") -> str:
    region_text = to_plain_text(region)
    place_text = to_plain_text(place_name)
    parts = [part for part in (region_text, place_text) if part]
    core = " ".join(parts).strip() or "일본 여행"
    return f"{core} - {suffix}".strip()


def build_keyword_list(
    primary_keyword: str,
    secondary_topics: Sequence[str],
    region: str = "",
    place_name: str = "",
    content_category: str = "",
    max_items: int = 10,
) -> list[str]:
    values: list[str] = []
    for candidate in (
        primary_keyword,
        f"{region} {content_category}".strip(),
        f"{region} {place_name}".strip(),
        f"{place_name} {content_category}".strip(),
        region,
        place_name,
        content_category,
        *secondary_topics,
    ):
        cleaned = to_plain_text(candidate)
        if not cleaned or cleaned in values:
            continue
        values.append(cleaned)
        if len(values) >= max_items:
            break
    return values


def infer_content_category(values: Sequence[Any]) -> str:
    for value in values:
        normalized = to_plain_text(value).lower()
        if not normalized:
            continue
        mapped = CONTENT_CATEGORY_MAP.get(normalized)
        if mapped:
            return mapped
    return "여행지"


def infer_schema_type(values: Sequence[Any]) -> str:
    for value in values:
        normalized = to_plain_text(value).lower()
        if not normalized:
            continue
        mapped = SCHEMA_TYPE_MAP.get(normalized)
        if mapped:
            return mapped
    return "Place"


def slugify_path_segment(text: str, max_length: int = 60) -> str:
    cleaned = to_plain_text(text).lower()
    if not cleaned:
        return ""
    translated = SLUG_TRANSLATIONS.get(cleaned, cleaned)
    if translated != cleaned:
        return translated[:max_length].strip("-")

    normalized = unicodedata.normalize("NFKD", cleaned)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    if not ascii_text:
        ascii_text = cleaned.replace(" ", "-")
    ascii_text = re.sub(r"[^a-z0-9가-힣\- ]", " ", ascii_text)
    ascii_text = ascii_text.replace(" ", "-")
    ascii_text = re.sub(r"-{2,}", "-", ascii_text).strip("-")
    return ascii_text[:max_length].strip("-")


def build_canonical_path(country: str, region: str, leaf: str) -> str:
    parts = [
        slugify_path_segment(country),
        slugify_path_segment(region),
        slugify_path_segment(leaf),
    ]
    normalized = [part for part in parts if part]
    return "/" + "/".join(normalized)
