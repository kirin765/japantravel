"""SEO and text-normalization helpers for generated travel content."""

from __future__ import annotations

import html
import re
from typing import Any, Sequence

HTML_TAG_RE = re.compile(r"<[^>]+>")
IMAGE_MD_RE = re.compile(r"!\[[^\]]*\]\([^)]+\)")
LINK_MD_RE = re.compile(r"\[([^\]]+)\]\([^)]+\)")
WHITESPACE_RE = re.compile(r"\s+")

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
