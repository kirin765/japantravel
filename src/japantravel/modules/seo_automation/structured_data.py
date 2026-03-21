"""Structured data builders for SEO pages and posts."""

from __future__ import annotations

import json
from typing import Any, Mapping

from ..generation.seo import build_meta_description, infer_schema_type, to_plain_text


def build_structured_data(payload: Mapping[str, Any], page_url: str = "") -> dict[str, Any]:
    title = to_plain_text(payload.get("title"))
    region = to_plain_text(payload.get("region"))
    seo = payload.get("seo") if isinstance(payload.get("seo"), Mapping) else {}
    description = to_plain_text(seo.get("meta_description")) or build_meta_description(
        title=title,
        summary=str(payload.get("summary", "")),
        intro=str(payload.get("intro", "")),
        region=region,
    )
    schema_type = to_plain_text(seo.get("schema_type")) or infer_schema_type(_collect_category_values(payload))
    image = _first_image(payload)
    address = _first_address(payload)

    graph: list[dict[str, Any]] = []
    if page_url:
        graph.append(
            {
                "@type": "WebPage",
                "@id": page_url,
                "url": page_url,
                "name": title,
                "description": description,
            }
        )

    place_node: dict[str, Any] = {
        "@type": schema_type or "Place",
        "name": title or to_plain_text(seo.get("primary_keyword")) or "일본 여행 가이드",
        "description": description,
    }
    if page_url:
        place_node["url"] = page_url
    if address:
        place_node["address"] = address
    if image:
        place_node["image"] = image
    graph.append(place_node)

    faq_items = _faq_items(payload)
    if faq_items:
        graph.append(
            {
                "@type": "FAQPage",
                "mainEntity": faq_items,
            }
        )

    breadcrumb = _breadcrumb_node(payload, page_url)
    if breadcrumb:
        graph.append(breadcrumb)

    return {"@context": "https://schema.org", "@graph": graph}


def build_structured_data_json(payload: Mapping[str, Any], page_url: str = "") -> str:
    return json.dumps(build_structured_data(payload, page_url=page_url), ensure_ascii=False, separators=(",", ":"))


def _collect_category_values(payload: Mapping[str, Any]) -> list[str]:
    values: list[str] = []
    seo = payload.get("seo")
    if isinstance(seo, Mapping):
        for key in ("content_category", "schema_type", "primary_keyword"):
            value = to_plain_text(seo.get(key))
            if value:
                values.append(value)
    for section in payload.get("place_sections", []):
        if isinstance(section, Mapping):
            for key in ("category", "place_type", "title"):
                value = to_plain_text(section.get(key))
                if value:
                    values.append(value)
    return values


def _first_image(payload: Mapping[str, Any]) -> str:
    for section in payload.get("place_sections", []):
        if not isinstance(section, Mapping):
            continue
        for url in section.get("image_urls", []) or []:
            value = to_plain_text(url)
            if value:
                return value
    return ""


def _first_address(payload: Mapping[str, Any]) -> str:
    snapshots = payload.get("place_snapshots", [])
    if isinstance(snapshots, list):
        for item in snapshots:
            if isinstance(item, Mapping):
                address = to_plain_text(item.get("address"))
                if address:
                    return address
    for section in payload.get("place_sections", []):
        if isinstance(section, Mapping):
            address = to_plain_text(section.get("address"))
            if address:
                return address
    return to_plain_text(payload.get("region"))


def _faq_items(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for raw in payload.get("faq", []) or []:
        if not isinstance(raw, Mapping):
            continue
        question = to_plain_text(raw.get("question") or raw.get("q"))
        answer = to_plain_text(raw.get("answer") or raw.get("a"))
        if not question or not answer:
            continue
        items.append(
            {
                "@type": "Question",
                "name": question,
                "acceptedAnswer": {
                    "@type": "Answer",
                    "text": answer,
                },
            }
        )
    return items


def _breadcrumb_node(payload: Mapping[str, Any], page_url: str) -> dict[str, Any] | None:
    seo = payload.get("seo")
    if not isinstance(seo, Mapping):
        return None
    canonical_path = to_plain_text(seo.get("canonical_path"))
    if not canonical_path:
        return None
    parts = [part for part in canonical_path.strip("/").split("/") if part]
    if not parts:
        return None

    item_list = []
    current = ""
    for idx, part in enumerate(parts, start=1):
        current += f"/{part}"
        item_url = page_url if idx == len(parts) and page_url else current
        item_list.append(
            {
                "@type": "ListItem",
                "position": idx,
                "name": part.replace("-", " ").title(),
                "item": item_url,
            }
        )
    return {"@type": "BreadcrumbList", "itemListElement": item_list}
