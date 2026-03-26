"""Render SEO page payloads into standalone HTML documents."""

from __future__ import annotations

import html
from typing import Any, Mapping

from ..generation.formatter import build_post_meta_description, format_wordpress_html_payload
from ..generation.seo import to_plain_text
from .structured_data import build_structured_data_json


def render_full_html_document(payload: Mapping[str, Any], canonical_url: str = "") -> str:
    seo = payload.get("seo") if isinstance(payload.get("seo"), Mapping) else {}
    title_tag = to_plain_text(seo.get("title_tag")) or to_plain_text(payload.get("title")) or "일본 여행 가이드"
    description = build_post_meta_description(payload)
    keywords = ", ".join([to_plain_text(item) for item in seo.get("keywords", []) if to_plain_text(item)]) if seo else ""
    canonical = to_plain_text(canonical_url)
    body_html = format_wordpress_html_payload(payload, include_map_iframe=True)
    schema_json = build_structured_data_json(payload, page_url=canonical)

    head_parts = [
        '<meta charset="utf-8" />',
        '<meta name="viewport" content="width=device-width, initial-scale=1" />',
        '<meta name="naver-site-verification" content="e2d3117f0f247b020d2d1b63dd52eb966ec19c92" />',
        f"<title>{html.escape(title_tag)}</title>",
        f'<meta name="description" content="{html.escape(description, quote=True)}" />',
    ]
    if keywords:
        head_parts.append(f'<meta name="keywords" content="{html.escape(keywords, quote=True)}" />')
    if canonical:
        head_parts.append(f'<link rel="canonical" href="{html.escape(canonical, quote=True)}" />')
    head_parts.append(f'<script type="application/ld+json">{schema_json}</script>')

    return (
        "<!DOCTYPE html>\n"
        '<html lang="ko">\n'
        "<head>\n"
        + "\n".join(head_parts)
        + "\n</head>\n"
        "<body>\n"
        + body_html
        + "\n</body>\n"
        "</html>\n"
    )
