"""Formatting helpers for generation outputs."""

from __future__ import annotations

import html
import re
from typing import Any, Dict, List, Mapping, Sequence

from bs4 import BeautifulSoup

from .pipeline import GeneratedArticle
from .seo import (
    build_featured_media_alt_text,
    build_image_alt_text,
    build_meta_description,
    build_primary_keyword,
    normalize_generated_text,
    to_plain_text,
)

ARTICLE_STYLE = (
    "max-width:820px;margin:0 auto;color:#1f2937;font-size:17px;line-height:1.85;"
    "font-family:'Segoe UI',-apple-system,BlinkMacSystemFont,sans-serif;"
)
SECTION_STYLE = "margin:38px 0 0;"
SECTION_TITLE_STYLE = (
    "margin:0 0 16px;color:#0f766e;font-size:1.45rem;line-height:1.4;"
    "padding-bottom:8px;border-bottom:2px solid #d1fae5;"
)
LEAD_BOX_STYLE = (
    "margin:18px 0 28px;padding:22px 24px;border-radius:18px;border:1px solid #dbeafe;"
    "background:linear-gradient(135deg,#f8fbff 0%,#eefbf6 100%);"
    "box-shadow:0 10px 30px rgba(15,23,42,0.05);"
)
CARD_STYLE = (
    "margin:18px 0 26px;padding:22px 24px;border-radius:18px;border:1px solid #e5e7eb;"
    "background:#ffffff;box-shadow:0 12px 32px rgba(15,23,42,0.06);"
)
PLACE_CARD_STYLE = CARD_STYLE + "white-space:normal;"
SUBTITLE_STYLE = "margin:0 0 14px;color:#0f172a;font-size:1.2rem;line-height:1.5;"
BODY_P_STYLE = "margin:0 0 14px;color:#334155;font-size:1rem;line-height:1.9;"
EMPHASIS_LABEL_STYLE = "color:#0f766e;font-weight:700;"
CHECKLIST_STYLE = "margin:0;padding:0 0 0 20px;color:#334155;"
CHECKLIST_ITEM_STYLE = "margin:0 0 12px;line-height:1.85;"
FAQ_ITEM_STYLE = (
    "margin:0 0 14px;padding:18px 20px;border-radius:16px;background:#fafaf9;"
    "border:1px solid #e7e5e4;"
)
NOTICE_STYLE = (
    "margin:28px 0 0;padding:16px 18px;border-radius:14px;background:#fff7ed;"
    "border:1px solid #fed7aa;color:#9a3412;font-size:0.95rem;"
)
MAP_LINK_STYLE = (
    "display:inline-block;margin-top:12px;padding:10px 14px;border-radius:999px;"
    "background:#0f766e;color:#ffffff;text-decoration:none;font-weight:700;font-size:0.95rem;"
)
IMAGE_STYLE = (
    "display:block;width:100%;height:auto;border-radius:16px;border:1px solid #e5e7eb;"
    "box-shadow:0 10px 24px rgba(15,23,42,0.08);"
)
IMAGE_FIGURE_STYLE = "margin:18px 0;"
ROUTE_BOX_STYLE = (
    "margin:18px 0 26px;padding:22px 24px;border-radius:18px;border:1px solid #fde68a;"
    "background:linear-gradient(180deg,#fffdf4 0%,#fffbeb 100%);"
)
FAQ_WRAPPER_STYLE = "margin-top:8px;"
RELATED_LINK_BOX_STYLE = (
    "margin:18px 0 0;padding:18px 20px;border-radius:16px;border:1px solid #cbd5e1;"
    "background:#f8fafc;color:#0f172a;"
)
RELATED_LIST_STYLE = "margin:0;padding:0 0 0 18px;color:#334155;"
RELATED_LINK_STYLE = "color:#0f766e;text-decoration:none;font-weight:700;"


def format_markdown(article: GeneratedArticle) -> str:
    return format_markdown_payload(article_to_payload(article), include_map_iframe=True)


def format_markdown_payload(payload: Mapping[str, Any], include_map_iframe: bool = True) -> str:
    lines: List[str] = []
    lines.append(f"# {payload.get('title', '여행 추천 글')}")
    lines.append("")
    lines.append("## 한줄 요약")
    lines.append(str(payload.get("summary", "")))
    lines.append("")
    lines.append("## 인트로")
    lines.append(str(payload.get("intro", "")))
    lines.append("")
    lines.append("## 장소 추천")
    for section in payload.get("place_sections", []):
        if not isinstance(section, Mapping):
            continue
        title = str(section.get("title") or section.get("place_name") or "")
        body = str(section.get("body", ""))
        image_urls = _coerce_strings(section.get("image_urls", []))
        maps_url = str(section.get("maps_url", ""))
        map_embed_url = str(section.get("map_embed_url", ""))
        lines.append(f"### {title}")
        lines.append(body)
        lines.extend(_render_section_images(title, image_urls, max_images=2))
        lines.extend(_render_section_map(maps_url, map_embed_url, include_iframe=include_map_iframe))
        lines.append("")

    lines.append("## 동선 제안")
    lines.append(str(payload.get("route_suggestion", "")))
    lines.append("")
    lines.append("## 체크리스트")
    for item in payload.get("checklist", []):
        if item:
            lines.append(f"- {item}")
    lines.append("")
    lines.append("## FAQ")
    for item in payload.get("faq", []):
        lines.append(_build_faq_block(item))
    related_posts = _coerce_related_posts(payload.get("related_posts", []))
    if related_posts:
        lines.append("")
        lines.append("## 관련 글")
        for post in related_posts:
            lines.append(f"- [{post['title']}]({post['url']})")
    lines.append("")
    lines.append("## 마무리")
    lines.append(str(payload.get("conclusion", "")))
    lines.append("")
    lines.append("※ 모든 운영시간·혼잡도·가격은 방문 직전 최신 공지를 확인하세요.")
    return "\n".join(lines)


def resolve_primary_keyword(payload: Mapping[str, Any]) -> str:
    seo = payload.get("seo")
    if isinstance(seo, Mapping):
        primary = to_plain_text(seo.get("primary_keyword"))
        if primary:
            return primary
    return build_primary_keyword(
        title=str(payload.get("title", "")),
        region=str(payload.get("region", "")),
        scenario=str(payload.get("scenario", "")),
    )


def build_post_meta_description(payload: Mapping[str, Any], max_length: int = 155) -> str:
    seo = payload.get("seo")
    if isinstance(seo, Mapping):
        explicit = to_plain_text(seo.get("meta_description"), max_length=max_length)
        if explicit:
            return explicit
    return build_meta_description(
        title=str(payload.get("title", "")),
        summary=str(payload.get("summary", "")),
        intro=str(payload.get("intro", "")),
        region=str(payload.get("region", "")),
        max_length=max_length,
    )


def build_post_featured_media_alt_text(payload: Mapping[str, Any]) -> str:
    return build_featured_media_alt_text(
        resolve_primary_keyword(payload),
        _extract_place_names(payload),
        region=str(payload.get("region", "")),
    )


def format_wordpress_html_payload(payload: Mapping[str, Any], include_map_iframe: bool = True) -> str:
    primary_keyword = resolve_primary_keyword(payload)
    summary = normalize_generated_text(payload.get("summary", ""), drop_heading_lines=True)
    intro = normalize_generated_text(payload.get("intro", ""), drop_heading_lines=True)
    route_suggestion = normalize_generated_text(payload.get("route_suggestion", ""), drop_heading_lines=True)
    conclusion = normalize_generated_text(payload.get("conclusion", ""), drop_heading_lines=True)
    related_posts = _coerce_related_posts(payload.get("related_posts", []))

    parts: List[str] = [f'<div class="jt-article" style="{ARTICLE_STYLE}">']

    if summary:
        parts.append(_render_summary_section(f"{primary_keyword} 한눈에 보기", summary))

    if intro:
        parts.append(_render_section(f"{primary_keyword} 여행 개요", intro))

    if related_posts:
        parts.append(_render_inline_links(related_posts, primary_keyword))

    place_sections = payload.get("place_sections", [])
    if isinstance(place_sections, list) and place_sections:
        parts.append(f'<section style="{SECTION_STYLE}">')
        parts.append(f'<h2 style="{SECTION_TITLE_STYLE}">{_format_inline(f"{primary_keyword} 추천 장소")}</h2>')
        for section in place_sections:
            if isinstance(section, Mapping):
                rendered = _render_place_section(
                    section,
                    primary_keyword=primary_keyword,
                    region=str(payload.get("region", "")),
                    include_map_iframe=include_map_iframe,
                )
                if rendered:
                    parts.append(rendered)
        parts.append("</section>")

    if route_suggestion:
        parts.append(_render_route_section(f"{primary_keyword} 동선 제안", route_suggestion))

    checklist = payload.get("checklist", [])
    if isinstance(checklist, list) and checklist:
        parts.append(f'<section style="{SECTION_STYLE}">')
        parts.append(f'<h2 style="{SECTION_TITLE_STYLE}">{_format_inline(f"{primary_keyword} 체크리스트")}</h2>')
        parts.append(f'<div style="{CARD_STYLE}">')
        parts.append(f'<ul style="{CHECKLIST_STYLE}">')
        for item in checklist:
            cleaned = normalize_generated_text(item, drop_heading_lines=True)
            if cleaned:
                parts.append(f'<li style="{CHECKLIST_ITEM_STYLE}">{_format_inline(cleaned)}</li>')
        parts.append("</ul>")
        parts.append("</div>")
        parts.append("</section>")

    faq_items = payload.get("faq", [])
    if isinstance(faq_items, list) and faq_items:
        parts.append(f'<section style="{SECTION_STYLE}">')
        parts.append(f'<h2 style="{SECTION_TITLE_STYLE}">{_format_inline(f"{primary_keyword} FAQ")}</h2>')
        parts.append(f'<div style="{FAQ_WRAPPER_STYLE}">')
        for item in faq_items:
            block = _build_faq_html(item)
            if block:
                parts.append(block)
        parts.append("</div>")
        parts.append("</section>")

    if related_posts:
        parts.append(_render_related_posts_section(related_posts, primary_keyword))

    if conclusion:
        parts.append(_render_section(f"{primary_keyword} 마무리", conclusion))

    parts.append(f'<div style="{NOTICE_STYLE}">※ 모든 운영시간, 혼잡도, 가격 정보는 방문 직전에 다시 확인하세요.</div>')
    parts.append("</div>")
    return "\n".join(part for part in parts if part)


def restyle_existing_wordpress_html(
    content: str,
    primary_keyword: str = "",
    related_posts: Sequence[Mapping[str, Any]] | None = None,
) -> str:
    updated = content or ""
    if not updated:
        return updated

    updated = re.sub(
        r"!\[[^\]]*\]\((https?://[^)\s]+)\)",
        lambda match: _render_legacy_image(match.group(1), primary_keyword=primary_keyword),
        updated,
    )
    updated = re.sub(
        r"지도 보기:\s*(https?://[^\s<]+)",
        lambda match: _render_map_link(html.unescape(match.group(1))),
        updated,
    )

    soup = BeautifulSoup(updated, "html.parser")
    root = _ensure_article_wrapper(soup)
    primary = to_plain_text(primary_keyword) or _infer_primary_keyword_from_html(root)
    related = _coerce_related_posts(related_posts or [])

    for selector in (".jt-inline-links", ".jt-related-posts"):
        for node in root.select(selector):
            node.decompose()

    for tag in root.find_all("h1"):
        tag.decompose()

    for tag in root.find_all("h2"):
        tag["style"] = SECTION_TITLE_STYLE
    for tag in root.find_all("h3"):
        tag["style"] = SUBTITLE_STYLE
    for tag in root.find_all("p"):
        existing = tag.get("style", "")
        tag["style"] = existing or BODY_P_STYLE
    for tag_name in ("ul", "ol"):
        for tag in root.find_all(tag_name):
            existing = tag.get("style", "")
            tag["style"] = existing or CHECKLIST_STYLE
    for tag in root.find_all("li"):
        existing = tag.get("style", "")
        tag["style"] = existing or CHECKLIST_ITEM_STYLE
    for tag in root.find_all("iframe"):
        existing = tag.get("style", "")
        tag["style"] = existing or "border:0;display:block;width:100%;min-height:280px;"
        tag["loading"] = tag.get("loading", "lazy")
        tag["referrerpolicy"] = tag.get("referrerpolicy", "no-referrer-when-downgrade")

    for article in root.find_all("article"):
        article["style"] = article.get("style", "") or PLACE_CARD_STYLE
        place_heading = article.find("h3")
        place_name = _clean_place_name(place_heading.get_text(" ", strip=True) if place_heading else "")
        for image in article.find_all("img"):
            image["style"] = image.get("style", "") or IMAGE_STYLE
            image["loading"] = image.get("loading", "lazy")
            image["referrerpolicy"] = image.get("referrerpolicy", "no-referrer-when-downgrade")
            if _should_replace_alt_text(image.get("alt", "")):
                image["alt"] = build_image_alt_text(primary, place_name)

    all_images = root.find_all("img")
    for image in all_images:
        image["style"] = image.get("style", "") or IMAGE_STYLE
        image["loading"] = image.get("loading", "lazy")
        image["referrerpolicy"] = image.get("referrerpolicy", "no-referrer-when-downgrade")
        if _should_replace_alt_text(image.get("alt", "")):
            image["alt"] = build_featured_media_alt_text(primary, _extract_place_names_from_html(root))

    if related:
        intro_target = _find_inline_link_anchor(root)
        if intro_target is not None:
            intro_target.insert_after(BeautifulSoup(_render_inline_links(related, primary), "html.parser"))
        else:
            root.insert(0, BeautifulSoup(_render_inline_links(related, primary), "html.parser"))
        root.append(BeautifulSoup(_render_related_posts_section(related, primary), "html.parser"))

    if not root.find(string=lambda text: isinstance(text, str) and "운영시간" in text):
        root.append(BeautifulSoup(f'<div style="{NOTICE_STYLE}">※ 모든 운영시간, 혼잡도, 가격 정보는 방문 직전에 다시 확인하세요.</div>', "html.parser"))

    return str(root)


def to_wordpress_blocks(article: GeneratedArticle) -> str:
    return format_wordpress_html_payload(article_to_payload(article), include_map_iframe=True)


def article_to_payload(article: GeneratedArticle) -> Dict[str, Any]:
    return article.to_payload()


def _render_section_images(image_label: str, images: List[str], max_images: int = 2) -> List[str]:
    lines: List[str] = []
    for url in images[:max_images]:
        if url:
            lines.append(f"![{image_label}]({url})")
    if lines:
        lines.append("")
    return lines


def _render_section_map(maps_url: str, map_embed_url: str, include_iframe: bool = True) -> List[str]:
    lines: List[str] = []
    if include_iframe and map_embed_url:
        lines.append("<div class='apify-map-embed'>")
        lines.append(
            f'<iframe src="{map_embed_url}" width="100%" height="280" style="border:0;" loading="lazy" referrerpolicy="no-referrer-when-downgrade" allowfullscreen></iframe>'
        )
        lines.append("</div>")
    if maps_url:
        lines.append(f"지도 보기: {maps_url}")
    elif map_embed_url:
        lines.append(f"지도 보기: {map_embed_url}")
    return lines


def _render_summary_section(title: str, text: str) -> str:
    return (
        f'<section style="{SECTION_STYLE}">'
        f'<h2 style="{SECTION_TITLE_STYLE}">{_format_inline(title)}</h2>'
        f'<div style="{LEAD_BOX_STYLE}">{_render_paragraphs(text, compact=False, in_box=True)}</div>'
        "</section>"
    )


def _render_section(title: str, text: str, box_style: str = CARD_STYLE) -> str:
    cleaned = normalize_generated_text(text, drop_heading_lines=True)
    if not cleaned:
        return ""
    return (
        f'<section style="{SECTION_STYLE}">'
        f'<h2 style="{SECTION_TITLE_STYLE}">{_format_inline(title)}</h2>'
        f'<div style="{box_style}">{_render_paragraphs(cleaned, compact=False, in_box=True)}</div>'
        "</section>"
    )


def _render_route_section(title: str, text: str) -> str:
    sections = _parse_route_sections(text)
    if not sections:
        return _render_section(title, text, box_style=ROUTE_BOX_STYLE)

    parts: List[str] = [
        f'<section style="{SECTION_STYLE}">',
        f'<h2 style="{SECTION_TITLE_STYLE}">{_format_inline(title)}</h2>',
        f'<div style="{ROUTE_BOX_STYLE}">',
    ]
    for section_title, body in sections:
        parts.append(f'<h3 style="{SUBTITLE_STYLE}">{_format_inline(section_title)}</h3>')
        parts.append(_render_paragraphs(body, compact=False, in_box=True))
    parts.append("</div>")
    parts.append("</section>")
    return "\n".join(parts)


def _render_place_section(
    section: Mapping[str, Any],
    primary_keyword: str,
    region: str = "",
    include_map_iframe: bool = True,
) -> str:
    place_name = to_plain_text(section.get("place_name") or section.get("title") or "")
    title = to_plain_text(section.get("title") or place_name)
    body = normalize_generated_text(section.get("body", ""), drop_heading_lines=True)
    image_urls = [url for url in _coerce_strings(section.get("image_urls", [])) if _is_probable_image_url(url)]
    maps_url = to_plain_text(section.get("maps_url", ""))
    map_embed_url = to_plain_text(section.get("map_embed_url", ""))
    image_alt = build_image_alt_text(primary_keyword, place_name, region=region)

    parts: List[str] = [f'<article style="{PLACE_CARD_STYLE}">']
    if title:
        parts.append(f'<h3 style="{SUBTITLE_STYLE}">{_format_inline(title)}</h3>')
    if body:
        parts.append(_render_paragraphs(body))
    if image_urls:
        parts.append(_render_image_gallery(image_alt, image_urls[:2]))
    if include_map_iframe and map_embed_url:
        parts.append(
            "<div style=\"margin-top:18px;border-radius:16px;overflow:hidden;border:1px solid #dbeafe;"
            "box-shadow:0 8px 24px rgba(15,23,42,0.06);\">"
            f'<iframe src="{html.escape(map_embed_url, quote=True)}" width="100%" height="280" '
            'style="border:0;display:block;" loading="lazy" referrerpolicy="no-referrer-when-downgrade" allowfullscreen></iframe>'
            "</div>"
        )
    if maps_url:
        parts.append(_render_map_link(maps_url))
    elif map_embed_url:
        parts.append(_render_map_link(map_embed_url))
    parts.append("</article>")
    return "\n".join(parts)


def _render_image_gallery(image_alt: str, images: List[str]) -> str:
    parts: List[str] = ['<div style="margin-top:18px;">']
    for url in images:
        parts.append(
            f'<figure style="{IMAGE_FIGURE_STYLE}">'
            f'<img src="{html.escape(url, quote=True)}" alt="{html.escape(image_alt, quote=True)}" '
            f'style="{IMAGE_STYLE}" loading="lazy" referrerpolicy="no-referrer-when-downgrade" />'
            "</figure>"
        )
    parts.append("</div>")
    return "\n".join(parts)


def _render_inline_links(related_posts: Sequence[Mapping[str, str]], primary_keyword: str) -> str:
    links = [
        f'<a href="{html.escape(post["url"], quote=True)}" style="{RELATED_LINK_STYLE}">{_format_inline(post["title"])}</a>'
        for post in related_posts[:2]
        if post.get("url") and post.get("title")
    ]
    if not links:
        return ""
    links_html = " · ".join(links)
    return (
        f'<section class="jt-inline-links" style="{SECTION_STYLE}">'
        f'<div style="{RELATED_LINK_BOX_STYLE}">'
        f'<p style="margin:0 0 8px;color:#0f172a;font-weight:800;">{_format_inline(primary_keyword)}와 함께 보면 좋은 글</p>'
        f'<p style="{BODY_P_STYLE}">{links_html}</p>'
        "</div>"
        "</section>"
    )


def _render_related_posts_section(related_posts: Sequence[Mapping[str, str]], primary_keyword: str) -> str:
    items = []
    for post in related_posts[:3]:
        url = post.get("url")
        title = post.get("title")
        if not url or not title:
            continue
        items.append(
            f'<li style="{CHECKLIST_ITEM_STYLE}">'
            f'<a href="{html.escape(url, quote=True)}" style="{RELATED_LINK_STYLE}">{_format_inline(title)}</a>'
            "</li>"
        )
    if not items:
        return ""

    return (
        f'<section class="jt-related-posts" style="{SECTION_STYLE}">'
        f'<h2 style="{SECTION_TITLE_STYLE}">{_format_inline(f"{primary_keyword} 관련 글")}</h2>'
        f'<div style="{CARD_STYLE}"><ul style="{RELATED_LIST_STYLE}">{"".join(items)}</ul></div>'
        "</section>"
    )


def _render_map_link(url: str) -> str:
    safe_url = html.escape(url, quote=True)
    return (
        '<p style="margin:14px 0 0;">'
        f'<a href="{safe_url}" target="_blank" rel="noopener noreferrer" style="{MAP_LINK_STYLE}">지도에서 보기</a>'
        "</p>"
    )


def _render_legacy_image(url: str, primary_keyword: str = "") -> str:
    cleaned = html.unescape(url.strip())
    if not _is_probable_image_url(cleaned):
        return cleaned
    alt_text = build_image_alt_text(primary_keyword or "일본 여행", place_name="")
    return (
        f'<figure style="{IMAGE_FIGURE_STYLE}">'
        f'<img src="{html.escape(cleaned, quote=True)}" alt="{html.escape(alt_text, quote=True)}" '
        f'style="{IMAGE_STYLE}" loading="lazy" referrerpolicy="no-referrer-when-downgrade" />'
        "</figure>"
    )


def _render_paragraphs(text: str, compact: bool = False, in_box: bool = False) -> str:
    paragraphs = _split_paragraphs(text)
    if not paragraphs:
        return ""

    style = BODY_P_STYLE if not compact else "margin:0 0 10px;color:#334155;font-size:0.98rem;line-height:1.8;"
    if in_box:
        style = style.replace("color:#334155;", "color:#1f2937;")

    rendered: List[str] = []
    for paragraph in paragraphs:
        bullet = _parse_labeled_bullet(paragraph)
        if bullet:
            label, body = bullet
            rendered.append(
                f'<p style="{style}"><strong style="{EMPHASIS_LABEL_STYLE}">{_format_inline(label)}:</strong> '
                f'{_format_inline(body)}</p>'
            )
            continue
        rendered.append(f'<p style="{style}">{_format_inline(paragraph).replace(chr(10), "<br />")}</p>')
    return "\n".join(rendered)


def _parse_labeled_bullet(text: str) -> tuple[str, str] | None:
    match = re.match(
        r"^\s*[\-\u2013\u2014\u2022]\s*(?:\*\*)?([^:\n*]{1,40}?)(?:\*\*)?\s*:\s*(.+?)\s*$",
        text,
        flags=re.S,
    )
    if not match:
        return None
    label = to_plain_text(match.group(1))
    body = to_plain_text(match.group(2))
    if not label or not body:
        return None
    return label, body


def _split_paragraphs(text: str) -> List[str]:
    normalized = _normalize_breaks(text)
    chunks = [chunk.strip() for chunk in re.split(r"\n\s*\n+", normalized) if chunk.strip()]
    if len(chunks) == 1 and len(chunks[0]) > 260:
        chunks = _split_long_paragraph(chunks[0])
    return chunks


def _split_long_paragraph(text: str, max_sentences: int = 2) -> List[str]:
    sentences = re.split(r"(?<=[.!?。！？])\s+", text.strip())
    grouped: List[str] = []
    bucket: List[str] = []
    for sentence in sentences:
        cleaned = sentence.strip()
        if not cleaned:
            continue
        bucket.append(cleaned)
        if len(bucket) >= max_sentences:
            grouped.append(" ".join(bucket).strip())
            bucket = []
    if bucket:
        grouped.append(" ".join(bucket).strip())
    return grouped or [text.strip()]


def _build_faq_html(item: Any) -> str:
    question = ""
    answer = ""

    if isinstance(item, Mapping):
        question = to_plain_text(item.get("question") or item.get("q") or "")
        answer = to_plain_text(item.get("answer") or item.get("a") or "")
    elif item:
        normalized = to_plain_text(item)
        match = re.match(r"(?is)Q\s*:\s*(.*?)\s*A\s*:\s*(.+)", normalized)
        if match:
            question = to_plain_text(match.group(1))
            answer = to_plain_text(match.group(2))

    if not question or not answer:
        return ""

    return (
        f'<div style="{FAQ_ITEM_STYLE}">'
        f'<p style="margin:0 0 8px;color:#0f172a;font-weight:800;">Q. {_format_inline(question)}</p>'
        f'<p style="margin:0;color:#475569;line-height:1.85;">A. {_format_inline(answer)}</p>'
        "</div>"
    )


def _coerce_strings(values: Any) -> List[str]:
    if not values:
        return []
    if isinstance(values, list):
        return [str(v).strip() for v in values if str(v).strip()]
    if isinstance(values, str):
        return [values]
    return []


def _coerce_related_posts(values: Any) -> list[dict[str, str]]:
    posts: list[dict[str, str]] = []
    if not isinstance(values, list):
        return posts
    for value in values:
        if not isinstance(value, Mapping):
            continue
        title = to_plain_text(value.get("title"))
        url = to_plain_text(value.get("url") or value.get("link"))
        slug = to_plain_text(value.get("slug"))
        if not title or not url:
            continue
        posts.append({"title": title, "url": url, "slug": slug})
    return posts


def _build_faq_block(item: Any) -> str:
    if isinstance(item, Mapping):
        question = re.sub(r"\s+", " ", str(item.get("question") or item.get("q") or "").strip())
        answer = re.sub(r"\s+", " ", str(item.get("answer") or item.get("a") or "").strip())
        if question and answer:
            return _build_single_faq(question, answer)
        item = f"Q: {question} A: {answer}".strip()

    if not item:
        return ""

    normalized = str(item).strip()

    multi = re.findall(
        r"(?is)Q\s*:\s*(.*?)\s*A\s*:\s*(.*?)(?=\s*(?:Q\s*:\s*|$))",
        normalized.replace("<br>", "\n"),
    )
    if multi:
        html_items: List[str] = []
        for q, a in multi:
            q_text = re.sub(r"\s+", " ", (q or "").strip())
            a_text = re.sub(r"\s+", " ", (a or "").strip())
            if q_text and a_text:
                html_items.append(_build_single_faq(q_text, a_text))
        if html_items:
            return "<div class='tj-faq'>\n" + "\n".join(html_items) + "\n</div>"

    qa_pattern = re.compile(
        r"(?is)(?:^|\n)\s*(?:-|\d+[\.\)]\s*)?\s*Q\s*[:.]?\s*(.+?)\s*(?:\n|$)\s*A\s*[:.]?\s*(.+?)(?=(?:\n|^)\s*(?:-|\d+[\.\)]\s*)?\s*Q\s*[:.]|\Z)",
        re.MULTILINE,
    )
    parsed: List[str] = []
    for match in qa_pattern.finditer(normalized):
        question = re.sub(r"\s+", " ", (match.group(1) or "").strip())
        answer = re.sub(r"\s+", " ", (match.group(2) or "").strip())
        if question and answer:
            parsed.append(_build_single_faq(question, answer))

    if parsed:
        return "\n".join(parsed)

    if "Q:" not in normalized and "A:" not in normalized:
        return f"- {normalized}"

    if normalized.startswith("Q:") and "A:" in normalized:
        q, a = normalized.split("A:", 1)
        return _build_single_faq(q.strip(), a.strip())

    return f"- {normalized}"


def _build_single_faq(question: str, answer: str) -> str:
    question_text = re.sub(r"\s+", " ", question.replace("Q:", "").replace("Q.", "").strip())
    answer_text = re.sub(r"\s+", " ", answer.replace("A:", "").strip())
    return (
        "<div class='tj-faq-item'>\n"
        f"<p><strong>Q.</strong> {question_text}</p>\n"
        f"<p><strong>A.</strong> {answer_text}</p>\n"
        "</div>"
    )


def _parse_route_sections(text: str) -> list[tuple[str, str]]:
    normalized = _normalize_breaks(text)
    if not normalized:
        return []

    sections: list[tuple[str, str]] = []
    current_title = ""
    current_body: list[str] = []
    for line in normalized.splitlines():
        stripped = line.strip()
        if not stripped:
            if current_body and current_body[-1] != "":
                current_body.append("")
            continue
        match = re.match(r"^\d+\.\s*(.+)$", stripped)
        if match:
            if current_title:
                sections.append((current_title, "\n".join(part for part in current_body if part is not None).strip()))
            current_title = match.group(1).strip()
            current_body = []
            continue
        current_body.append(stripped)

    if current_title:
        sections.append((current_title, "\n".join(part for part in current_body if part is not None).strip()))
    return [(title, body) for title, body in sections if title and body]


def _should_replace_alt_text(value: str) -> bool:
    cleaned = to_plain_text(value)
    return not cleaned or cleaned.isdigit() or cleaned in {"추천 장소", "추천 장소 이미지"}


def _infer_primary_keyword_from_html(root: Any) -> str:
    heading = root.find(["h2", "h3", "p"])
    if heading:
        return build_primary_keyword(heading.get_text(" ", strip=True))
    return "일본 여행 추천"


def _extract_place_names(payload: Mapping[str, Any]) -> list[str]:
    place_names: list[str] = []
    for section in payload.get("place_sections", []):
        if not isinstance(section, Mapping):
            continue
        place_name = to_plain_text(section.get("place_name") or section.get("title"))
        if place_name and place_name not in place_names:
            place_names.append(place_name)
    return place_names


def _extract_place_names_from_html(root: Any) -> list[str]:
    place_names: list[str] = []
    for heading in root.find_all("h3"):
        cleaned = _clean_place_name(heading.get_text(" ", strip=True))
        if cleaned and cleaned not in place_names:
            place_names.append(cleaned)
    return place_names


def _clean_place_name(value: str) -> str:
    cleaned = to_plain_text(value)
    cleaned = re.sub(r"^[^\w가-힣ぁ-んァ-ン一-龥]+", "", cleaned)
    cleaned = re.sub(r"\s*\([^)]*\)\s*$", "", cleaned)
    return cleaned.strip()


def _find_inline_link_anchor(root: Any) -> Any:
    for section in root.find_all("section"):
        if section.get("class") == ["jt-inline-links"]:
            continue
        return section
    return root.find("p")


def _ensure_article_wrapper(soup: BeautifulSoup) -> Any:
    wrapper = soup.find("div", class_="jt-article")
    if wrapper is not None:
        wrapper["style"] = ARTICLE_STYLE
        return wrapper

    wrapper = soup.new_tag("div")
    wrapper["class"] = ["jt-article"]
    wrapper["style"] = ARTICLE_STYLE
    for child in list(soup.contents):
        wrapper.append(child.extract())
    soup.append(wrapper)
    return wrapper


def _is_probable_image_url(value: str) -> bool:
    url = value.strip()
    if not url.lower().startswith(("http://", "https://")):
        return False

    lower = url.lower()
    if any(token in lower for token in ("google.com/maps", "/maps/search", "/search/?api=1", "/place/", "output=embed")):
        return False
    if re.search(r"\.(jpg|jpeg|png|webp|gif)(?:\?|$)", lower):
        return True
    if any(host in lower for host in ("googleusercontent.com", "ggpht.com", "streetviewpixels-pa.googleapis.com")):
        return True
    if "googleapis.com/v1/thumbnail" in lower:
        return True
    return False


def _normalize_breaks(text: str) -> str:
    normalized = str(text or "")
    normalized = normalized.replace("<br />", "\n").replace("<br/>", "\n").replace("<br>", "\n")
    normalized = re.sub(r"\r\n?", "\n", normalized)
    return normalized.strip()


def _format_inline(text: str) -> str:
    escaped = html.escape(to_plain_text(text))
    escaped = re.sub(r"\*\*(.+?)\*\*", r"<strong>\1</strong>", escaped)
    escaped = re.sub(r"`(.+?)`", r"<code>\1</code>", escaped)
    return escaped
