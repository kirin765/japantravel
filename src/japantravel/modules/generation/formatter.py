"""Formatting helpers for generation outputs."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Mapping

from .pipeline import GeneratedArticle


def format_markdown(article: GeneratedArticle) -> str:
    lines: List[str] = []
    lines.append(f"# ✨ {article.title}")
    lines.append("")
    lines.append("## 🎯 한줄 요약")
    lines.append(article.summary)
    lines.append("")
    lines.append("## 🌤️ 인트로")
    lines.append(article.intro)
    lines.append("")
    lines.append("## 🧭 장소 추천 하이라이트")
    for section in article.place_sections:
        lines.append("")
        lines.append(f"### {section.title}")
        lines.append("<div class='tj-place-card'>")
        lines.append(section.body)
        lines.extend(_render_section_images(section.place_id, section.image_urls, max_images=2))
        lines.extend(_render_section_map(section.maps_url, section.map_embed_url, include_iframe=True))
        lines.append("</div>")
        lines.append("")

    lines.append("## 🗺️ 동선 제안")
    lines.append(article.route_suggestion)
    lines.append("")
    lines.append("## ✅ 체크리스트")
    for item in article.checklist:
        lines.append(f"- [ ] {item}")
    lines.append("")
    lines.append("## FAQ")
    for item in article.faq:
        lines.append(_build_faq_block(str(item)))
    lines.append("")
    lines.append("## 🏁 결론")
    lines.append(article.conclusion)
    lines.append("")
    lines.append("<div class='tj-readability-notice'>")
    lines.append("※ 모든 운영시간·혼잡도·가격은 방문 직전 최신 공지를 확인하세요.</div>")
    return "\n".join(lines)


def format_markdown_payload(payload: Mapping[str, Any], include_map_iframe: bool = True) -> str:
    lines: List[str] = []
    lines.append(f"# ✨ {payload.get('title', '여행 추천 글')}")
    lines.append("")
    lines.append("## 🎯 한줄 요약")
    lines.append(str(payload.get("summary", "")))
    lines.append("")
    lines.append("## 🌤️ 인트로")
    lines.append(str(payload.get("intro", "")))
    lines.append("")
    lines.append("## 🧭 장소 추천 하이라이트")
    for section in payload.get("place_sections", []):
        if not isinstance(section, Mapping):
            continue
        title = str(section.get("title", ""))
        body = str(section.get("body", ""))
        image_urls = _coerce_strings(section.get("image_urls", []))
        maps_url = str(section.get("maps_url", ""))
        map_embed_url = str(section.get("map_embed_url", ""))
        lines.append(f"### {title}")
        lines.append("<div class='tj-place-card'>")
        lines.append(body)
        lines.extend(_render_section_images(section.get("place_id", ""), image_urls, max_images=2))
        lines.extend(_render_section_map(maps_url, map_embed_url, include_iframe=include_map_iframe))
        lines.append("</div>")
        lines.append("")

    lines.append("## 🗺️ 동선 제안")
    lines.append(str(payload.get("route_suggestion", "")))
    lines.append("")
    lines.append("## ✅ 체크리스트")
    for item in payload.get("checklist", []):
        if item:
            lines.append(f"- [ ] {item}")
    lines.append("")
    lines.append("## FAQ")
    for item in payload.get("faq", []):
        lines.append(_build_faq_block(str(item)))
    lines.append("")
    lines.append("## 🏁 결론")
    lines.append(str(payload.get("conclusion", "")))
    lines.append("")
    lines.append("<div class='tj-readability-notice'>")
    lines.append("※ 모든 운영시간·혼잡도·가격은 방문 직전 최신 공지를 확인하세요.</div>")
    return "\n".join(lines)


def to_wordpress_blocks(article: GeneratedArticle) -> str:
    # Lightweight block-oriented format for copy/paste workflows.
    body = format_markdown(article)
    escaped = body.replace("<", "&lt;").replace(">", "&gt;")
    return f"<!-- wp:paragraph --><p>{escaped}</p><!-- /wp:paragraph -->"


def article_to_payload(article: GeneratedArticle) -> Dict[str, Any]:
    return article.to_payload()


def _render_section_images(place_id: str, images: List[str], max_images: int = 2) -> List[str]:
    lines: List[str] = []
    for url in images[:max_images]:
        if url:
            if place_id:
                lines.append(f"![{place_id}]({url})")
            else:
                lines.append(f"![]({url})")
    if not lines:
        return []
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


def _coerce_strings(values: Any) -> List[str]:
    if not values:
        return []
    if isinstance(values, list):
        return [str(v).strip() for v in values if str(v).strip()]
    if isinstance(values, str):
        return [values]
    return []


def _build_faq_block(item: str) -> str:
    if not item:
        return ""

    normalized = item.strip()

    # If a single FAQ item contains multiple Q/A pairs, split all pairs and render separately.
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

    # Parse markdown style list with `Q:` / `A:` pairs and force block separation.
    qa_pattern = re.compile(
        r"(?is)(?:^|\n)\s*Q\s*:\s*(.+?)\s*(?:\n|$)\s*A\s*:\s*(.+?)(?=(?:\n|^)\s*Q\s*:|\Z)",
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

    # Parse HTML list style fallback, often appears in WP-rendered content.
    if "<li>" in normalized and "summary" not in normalized:
        html_items = []
        for match in re.findall(
            r"<li>\s*(?:<p>)?\s*(?:\*|•)?\s*Q\s*[:.]?\s*(.+?)\s*(?:</p>)?\s*.*?A\s*[:.]?\s*(.+?)(?:</li>|</p>|$)",
            normalized,
            flags=re.S | re.I,
        ):
            q_text = re.sub(r"<[^>]+>", "", match[0] or "").strip()
            a_text = re.sub(r"<[^>]+>", "", match[1] or "").strip()
            q_text = re.sub(r"\s+", " ", q_text)
            a_text = re.sub(r"\s+", " ", a_text)
            if q_text and a_text:
                html_items.append(_build_single_faq(q_text, a_text))
        if not html_items:
            for match in re.findall(
                r"<li>\s*<p><strong>\s*([^<]+)\s*</strong></p>\s*<ul>\s*<li>\s*([^<]+)\s*</li>",
                normalized,
                flags=re.S | re.I,
            ):
                q_text = re.sub(r"\s+", " ", (match[0] or "").strip())
                a_text = re.sub(r"\s+", " ", (match[1] or "").strip())
                if q_text and a_text:
                    html_items.append(_build_single_faq(q_text, a_text))
        if html_items:
            return "<div class='tj-faq'>\n" + "\n".join(html_items) + "\n</div>"

    if "Q:" not in normalized and "A:" not in normalized:
        return f"- {normalized}"

    if normalized.startswith("Q:") and "A:" in normalized:
        q, a = normalized.split("A:", 1)
        return _build_single_faq(q.strip(), a.strip())

    return f"- {normalized}"


def _build_single_faq(question: str, answer: str) -> str:
    question_text = question.replace("Q:", "").replace("Q.", "").strip()
    question_text = re.sub(r"^\s*[\-\•\*]\s*", "", question_text)
    question_text = re.sub(r"\s+", " ", question_text)
    question_text = re.sub(r"\s*[\-\•\*]\s*$", "", question_text)
    answer_text = answer.replace("A:", "").strip()
    answer_text = re.sub(r"^\s*[\-\•\*]\s*", "", answer_text)
    answer_text = re.sub(r"\s+", " ", answer_text)
    answer_text = re.sub(r"\s*[\-\•\*]\s*$", "", answer_text)
    return (
        "<div class='tj-faq-item'>\n"
        f"<p><strong>Q.</strong> {question_text}</p>\n"
        f"<p><strong>A.</strong> {answer_text}</p>\n"
        "</div>"
    )
