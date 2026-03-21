"""Robots and sitemap asset builders."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable, Mapping
import html


def render_robots_txt(
    base_url: str,
    *,
    disallow_paths: Iterable[str] = ("/wp-admin/",),
    allow_paths: Iterable[str] = ("/wp-admin/admin-ajax.php",),
    include_sitemap: bool = True,
) -> str:
    root = base_url.rstrip("/")
    lines = ["User-agent: *"]
    for value in disallow_paths:
        path = str(value).strip()
        if path:
            lines.append(f"Disallow: {path}")
    for value in allow_paths:
        path = str(value).strip()
        if path:
            lines.append(f"Allow: {path}")
    if include_sitemap and root:
        lines.append("")
        lines.append(f"Sitemap: {root}/sitemap.xml")
    return "\n".join(lines).strip() + "\n"


def render_sitemap_xml(urls: Iterable[str | Mapping[str, str]]) -> str:
    timestamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    items = []
    for value in urls:
        loc = ""
        lastmod = timestamp
        if isinstance(value, Mapping):
            loc = str(value.get("loc") or value.get("url") or "").strip()
            lastmod = str(value.get("lastmod") or timestamp).strip() or timestamp
        else:
            loc = str(value).strip()
        if not loc:
            continue
        items.append(
            "  <url>\n"
            f"    <loc>{html.escape(loc)}</loc>\n"
            f"    <lastmod>{html.escape(lastmod)}</lastmod>\n"
            "  </url>"
        )
    return (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        + "\n".join(items)
        + "\n</urlset>\n"
    )
