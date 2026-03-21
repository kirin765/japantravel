"""SEO audit helpers for the live WordPress site."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from urllib.parse import urlparse
from typing import Any

import requests
from bs4 import BeautifulSoup


@dataclass
class PageAudit:
    url: str
    title: str = ""
    meta_description: str = ""
    meta_keywords: str = ""
    h1_count: int = 0
    h2_count: int = 0
    internal_link_count: int = 0
    schema_count: int = 0
    image_count: int = 0
    missing_alt_count: int = 0
    non_lazy_image_count: int = 0
    non_webp_image_count: int = 0
    findings: list[str] = field(default_factory=list)

    def to_payload(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "title": self.title,
            "meta_description": self.meta_description,
            "meta_keywords": self.meta_keywords,
            "h1_count": self.h1_count,
            "h2_count": self.h2_count,
            "internal_link_count": self.internal_link_count,
            "schema_count": self.schema_count,
            "image_count": self.image_count,
            "missing_alt_count": self.missing_alt_count,
            "non_lazy_image_count": self.non_lazy_image_count,
            "non_webp_image_count": self.non_webp_image_count,
            "findings": list(self.findings),
        }


@dataclass
class SiteAuditReport:
    base_url: str
    audited_at: str
    robots_txt: str
    sitemap_xml_available: bool
    pages: list[PageAudit]

    def to_payload(self) -> dict[str, Any]:
        return {
            "base_url": self.base_url,
            "audited_at": self.audited_at,
            "robots_txt": self.robots_txt,
            "sitemap_xml_available": self.sitemap_xml_available,
            "pages": [page.to_payload() for page in self.pages],
        }


def audit_site(base_url: str, sample_post_urls: list[str] | None = None, sample_posts: int = 3) -> SiteAuditReport:
    root = base_url.rstrip("/")
    robots_txt = requests.get(f"{root}/robots.txt", timeout=20).text
    sitemap_response = requests.get(f"{root}/sitemap.xml", timeout=20)
    sitemap_ok = sitemap_response.status_code < 400 and "<sitemap" in sitemap_response.text.lower()

    urls = [root]
    if sample_post_urls:
        urls.extend(sample_post_urls)
    else:
        try:
            response = requests.get(
                f"{root}/wp-json/wp/v2/posts",
                params={"per_page": max(sample_posts, 1), "orderby": "date", "order": "desc", "_fields": "link"},
                timeout=20,
            )
            for item in response.json():
                link = str(item.get("link") or "").strip()
                if link:
                    urls.append(link)
        except Exception:
            pass

    seen: set[str] = set()
    pages: list[PageAudit] = []
    for url in urls:
        if url in seen:
            continue
        seen.add(url)
        pages.append(audit_page(url, base_url=root))

    return SiteAuditReport(
        base_url=root,
        audited_at=datetime.now(timezone.utc).isoformat(),
        robots_txt=robots_txt,
        sitemap_xml_available=sitemap_ok,
        pages=pages,
    )


def audit_page(url: str, base_url: str) -> PageAudit:
    response = requests.get(url, timeout=20)
    soup = BeautifulSoup(response.text, "html.parser")
    base_host = urlparse(base_url).netloc

    title = soup.title.get_text(" ", strip=True) if soup.title else ""
    meta_description = _meta_content(soup, "description")
    meta_keywords = _meta_content(soup, "keywords")
    h1_count = len(soup.find_all("h1"))
    h2_count = len(soup.find_all("h2"))
    schema_count = len(soup.find_all("script", attrs={"type": "application/ld+json"}))
    images = soup.find_all("img")
    missing_alt = 0
    non_lazy = 0
    non_webp = 0
    for image in images:
        alt = str(image.get("alt") or "").strip()
        if not alt:
            missing_alt += 1
        if str(image.get("loading") or "").lower() != "lazy":
            non_lazy += 1
        src = str(image.get("src") or "").lower()
        if src and ".webp" not in src:
            non_webp += 1

    internal_links = 0
    for anchor in soup.find_all("a", href=True):
        href = anchor.get("href")
        if not href:
            continue
        parsed = urlparse(href)
        if not parsed.netloc or parsed.netloc == base_host:
            internal_links += 1

    findings: list[str] = []
    if not meta_description:
        findings.append("meta description missing")
    if not meta_keywords:
        findings.append("meta keywords missing")
    if h1_count != 1:
        findings.append(f"h1 count is {h1_count}, expected 1")
    if h2_count < 4:
        findings.append(f"h2 count is {h2_count}, expected at least 4")
    if internal_links < 6:
        findings.append(f"internal link count is {internal_links}, expected at least 6")
    if schema_count == 0:
        findings.append("schema markup missing")
    if missing_alt:
        findings.append(f"{missing_alt} images missing alt text")
    if non_lazy:
        findings.append(f"{non_lazy} images missing lazy loading")
    if non_webp:
        findings.append(f"{non_webp} images are not webp")

    return PageAudit(
        url=url,
        title=title,
        meta_description=meta_description,
        meta_keywords=meta_keywords,
        h1_count=h1_count,
        h2_count=h2_count,
        internal_link_count=internal_links,
        schema_count=schema_count,
        image_count=len(images),
        missing_alt_count=missing_alt,
        non_lazy_image_count=non_lazy,
        non_webp_image_count=non_webp,
        findings=findings,
    )


def _meta_content(soup: BeautifulSoup, name: str) -> str:
    tag = soup.find("meta", attrs={"name": name})
    if tag is None:
        return ""
    return str(tag.get("content") or "").strip()
