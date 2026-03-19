"""Sitemap verification helpers for published WordPress content."""

from __future__ import annotations

from dataclasses import dataclass
import xml.etree.ElementTree as ET
from typing import List

import requests


@dataclass
class SitemapVerificationResult:
    found: bool
    checked_urls: List[str]
    matched_sitemap: str = ""
    error: str = ""

    def to_payload(self) -> dict[str, object]:
        return {
            "found": self.found,
            "checked_urls": list(self.checked_urls),
            "matched_sitemap": self.matched_sitemap,
            "error": self.error,
        }


def verify_post_url_in_sitemap(base_url: str, post_url: str, timeout_seconds: int = 15) -> SitemapVerificationResult:
    root_url = str(base_url or "").rstrip("/")
    target_url = str(post_url or "").strip()
    if not root_url or not target_url:
        return SitemapVerificationResult(found=False, checked_urls=[], error="base_url_or_post_url_missing")

    sitemap_url = f"{root_url}/sitemap.xml"
    checked = [sitemap_url]
    try:
        response = requests.get(sitemap_url, timeout=timeout_seconds)
        response.raise_for_status()
        child_sitemaps = _extract_sitemap_locations(response.text)
        if not child_sitemaps:
            found = _sitemap_contains_url(response.text, target_url)
            return SitemapVerificationResult(found=found, checked_urls=checked, matched_sitemap=sitemap_url if found else "")

        for child_url in child_sitemaps:
            checked.append(child_url)
            child_response = requests.get(child_url, timeout=timeout_seconds)
            child_response.raise_for_status()
            if _sitemap_contains_url(child_response.text, target_url):
                return SitemapVerificationResult(found=True, checked_urls=checked, matched_sitemap=child_url)

        return SitemapVerificationResult(found=False, checked_urls=checked)
    except requests.RequestException as exc:
        return SitemapVerificationResult(found=False, checked_urls=checked, error=str(exc))
    except ET.ParseError as exc:
        return SitemapVerificationResult(found=False, checked_urls=checked, error=f"xml_parse_error: {exc}")


def _extract_sitemap_locations(xml_text: str) -> list[str]:
    root = ET.fromstring(xml_text)
    if not root.tag.endswith("sitemapindex"):
        return []
    locations: list[str] = []
    for node in root.iter():
        if node.tag.endswith("loc") and node.text:
            locations.append(node.text.strip())
    return locations


def _sitemap_contains_url(xml_text: str, target_url: str) -> bool:
    root = ET.fromstring(xml_text)
    normalized_target = target_url.rstrip("/")
    for node in root.iter():
        if node.tag.endswith("loc") and node.text:
            if node.text.strip().rstrip("/") == normalized_target:
                return True
    return False
