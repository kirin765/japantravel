"""Generate robots.txt and sitemap.xml from live WordPress content."""

from __future__ import annotations

import argparse
from pathlib import Path

from japantravel.clients.wordpress_client import WordPressClient
from japantravel.config.settings import Settings
from japantravel.modules.seo_automation import render_robots_txt, render_sitemap_xml


def main() -> None:
    settings = Settings()
    parser = argparse.ArgumentParser(description="Generate SEO asset files from WordPress content.")
    parser.add_argument("--output-dir", default="tmp/seo-assets")
    parser.add_argument("--base-url", default=settings.wordpress_base_url or "https://www.japantravel.co.kr")
    parser.add_argument("--limit", type=int, default=200)
    args = parser.parse_args()

    wp = WordPressClient()
    urls = _collect_public_urls(wp, limit=max(args.limit, 1))

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    robots_path = output_dir / "robots.txt"
    sitemap_path = output_dir / "sitemap.xml"

    robots_path.write_text(render_robots_txt(args.base_url), encoding="utf-8")
    sitemap_path.write_text(render_sitemap_xml(urls), encoding="utf-8")

    print(f"robots={robots_path}")
    print(f"sitemap={sitemap_path}")
    print(f"url_count={len(urls)}")


def _collect_public_urls(wp: WordPressClient, limit: int) -> list[dict[str, str]]:
    urls: list[dict[str, str]] = []
    for item in wp.list_posts(per_page=limit, orderby="date", order="desc", status="publish"):
        link = str(item.get("link") or "").strip()
        if link:
            urls.append({"loc": link})
    for item in wp.list_pages(per_page=limit, orderby="date", order="desc", status="publish"):
        link = str(item.get("link") or "").strip()
        if link:
            urls.append({"loc": link})
    return urls


if __name__ == "__main__":
    main()
