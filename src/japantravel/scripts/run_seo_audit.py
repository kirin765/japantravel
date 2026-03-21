"""Run a live SEO audit against the configured site."""

from __future__ import annotations

import argparse
import json

from japantravel.config.settings import Settings
from japantravel.modules.seo_automation import audit_site


def main() -> None:
    settings = Settings()
    parser = argparse.ArgumentParser(description="Audit live SEO structure for japantravel.")
    parser.add_argument("--base-url", default=settings.wordpress_base_url or "https://www.japantravel.co.kr")
    parser.add_argument("--sample-posts", type=int, default=3)
    parser.add_argument("--format", choices=("json", "summary"), default="summary")
    args = parser.parse_args()

    report = audit_site(args.base_url, sample_posts=max(args.sample_posts, 1))
    payload = report.to_payload()

    if args.format == "json":
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    print(f"base_url={payload['base_url']}")
    print(f"audited_at={payload['audited_at']}")
    print(f"sitemap_xml_available={payload['sitemap_xml_available']}")
    print("robots.txt")
    print(payload["robots_txt"].strip())
    print("")
    for page in payload["pages"]:
        print(f"url={page['url']}")
        print(f"  title={page['title']}")
        print(f"  h1={page['h1_count']} h2={page['h2_count']} internal_links={page['internal_link_count']} schema={page['schema_count']}")
        print(
            "  images="
            f"{page['image_count']} missing_alt={page['missing_alt_count']} "
            f"non_lazy={page['non_lazy_image_count']} non_webp={page['non_webp_image_count']}"
        )
        print(f"  meta_description={'yes' if page['meta_description'] else 'no'} meta_keywords={'yes' if page['meta_keywords'] else 'no'}")
        if page["findings"]:
            print("  findings=" + " | ".join(page["findings"]))


if __name__ == "__main__":
    main()
