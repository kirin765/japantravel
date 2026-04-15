"""Generate and optionally publish programmatic SEO landing pages."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Iterable

from japantravel.clients.wordpress_client import WordPressClient
from japantravel.config.settings import Settings
from japantravel.modules.generation.formatter import build_post_meta_description, format_wordpress_html_payload
from japantravel.modules.publish.pipeline import PublishPipeline
from japantravel.modules.seo_automation import (
    build_keyword_target,
    build_programmatic_page_payload,
    expand_core_keyword_targets,
)
from japantravel.modules.seo_automation.renderer import render_full_html_document
from japantravel.storage.place_repository import PlaceRepository


def main() -> None:
    settings = Settings()
    parser = argparse.ArgumentParser(description="Build programmatic SEO landing pages.")
    parser.add_argument("--keyword", action="append", default=[])
    parser.add_argument("--keywords-file")
    parser.add_argument("--auto-core", action="store_true")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--status", choices=("draft", "publish"), default="draft")
    parser.add_argument("--output-dir")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    targets = _load_targets(args.keyword, args.keywords_file, args.auto_core, args.limit)
    if not targets:
        raise SystemExit("No keyword targets were provided.")

    repo = PlaceRepository(settings.db_url) if settings.db_url else None
    wp = None if args.dry_run else WordPressClient()
    publisher = PublishPipeline(wp_client=wp) if wp is not None else None

    output_dir = Path(args.output_dir) if args.output_dir else None
    if output_dir is not None:
        output_dir.mkdir(parents=True, exist_ok=True)

    for target in targets:
        places = _load_places(repo, target.region_name)
        internal_links = _load_internal_links(wp, target) if wp is not None else {}
        page = build_programmatic_page_payload(target, places, internal_links=internal_links)
        canonical_url = ""
        if settings.wordpress_base_url:
            canonical_url = f"{settings.wordpress_base_url.rstrip('/')}{target.canonical_path}"
        full_html = render_full_html_document(page.payload, canonical_url=canonical_url)

        if output_dir is not None:
            output_path = output_dir / f"{target.leaf_slug}.html"
            output_path.write_text(full_html, encoding="utf-8")
            print(f"saved_html={output_path}")

        if publisher is None:
            print(
                json.dumps(
                    {
                        "keyword": target.keyword,
                        "canonical_path": target.canonical_path,
                        "excerpt": build_post_meta_description(page.payload),
                    },
                    ensure_ascii=False,
                )
            )
            continue

        parent_id = _ensure_page_hierarchy(wp, publisher, target, status=args.status)
        page_content = format_wordpress_html_payload(page.payload)
        meta_fields = _build_meta_fields(settings, page.payload, canonical_url)
        existing = _find_page_by_slug(wp, target.leaf_slug, parent=parent_id)
        excerpt = build_post_meta_description(page.payload)

        if existing is not None:
            result = wp.update_page(
                int(existing["id"]),
                title=page.payload["title"],
                slug=target.leaf_slug,
                status=args.status,
                parent=parent_id,
                excerpt=excerpt,
                content=page_content,
                meta=meta_fields or None,
            )
        else:
            result = publisher.publish_page(
                title=page.payload["title"],
                content=page_content,
                status=args.status,
                slug=target.leaf_slug,
                excerpt=excerpt,
                parent=parent_id,
                meta_fields=meta_fields,
                dry_run=False,
            )

        print(
            json.dumps(
                {
                    "keyword": target.keyword,
                    "post_id": result.get("id") or result.get("post_id"),
                    "slug": result.get("slug", target.leaf_slug),
                    "status": result.get("status") or result.get("actual_status") or args.status,
                    "canonical_path": target.canonical_path,
                },
                ensure_ascii=False,
            )
        )


def _load_targets(raw_keywords: Iterable[str], keyword_file: str | None, auto_core: bool, limit: int) -> list[Any]:
    values: list[str] = []
    for raw in raw_keywords:
        if raw and raw.strip():
            values.append(raw.strip())
    if keyword_file:
        data = Path(keyword_file).read_text(encoding="utf-8")
        if keyword_file.endswith(".json"):
            parsed = json.loads(data)
            if isinstance(parsed, list):
                for item in parsed:
                    value = str(item).strip()
                    if value:
                        values.append(value)
        else:
            for line in data.splitlines():
                value = line.strip()
                if value:
                    values.append(value)
    targets = [build_keyword_target(value) for value in values]
    if auto_core:
        targets.extend(expand_core_keyword_targets(limit=limit))
    deduped = []
    seen: set[str] = set()
    for target in targets:
        if target.keyword in seen:
            continue
        seen.add(target.keyword)
        deduped.append(target)
        if len(deduped) >= max(limit, 1):
            break
    return deduped


def _load_places(repo: PlaceRepository | None, region_name: str) -> list[dict[str, Any]]:
    if repo is None:
        return []
    places = repo.fetch_reusable_candidates(city=region_name, country="Japan", limit=120, stale_days=90, strict_fields=False)
    if places:
        return places
    return repo.fetch_reusable_candidates(country="Japan", limit=120, stale_days=90, strict_fields=False)


def _load_internal_links(wp: WordPressClient | None, target: Any) -> dict[str, list[dict[str, str]]]:
    if wp is None:
        return {}
    same_region = _search_links(wp, target.region_name, limit=5)
    same_category = _search_links(wp, target.category_name, limit=3, exclude_slugs={item["slug"] for item in same_region})
    return {"same_region": same_region, "same_category": same_category}


def _search_links(
    wp: WordPressClient,
    query: str,
    *,
    limit: int,
    exclude_slugs: set[str] | None = None,
) -> list[dict[str, str]]:
    if not query:
        return []
    exclude = exclude_slugs or set()
    selected: list[dict[str, str]] = []
    for loader in (wp.list_posts, wp.list_pages):
        try:
            items = loader(per_page=max(limit * 3, 10), search=query, orderby="date", order="desc", status="publish")
        except Exception:
            continue
        for item in items:
            title = _title_text(item)
            link = str(item.get("link") or "").strip()
            slug = str(item.get("slug") or "").strip()
            if not title or not link or slug in exclude:
                continue
            exclude.add(slug)
            selected.append({"title": title, "url": link, "slug": slug})
            if len(selected) >= limit:
                return selected
    return selected


def _ensure_page_hierarchy(wp: WordPressClient, publisher: PublishPipeline, target: Any, status: str) -> int:
    root_page = _find_page_by_slug(wp, "japan", parent=0)
    if root_page is None:
        root_result = publisher.publish_page(
            title="Japan",
            content="<div class=\"jt-article\"><p>Programmatic SEO root page.</p></div>",
            status=status,
            slug="japan",
            excerpt="일본 여행 지역 허브 페이지",
            parent=None,
            dry_run=False,
        )
        root_id = int(root_result.get("post_id") or 0)
    else:
        root_id = int(root_page["id"])

    region_page = _find_page_by_slug(wp, target.region_slug, parent=root_id)
    if region_page is None:
        region_result = publisher.publish_page(
            title=target.region_name,
            content=f"<div class=\"jt-article\"><p>{target.region_name} 관련 여행 키워드를 모은 허브 페이지입니다.</p></div>",
            status=status,
            slug=target.region_slug,
            excerpt=f"{target.region_name} 여행 허브 페이지",
            parent=root_id,
            dry_run=False,
        )
        return int(region_result.get("post_id") or 0)
    return int(region_page["id"])


def _find_page_by_slug(wp: WordPressClient, slug: str, parent: int | None) -> dict[str, Any] | None:
    params: dict[str, Any] = {"per_page": 100, "search": slug, "status": "publish,draft,pending,private"}
    if parent is not None:
        params["parent"] = parent
    items = wp.list_pages(**params)
    for item in items:
        if str(item.get("slug") or "").strip() == slug and int(item.get("parent") or 0) == int(parent or 0):
            return item
    return None


def _build_meta_fields(settings: Settings, payload: dict[str, Any], canonical_url: str) -> dict[str, Any]:
    seo = payload.get("seo") if isinstance(payload.get("seo"), dict) else {}
    fields: dict[str, Any] = {}
    if settings.wordpress_meta_title_key and seo.get("title_tag"):
        fields[settings.wordpress_meta_title_key] = seo["title_tag"]
    if settings.wordpress_meta_description_key:
        description = build_post_meta_description(payload)
        if description:
            fields[settings.wordpress_meta_description_key] = description
    if settings.wordpress_meta_keywords_key and seo.get("keywords"):
        fields[settings.wordpress_meta_keywords_key] = ", ".join(str(item) for item in seo["keywords"])
    if settings.wordpress_meta_canonical_key and canonical_url:
        fields[settings.wordpress_meta_canonical_key] = canonical_url
    return fields


def _title_text(item: dict[str, Any]) -> str:
    title = item.get("title")
    if isinstance(title, dict):
        return str(title.get("rendered") or "").strip()
    return str(title or "").strip()


if __name__ == "__main__":
    main()
