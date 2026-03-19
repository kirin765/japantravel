"""Backfill SEO improvements for existing WordPress posts."""

from __future__ import annotations

import argparse
from typing import Any, Iterable, List, Mapping

from bs4 import BeautifulSoup

from japantravel.clients.wordpress_client import WordPressClient
from japantravel.config.settings import Settings
from japantravel.modules.generation.formatter import restyle_existing_wordpress_html
from japantravel.modules.generation.seo import (
    build_featured_media_alt_text,
    build_meta_description,
    build_primary_keyword,
    to_plain_text,
)
from japantravel.modules.publish.sitemap import verify_post_url_in_sitemap


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill SEO updates for recent WordPress posts.")
    parser.add_argument("--post-id", dest="post_ids", action="append", type=int, default=[])
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--status", default="publish")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verify-sitemap", action="store_true")
    return parser.parse_args()


def _resolve_post_ids(wp: WordPressClient, requested_ids: Iterable[int], limit: int, status: str) -> List[int]:
    ids = [post_id for post_id in requested_ids if post_id > 0]
    if ids:
        return ids
    posts = wp.list_posts(per_page=max(limit, 1), orderby="date", order="desc", status=status)
    return [int(post.get("id")) for post in posts if isinstance(post.get("id"), int)]


def _select_related_posts(
    wp: WordPressClient,
    categories: list[int],
    tags: list[int],
    current_post_id: int,
    current_title: str,
    limit: int = 3,
) -> list[dict[str, str]]:
    selected: list[dict[str, str]] = []
    seen: set[int] = {current_post_id}

    queries: list[dict[str, Any]] = []
    if categories and tags:
        queries.append({"categories": ",".join(str(value) for value in categories), "tags": ",".join(str(value) for value in tags)})
    if categories:
        queries.append({"categories": ",".join(str(value) for value in categories)})
    if tags:
        queries.append({"tags": ",".join(str(value) for value in tags)})
    queries.append({})

    normalized_title = to_plain_text(current_title).lower()
    for query in queries:
        posts = wp.list_posts(
            per_page=max(limit * 3, 6),
            orderby="date",
            order="desc",
            status="publish",
            **query,
        )
        for post in posts:
            post_id = post.get("id")
            if not isinstance(post_id, int) or post_id in seen:
                continue
            title = to_plain_text((post.get("title") or {}).get("rendered") if isinstance(post.get("title"), Mapping) else post.get("title"))
            slug = to_plain_text(post.get("slug")).lower()
            url = to_plain_text(post.get("link"))
            if not title or not url or title.lower() == normalized_title:
                continue
            if slug in {"hello-world", "sample-page"} or "smoke-test" in slug:
                continue
            if title in {"안녕하세요", "예제 페이지"}:
                continue
            seen.add(post_id)
            selected.append({"title": title, "url": url, "slug": slug})
            if len(selected) >= limit:
                return selected
    return selected


def _extract_place_names(content: str) -> list[str]:
    soup = BeautifulSoup(content or "", "html.parser")
    names: list[str] = []
    for heading in soup.find_all("h3"):
        cleaned = to_plain_text(heading.get_text(" ", strip=True))
        cleaned = cleaned.split("(")[0].strip()
        if cleaned and cleaned not in names:
            names.append(cleaned)
    return names


def main() -> None:
    args = _parse_args()
    settings = Settings()
    wp = WordPressClient()
    post_ids = _resolve_post_ids(wp, args.post_ids, args.limit, args.status)

    if not post_ids:
        print("No posts found.")
        return

    updated = 0
    skipped = 0
    for post_id in post_ids:
        post = wp.get_post(post_id)
        title = to_plain_text((post.get("title") or {}).get("rendered") if isinstance(post.get("title"), Mapping) else post.get("title"))
        current = ((post.get("content") or {}).get("rendered") or "").strip()
        if not current:
            skipped += 1
            print(f"skip post_id={post_id} reason=empty-content title={title}")
            continue

        primary_keyword = build_primary_keyword(title=title)
        related_posts = _select_related_posts(
            wp,
            categories=[int(value) for value in post.get("categories", []) if isinstance(value, int)],
            tags=[int(value) for value in post.get("tags", []) if isinstance(value, int)],
            current_post_id=post_id,
            current_title=title,
            limit=3,
        )
        refreshed = restyle_existing_wordpress_html(
            current,
            primary_keyword=primary_keyword,
            related_posts=related_posts,
        ).strip()

        excerpt = build_meta_description(
            title=title,
            summary=(post.get("excerpt") or {}).get("rendered", ""),
            intro=current,
        )
        existing_excerpt = to_plain_text((post.get("excerpt") or {}).get("rendered", ""))
        updates: dict[str, Any] = {}
        if refreshed and refreshed != current:
            updates["content"] = refreshed
        if excerpt and excerpt != existing_excerpt:
            updates["excerpt"] = excerpt

        featured_media = post.get("featured_media")
        featured_alt = build_featured_media_alt_text(primary_keyword, _extract_place_names(refreshed or current))

        if args.dry_run:
            if updates or featured_media:
                updated += 1
                print(f"dry-run post_id={post_id} title={title} updates={','.join(sorted(updates.keys())) or 'media-alt-only'}")
            else:
                skipped += 1
                print(f"skip post_id={post_id} reason=no-change title={title}")
            continue

        if updates:
            wp.update_post(post_id, **updates)
        if isinstance(featured_media, int) and featured_media > 0 and featured_alt:
            wp.update_media(featured_media, alt_text=featured_alt)

        if updates or (isinstance(featured_media, int) and featured_media > 0 and featured_alt):
            updated += 1
            print(f"updated post_id={post_id} title={title}")
        else:
            skipped += 1
            print(f"skip post_id={post_id} reason=no-change title={title}")

        if args.verify_sitemap and settings.wordpress_base_url and post.get("link"):
            verification = verify_post_url_in_sitemap(settings.wordpress_base_url, str(post.get("link"))).to_payload()
            print(
                "sitemap"
                f" post_id={post_id}"
                f" found={verification.get('found', False)}"
                f" matched={verification.get('matched_sitemap', '')}"
                f" error={verification.get('error', '')}"
            )

    print(f"done updated={updated} skipped={skipped} total={len(post_ids)}")


if __name__ == "__main__":
    main()
