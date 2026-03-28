"""Normalize generated post taxonomy to one public category plus internal tags."""

from __future__ import annotations

import argparse
from typing import Any, Callable, Iterable, Mapping

from japantravel.clients.wordpress_client import WordPressClient
from japantravel.config.settings import Settings
from japantravel.modules.generation.seo import to_plain_text
from japantravel.modules.publish.pipeline import PublishPipeline


CONTENT_CATEGORY_SLUGS = {"rainy_day", "여행지", "맛집", "관광지", "카페", "숙소"}
SKIP_CATEGORY_SLUGS = {"japan", "asia", "미분류"}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Normalize WordPress post categories/tags for SEO.")
    parser.add_argument("--post-id", dest="post_ids", action="append", type=int, default=[])
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--status", default="publish")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--delete-empty-categories", action="store_true")
    return parser.parse_args()


def _normalize_tag_name(value: Any) -> str:
    import re
    from urllib.parse import unquote

    text = unquote(to_plain_text(value)).strip().lower()
    if not text:
        return ""
    text = re.sub(r"[^a-z0-9가-힣]+", "-", text)
    text = re.sub(r"-{2,}", "-", text).strip("-")
    return text


def _prefixed_tag(prefix: str, value: str) -> str:
    normalized = _normalize_tag_name(value)
    if not normalized:
        return ""
    return f"{prefix}-{normalized}"


def _iter_all_items(
    list_fn: Callable[..., list[Mapping[str, Any]]],
    *,
    status: str,
    page_size: int,
    limit: int,
) -> Iterable[Mapping[str, Any]]:
    page = 1
    yielded = 0
    while True:
        items = list_fn(per_page=max(1, min(page_size, 100)), page=page, orderby="date", order="desc", status=status)
        if not items:
            return
        for item in items:
            yield item
            yielded += 1
            if limit > 0 and yielded >= limit:
                return
        if len(items) < max(1, min(page_size, 100)):
            return
        page += 1


def _fetch_term_map(list_fn: Callable[..., list[Mapping[str, Any]]], page_size: int = 100) -> dict[int, Mapping[str, Any]]:
    page = 1
    items: dict[int, Mapping[str, Any]] = {}
    while True:
        batch = list_fn(per_page=max(1, min(page_size, 100)), page=page, hide_empty=False)
        if not batch:
            return items
        for item in batch:
            term_id = item.get("id")
            if isinstance(term_id, int):
                items[term_id] = item
        if len(batch) < max(1, min(page_size, 100)):
            return items
        page += 1


def _resolve_post_ids(wp: WordPressClient, requested_ids: list[int], status: str, limit: int, page_size: int) -> list[int]:
    if requested_ids:
        return [post_id for post_id in requested_ids if post_id > 0]
    return [
        int(post.get("id"))
        for post in _iter_all_items(wp.list_posts, status=status, page_size=page_size, limit=limit)
        if isinstance(post.get("id"), int)
    ]


def _build_target_tags(post: Mapping[str, Any], category_map: Mapping[int, Mapping[str, Any]], tag_map: Mapping[int, Mapping[str, Any]]) -> list[str]:
    tag_names: list[str] = []
    for tag_id in post.get("tags", []):
        if not isinstance(tag_id, int):
            continue
        current = tag_map.get(tag_id) or {}
        name = to_plain_text(current.get("name") or current.get("slug"))
        if name:
            tag_names.append(name)

    categories = [
        category_map.get(term_id) or {}
        for term_id in post.get("categories", [])
        if isinstance(term_id, int)
    ]
    for category in categories:
        slug = to_plain_text(category.get("slug"))
        name = to_plain_text(category.get("name") or slug)
        if not slug:
            continue
        if slug in SKIP_CATEGORY_SLUGS:
            continue
        if slug in CONTENT_CATEGORY_SLUGS:
            tag_names.append(_prefixed_tag("content", name))
            continue
        tag_names.append(_prefixed_tag("region", name))

    return [item for item in dict.fromkeys(tag_names) if item]


def main() -> None:
    args = _parse_args()
    settings = Settings()
    wp = WordPressClient()
    publisher = PublishPipeline(wp_client=wp)
    category_map = _fetch_term_map(wp.list_categories, page_size=max(args.page_size, 1))
    tag_map = _fetch_term_map(wp.list_tags, page_size=max(args.page_size, 1))
    keep_category_name = to_plain_text(settings.seo_single_post_category) or "japan"
    keep_category_id = publisher.resolve_term_ids(categories=[keep_category_name], tags=[])[0]

    post_ids = _resolve_post_ids(wp, args.post_ids, args.status, max(args.limit, 0), max(args.page_size, 1))
    if not post_ids:
        print("No posts found.")
        return

    updated = 0
    skipped = 0
    for post_id in post_ids:
        post = wp.get_post(post_id)
        title = to_plain_text((post.get("title") or {}).get("rendered") if isinstance(post.get("title"), Mapping) else post.get("title"))
        tag_names = _build_target_tags(post, category_map, tag_map)
        tag_ids = publisher.resolve_term_ids(categories=[], tags=tag_names)
        current_categories = [value for value in post.get("categories", []) if isinstance(value, int)]
        current_tags = [value for value in post.get("tags", []) if isinstance(value, int)]

        needs_category_update = current_categories != [keep_category_id]
        needs_tag_update = current_tags != tag_ids
        if not needs_category_update and not needs_tag_update:
            skipped += 1
            print(f"skip post_id={post_id} title={title} reason=no-change")
            continue

        if args.dry_run:
            updated += 1
            print(
                f"dry-run post_id={post_id} title={title}"
                f" categories={[keep_category_id]} tags={tag_names}"
            )
            continue

        wp.update_post(post_id, categories=[keep_category_id], tags=tag_ids)
        updated += 1
        print(f"updated post_id={post_id} title={title} categories={[keep_category_id]} tags={tag_names}")

    if args.delete_empty_categories:
        category_map = _fetch_term_map(wp.list_categories, page_size=max(args.page_size, 1))
        keep_slug = _normalize_tag_name(keep_category_name)
        for category_id, category in sorted(category_map.items()):
            slug = _normalize_tag_name(category.get("slug"))
            if not slug or slug == keep_slug:
                continue
            if int(category.get("count") or 0) > 0:
                continue
            if args.dry_run:
                print(f"dry-run delete-category id={category_id} slug={category.get('slug')} name={category.get('name')}")
                continue
            try:
                wp.delete_category(category_id, force=True)
                print(f"deleted-category id={category_id} slug={category.get('slug')} name={category.get('name')}")
            except Exception as exc:
                print(f"delete-category-failed id={category_id} slug={category.get('slug')} error={exc}")

    print(f"done total={len(post_ids)} updated={updated} skipped={skipped}")


if __name__ == "__main__":
    main()
