"""Restyle recent WordPress posts using the current HTML post-processor."""

from __future__ import annotations

import argparse
from typing import Iterable, List

from japantravel.clients.wordpress_client import WordPressClient
from japantravel.modules.generation.formatter import restyle_existing_wordpress_html


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Restyle recent WordPress posts.")
    parser.add_argument("--post-id", dest="post_ids", action="append", type=int, default=[])
    parser.add_argument("--limit", type=int, default=3)
    parser.add_argument("--status", default="publish,draft,pending")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def _resolve_post_ids(wp: WordPressClient, requested_ids: Iterable[int], limit: int, status: str) -> List[int]:
    ids = [post_id for post_id in requested_ids if post_id > 0]
    if ids:
        return ids
    posts = wp.list_posts(per_page=max(limit, 1), orderby="date", order="desc", status=status)
    return [int(post.get("id")) for post in posts if isinstance(post.get("id"), int)]


def main() -> None:
    args = _parse_args()
    wp = WordPressClient()
    post_ids = _resolve_post_ids(wp, args.post_ids, args.limit, args.status)

    if not post_ids:
        print("No posts found.")
        return

    updated = 0
    skipped = 0
    for post_id in post_ids:
        post = wp.get_post(post_id)
        title = ((post.get("title") or {}).get("rendered") or "").strip()
        current = ((post.get("content") or {}).get("rendered") or "").strip()
        if not current:
            skipped += 1
            print(f"skip post_id={post_id} reason=empty-content title={title}")
            continue

        refreshed = restyle_existing_wordpress_html(current).strip()
        if refreshed == current:
            skipped += 1
            print(f"skip post_id={post_id} reason=no-change title={title}")
            continue

        if args.dry_run:
            updated += 1
            print(f"dry-run post_id={post_id} title={title}")
            continue

        wp.update_post(post_id, content=refreshed)
        updated += 1
        print(f"updated post_id={post_id} title={title}")

    print(f"done updated={updated} skipped={skipped} total={len(post_ids)}")


if __name__ == "__main__":
    main()
