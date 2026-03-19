"""Identify and optionally privatize placeholder WordPress posts/pages."""

from __future__ import annotations

import argparse
import re
from typing import Any, Iterable, Mapping

from japantravel.clients.wordpress_client import WordPressClient
from japantravel.modules.generation.seo import to_plain_text


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Privatize placeholder posts/pages on WordPress.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--include-pages", action="store_true")
    parser.add_argument("--limit", type=int, default=30)
    return parser.parse_args()


def _looks_like_placeholder(title: str, slug: str) -> bool:
    normalized_title = to_plain_text(title).lower()
    normalized_slug = to_plain_text(slug).lower()
    if not normalized_title:
        return True
    if normalized_slug in {"hello-world", "sample-page"}:
        return True
    if normalized_title in {"안녕하세요", "예제 페이지"}:
        return True
    if "smoke-test" in normalized_slug or "smoke test" in normalized_title:
        return True
    if re.fullmatch(r"\d+(?:-\d+)?", normalized_slug):
        return True
    return False


def _iter_items(items: Iterable[Mapping[str, Any]]) -> list[tuple[int, str, str]]:
    result: list[tuple[int, str, str]] = []
    for item in items:
        item_id = item.get("id")
        if not isinstance(item_id, int):
            continue
        title = to_plain_text((item.get("title") or {}).get("rendered") if isinstance(item.get("title"), Mapping) else item.get("title"))
        slug = to_plain_text(item.get("slug"))
        if _looks_like_placeholder(title, slug):
            result.append((item_id, title, slug))
    return result


def main() -> None:
    args = _parse_args()
    wp = WordPressClient()

    posts = wp.list_posts(per_page=max(args.limit, 1), orderby="date", order="desc", status="publish")
    pages = wp.list_pages(per_page=max(args.limit, 1), orderby="date", order="desc", status="publish") if args.include_pages else []

    targets = [("post", item_id, title, slug) for item_id, title, slug in _iter_items(posts)]
    targets.extend(("page", item_id, title, slug) for item_id, title, slug in _iter_items(pages))

    if not targets:
        print("No placeholder content found.")
        return

    for item_type, item_id, title, slug in targets:
        if args.dry_run:
            print(f"dry-run type={item_type} id={item_id} slug={slug} title={title}")
            continue

        if item_type == "post":
            wp.update_post(item_id, status="private")
        else:
            wp.update_page(item_id, status="private")
        print(f"privatized type={item_type} id={item_id} slug={slug} title={title}")


if __name__ == "__main__":
    main()
