"""Identify and optionally privatize placeholder or broken public WordPress URLs."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import re
from typing import Any, Callable, Iterable, Mapping

import requests

from japantravel.clients.wordpress_client import WordPressClient
from japantravel.config.settings import Settings
from japantravel.modules.generation.seo import to_plain_text
from japantravel.modules.publish.sitemap import verify_post_url_in_sitemap


@dataclass(frozen=True)
class CleanupTarget:
    item_type: str
    item_id: int
    title: str
    slug: str
    link: str
    reason: str
    frontend_status: int = 0


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Privatize placeholder or frontend-404 public WordPress content.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--status", default="publish")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--verify-sitemap", action="store_true")
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


def classify_content_item(
    item_type: str,
    item: Mapping[str, Any],
    *,
    timeout_seconds: int = 15,
) -> CleanupTarget | None:
    item_id = item.get("id")
    if not isinstance(item_id, int):
        return None
    title = to_plain_text((item.get("title") or {}).get("rendered") if isinstance(item.get("title"), Mapping) else item.get("title"))
    slug = to_plain_text(item.get("slug"))
    link = to_plain_text(item.get("link"))

    if _looks_like_placeholder(title, slug):
        return CleanupTarget(item_type=item_type, item_id=item_id, title=title, slug=slug, link=link, reason="placeholder")

    if link:
        status_code = fetch_frontend_status(link, timeout_seconds=timeout_seconds)
        if status_code >= 400:
            return CleanupTarget(
                item_type=item_type,
                item_id=item_id,
                title=title,
                slug=slug,
                link=link,
                reason="frontend_404",
                frontend_status=status_code,
            )
    return None


def fetch_frontend_status(url: str, timeout_seconds: int = 15) -> int:
    try:
        response = requests.get(url, timeout=timeout_seconds, allow_redirects=True)
        return int(response.status_code)
    except requests.RequestException:
        return 599


def iter_cleanup_targets(
    wp: WordPressClient,
    *,
    status: str = "publish",
    limit: int = 0,
    page_size: int = 100,
) -> list[CleanupTarget]:
    targets: list[CleanupTarget] = []
    for item_type, list_fn in (("post", wp.list_posts), ("page", wp.list_pages)):
        for item in _iter_all_items(list_fn, status=status, page_size=page_size, limit=limit):
            target = classify_content_item(item_type, item)
            if target is not None:
                targets.append(target)
                if limit > 0 and len(targets) >= limit:
                    return targets
    return targets


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


def main() -> None:
    args = _parse_args()
    settings = Settings()
    wp = WordPressClient()

    targets = iter_cleanup_targets(
        wp,
        status=args.status,
        limit=max(args.limit, 0),
        page_size=max(args.page_size, 1),
    )
    if not targets:
        print("No placeholder or broken public content found.")
        return

    updated = 0
    for target in targets:
        if args.dry_run:
            print(
                f"dry-run type={target.item_type} id={target.item_id} slug={target.slug}"
                f" reason={target.reason} frontend_status={target.frontend_status or '-'} title={target.title}"
            )
        else:
            if target.item_type == "post":
                wp.update_post(target.item_id, status="private")
            else:
                wp.update_page(target.item_id, status="private")
            updated += 1
            print(
                f"privatized type={target.item_type} id={target.item_id} slug={target.slug}"
                f" reason={target.reason} frontend_status={target.frontend_status or '-'} title={target.title}"
            )

        if args.verify_sitemap and settings.wordpress_base_url and target.link:
            verification = verify_post_url_in_sitemap(settings.wordpress_base_url, target.link).to_payload()
            print(
                "sitemap"
                f" id={target.item_id}"
                f" found={verification.get('found', False)}"
                f" matched={verification.get('matched_sitemap', '')}"
                f" error={verification.get('error', '')}"
            )

    print(f"done total={len(targets)} updated={updated}")


if __name__ == "__main__":
    main()
