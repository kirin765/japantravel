"""Demote the newest duplicate WordPress post for a target region/topic."""

from __future__ import annotations

import argparse
from typing import Optional

from japantravel.clients.wordpress_client import WordPressClient
from japantravel.config.settings import Settings
from japantravel.modules.publish.sitemap import verify_post_url_in_sitemap
from japantravel.scheduler.jobs import (
    RecentPostSignature,
    _build_recent_post_signatures,
    _normalize_region_key,
    _title_tokens,
    _token_overlap,
)
from japantravel.storage.place_repository import PlaceRepository


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Demote the newest duplicate WordPress post for a target region.")
    parser.add_argument("--target-region", default="aogashima")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--status", default="publish")
    parser.add_argument("--demote-status", default="draft", choices=["draft", "private"])
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verify-sitemap", action="store_true")
    return parser.parse_args()


def _matches_target(signature: RecentPostSignature, target_region_key: str, target_tokens: set[str]) -> bool:
    if not target_region_key and not target_tokens:
        return True
    if target_region_key and signature.region_key == target_region_key:
        return True
    return bool(signature.title_tokens.intersection(target_tokens))


def _find_latest_duplicate(
    signatures: list[RecentPostSignature],
    target_region: str,
    threshold: float = 0.6,
) -> tuple[RecentPostSignature | None, RecentPostSignature | None, str]:
    target_region_key = _normalize_region_key(target_region)
    target_tokens = _title_tokens(target_region)

    for index, signature in enumerate(signatures):
        if not _matches_target(signature, target_region_key, target_tokens):
            continue
        for older in signatures[index + 1 :]:
            if not _matches_target(older, target_region_key, target_tokens):
                continue
            if signature.region_key and older.region_key and signature.region_key == older.region_key:
                return signature, older, "recent_region"
            if _token_overlap(signature.title_tokens, older.title_tokens) >= threshold:
                return signature, older, "recent_title_similarity"
    return None, None, ""


def _load_place_repo(settings: Settings) -> Optional[PlaceRepository]:
    if not settings.db_url:
        return None
    try:
        return PlaceRepository(settings.db_url)
    except Exception:
        return None


def _db_status_for_wp_status(status: str) -> str:
    normalized = str(status or "").strip().lower()
    if normalized in {"draft", "private"}:
        return "draft"
    if normalized == "publish":
        return "published"
    return "failed"


def main() -> None:
    args = _parse_args()
    settings = Settings()
    wp = WordPressClient()
    place_repo = _load_place_repo(settings)

    query: dict[str, str] = {}
    target_scope = args.target_region
    if args.target_region.strip():
        query["search"] = args.target_region.strip()
    posts = wp.list_posts(per_page=max(args.limit, 2), orderby="date", order="desc", status=args.status, **query)
    if query and not posts:
        posts = wp.list_posts(per_page=max(args.limit, 2), orderby="date", order="desc", status=args.status)
    else:
        target_scope = ""
    signatures = _build_recent_post_signatures(posts, place_repo)
    target, previous, reason = _find_latest_duplicate(
        signatures,
        target_region=target_scope,
        threshold=settings.recent_title_token_threshold,
    )

    if target is None:
        print("No duplicate topic found.")
        return

    if args.dry_run:
        print(
            "dry-run"
            f" post_id={target.post_id}"
            f" title={target.title}"
            f" region_key={target.region_key}"
            f" matched_post_id={previous.post_id if previous else 0}"
            f" reason={reason}"
        )
        return

    wp.update_post(target.post_id, status=args.demote_status)
    db_updated = False
    if place_repo is not None:
        db_updated = place_repo.update_published_article_status(
            wp_post_id=target.post_id,
            status=_db_status_for_wp_status(args.demote_status),
            published_at=None,
        )

    print(
        "updated"
        f" post_id={target.post_id}"
        f" title={target.title}"
        f" demote_status={args.demote_status}"
        f" db_updated={db_updated}"
        f" matched_post_id={previous.post_id if previous else 0}"
        f" reason={reason}"
    )

    if args.verify_sitemap and settings.wordpress_base_url and target.link:
        verification = verify_post_url_in_sitemap(settings.wordpress_base_url, target.link).to_payload()
        print(
            "sitemap"
            f" post_id={target.post_id}"
            f" found={verification.get('found', False)}"
            f" matched={verification.get('matched_sitemap', '')}"
            f" error={verification.get('error', '')}"
        )


if __name__ == "__main__":
    main()
