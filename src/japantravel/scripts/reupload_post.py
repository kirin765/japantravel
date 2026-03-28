"""Re-upload a single WordPress post by updating an existing post ID."""

from __future__ import annotations

import argparse
from pathlib import Path

from japantravel.clients.wordpress_client import WordPressClient
from japantravel.modules.publish.pipeline import PublishPipeline


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Re-upload an existing WordPress post.")
    parser.add_argument("--post-id", type=int, required=True)
    parser.add_argument("--title", required=True)
    parser.add_argument("--content-file", required=True)
    parser.add_argument("--status", choices=("draft", "pending_review", "publish"), default="draft")
    parser.add_argument("--slug")
    parser.add_argument("--excerpt", default="")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    content = Path(args.content_file).read_text(encoding="utf-8")

    wp = WordPressClient()
    publisher = PublishPipeline(wp_client=wp)
    result = publisher.publish(
        title=args.title,
        content=content,
        status=args.status,
        slug=args.slug,
        post_id=args.post_id,
        excerpt=args.excerpt,
        dry_run=args.dry_run,
    )
    print(result)


if __name__ == "__main__":
    main()
