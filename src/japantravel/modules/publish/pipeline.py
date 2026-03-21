"""WordPress publish pipeline for travel article drafts."""

from __future__ import annotations

import os
import re
import unicodedata
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence

from ...clients.wordpress_client import WordPressClient


@dataclass
class PublishResult:
    requested_status: str
    actual_status: str
    post_id: int
    post_url: Optional[str]
    slug: str
    term_ids: Dict[str, List[int]]
    featured_media_id: Optional[int]
    featured_media_ids: List[int]

    def to_payload(self) -> Dict[str, Any]:
        return {
            "requested_status": self.requested_status,
            "actual_status": self.actual_status,
            "post_id": self.post_id,
            "post_url": self.post_url,
            "slug": self.slug,
            "term_ids": self.term_ids,
            "featured_media_id": self.featured_media_id,
            "featured_media_ids": self.featured_media_ids,
        }


class PublishPipeline:
    """Wrap WordPress REST operations for draft/pending_review/publish workflows."""

    STATUS_MAP = {
        "draft": "draft",
        "pending_review": "pending",
        "publish": "publish",
    }

    def __init__(self, wp_client: WordPressClient):
        self.wp = wp_client
        self.logger = logging.getLogger(self.__class__.__name__)

    def resolve_term_ids(
        self,
        categories: Optional[Sequence[Any]] = None,
        tags: Optional[Sequence[Any]] = None,
    ) -> Dict[str, List[int]]:
        return {
            "categories": self._resolve_terms(
                term_values=categories or [],
                resolve_fn=self._ensure_category,
                field_name="카테고리",
            ),
            "tags": self._resolve_terms(
                term_values=tags or [],
                resolve_fn=self._ensure_tag,
                field_name="태그",
            ),
        }

    def publish(
        self,
        title: str,
        content: str,
        status: str = "draft",
        slug: Optional[str] = None,
        categories: Optional[Sequence[Any]] = None,
        tags: Optional[Sequence[Any]] = None,
        featured_media_urls: Optional[Sequence[Any]] = None,
        featured_media: Optional[Any] = None,
        excerpt: str = "",
        featured_media_alt_text: str = "",
        meta_fields: Optional[Mapping[str, Any]] = None,
        dry_run: bool = False,
        **extra_fields: Any,
    ) -> Dict[str, Any]:
        normalized_status = self._normalize_status(status)
        normalized_title = self._normalize_wp_title(title)
        final_slug = self._build_slug(slug or normalized_title)

        term_ids = self.resolve_term_ids(categories=categories, tags=tags)
        category_ids = term_ids["categories"]
        tag_ids = term_ids["tags"]
        featured_media_ids: List[int] = []
        featured_media_id = None
        try:
            featured_media_ids = self._resolve_featured_media_candidates(featured_media_urls or [])
            featured_media_id = featured_media_ids[0] if featured_media_ids else None
        except Exception as exc:
            self.logger.warning("featured media URL/file resolution failed: %s", exc)
        if featured_media_id is None:
            try:
                featured_media_id = self._resolve_featured_media(featured_media)
            except Exception as exc:
                self.logger.warning("featured media fallback resolution failed: %s", exc)

        payload: Dict[str, Any] = {
            "title": normalized_title,
            "content": content,
            "status": normalized_status,
            "slug": final_slug,
            "excerpt": excerpt,
        }

        if category_ids:
            payload["categories"] = category_ids
        if tag_ids:
            payload["tags"] = tag_ids
        if featured_media_id:
            payload["featured_media"] = featured_media_id
        if meta_fields:
            payload["meta"] = dict(meta_fields)

        payload.update(extra_fields)

        if dry_run:
            media_ids = list(dict.fromkeys([featured_media_id] + featured_media_ids)) if featured_media_id else featured_media_ids
            return {
                "requested_status": status,
                "actual_status": normalized_status,
                "post_id": None,
                "post_url": None,
                "slug": final_slug,
                "term_ids": {"categories": category_ids, "tags": tag_ids},
                "featured_media_id": featured_media_id,
                "featured_media_ids": media_ids,
                "payload": payload,
                "message": "Dry-run completed; request not sent.",
            }

        response = self.wp.create_post(**payload)
        media_ids = list(dict.fromkeys([featured_media_id] + featured_media_ids)) if featured_media_id else featured_media_ids
        self._update_media_alt_text(media_ids, featured_media_alt_text)
        return {
            "requested_status": status,
            "actual_status": response.get("status", normalized_status),
            "post_id": response.get("id"),
            "post_url": response.get("link"),
            "slug": response.get("slug", final_slug),
            "term_ids": {"categories": category_ids, "tags": tag_ids},
            "featured_media_id": featured_media_id,
            "featured_media_ids": media_ids,
            "wp_response": response,
        }

    def publish_page(
        self,
        title: str,
        content: str,
        status: str = "draft",
        slug: Optional[str] = None,
        excerpt: str = "",
        parent: Optional[int] = None,
        meta_fields: Optional[Mapping[str, Any]] = None,
        dry_run: bool = False,
        **extra_fields: Any,
    ) -> Dict[str, Any]:
        normalized_status = self._normalize_status(status)
        normalized_title = self._normalize_wp_title(title)
        final_slug = self._build_slug(slug or normalized_title)

        payload: Dict[str, Any] = {
            "title": normalized_title,
            "content": content,
            "status": normalized_status,
            "slug": final_slug,
            "excerpt": excerpt,
        }
        if parent:
            payload["parent"] = parent
        if meta_fields:
            payload["meta"] = dict(meta_fields)
        payload.update(extra_fields)

        if dry_run:
            return {
                "requested_status": status,
                "actual_status": normalized_status,
                "post_id": None,
                "post_url": None,
                "slug": final_slug,
                "term_ids": {"categories": [], "tags": []},
                "featured_media_id": None,
                "featured_media_ids": [],
                "payload": payload,
                "message": "Dry-run completed; request not sent.",
            }

        response = self.wp.create_page(**payload)
        return {
            "requested_status": status,
            "actual_status": response.get("status", normalized_status),
            "post_id": response.get("id"),
            "post_url": response.get("link"),
            "slug": response.get("slug", final_slug),
            "term_ids": {"categories": [], "tags": []},
            "featured_media_id": None,
            "featured_media_ids": [],
            "wp_response": response,
        }

    def _normalize_status(self, status: str) -> str:
        if status not in self.STATUS_MAP:
            raise ValueError(f"Unsupported status: {status}")
        return self.STATUS_MAP[status]

    def _build_slug(self, text: str, max_length: int = 60) -> str:
        if not text:
            return "draft-post"

        normalized = self._clean_title_text(text)
        normalized = unicodedata.normalize("NFKC", normalized).strip().lower()
        normalized = normalized.replace(" ", "-")
        normalized = re.sub(r"[^a-z0-9가-힣\\-]", "", normalized)
        normalized = re.sub(r"-{2,}", "-", normalized).strip("-")
        if not normalized:
            normalized = "post"

        if len(normalized) > max_length:
            return normalized[:max_length].strip("-")
        return normalized

    @staticmethod
    def _normalize_wp_title(text: str, max_len: int = 48) -> str:
        if not text:
            return "Travel Draft"
        first_lines = [line.strip() for line in text.splitlines() if line.strip()]
        normalized = first_lines[0] if first_lines else str(text).strip()
        normalized = PublishPipeline._clean_title_text(normalized.replace("###", "").replace("**", "").strip())
        normalized = re.sub(r"\s{2,}", " ", normalized)
        if len(normalized) > max_len:
            normalized = normalized[: max_len - 1].rstrip()
            return f"{normalized}…"
        return normalized

    @staticmethod
    def _clean_title_text(text: str) -> str:
        cleaned = re.sub(r"^\s*(제목|타이틀|title)\s*[:：\-]\s*", "", str(text).strip(), flags=re.IGNORECASE)
        return cleaned.strip("\"'[]() ")

    def _resolve_terms(
        self,
        term_values: Sequence[Any],
        resolve_fn,
        field_name: str,
    ) -> List[int]:
        ids: List[int] = []
        for value in term_values:
            term_id = resolve_fn(value)
            if term_id is None:
                continue
            if not isinstance(term_id, int) or term_id <= 0:
                raise ValueError(f"{field_name} value must resolve to positive int id: {value}")
            if term_id not in ids:
                ids.append(term_id)
        return ids

    def _ensure_category(self, value: Any) -> Optional[int]:
        return self._ensure_term(
            value=value,
            list_fn=self.wp.list_categories,
            create_fn=self.wp.create_category,
            field="categories",
        )

    def _ensure_tag(self, value: Any) -> Optional[int]:
        return self._ensure_term(
            value=value,
            list_fn=self.wp.list_tags,
            create_fn=self.wp.create_tag,
            field="tags",
        )

    def _ensure_term(
        self,
        value: Any,
        list_fn,
        create_fn,
        field: str,
    ) -> Optional[int]:
        if isinstance(value, bool):
            return None

        if isinstance(value, int):
            return value

        if isinstance(value, Mapping):
            if "id" in value and isinstance(value["id"], int):
                return value["id"]
            name = value.get("name")
            if isinstance(name, str) and name.strip():
                return self._find_or_create_term(list_fn=list_fn, create_fn=create_fn, name=name.strip())
            slug = value.get("slug")
            if isinstance(slug, str) and slug.strip():
                existing = list_fn(per_page=100, search=slug)
                for item in existing:
                    if slug == item.get("slug"):
                        return int(item["id"])
            return None

        if isinstance(value, str):
            txt = value.strip()
            if not txt:
                return None
            return self._find_or_create_term(list_fn=list_fn, create_fn=create_fn, name=txt)

        return None

    def _find_or_create_term(self, list_fn, create_fn, name: str) -> Optional[int]:
        existing = list_fn(per_page=100, search=name)
        for item in existing:
            if item.get("name") == name or item.get("slug") == self._build_slug(name):
                return int(item["id"])
        created = create_fn(name=name)
        return int(created["id"]) if created and created.get("id") else None

    def _resolve_featured_media(self, featured_media: Optional[Any]) -> Optional[int]:
        if featured_media is None:
            return None

        if isinstance(featured_media, (list, tuple)):
            for item in featured_media:
                resolved = self._resolve_featured_media(item)
                if resolved is not None:
                    return resolved
            return None

        if isinstance(featured_media, int):
            return featured_media

        if isinstance(featured_media, Mapping):
            media_id = featured_media.get("id")
            if isinstance(media_id, int):
                return media_id
            file_path = featured_media.get("file_path")
            if isinstance(file_path, str):
                return self._upload_media_file(file_path)
            file_url = featured_media.get("file_url")
            if isinstance(file_url, str):
                return self._upload_media_url(file_url)

        if isinstance(featured_media, str):
            if os.path.isfile(featured_media):
                return self._upload_media_file(featured_media)
            if featured_media.startswith("http://") or featured_media.startswith("https://"):
                return self._upload_media_url(featured_media)

        raise ValueError("featured_media should be int(media_id), mapping(id|file_path|file_url), URL, or local file path.")

    def _resolve_featured_media_candidates(self, featured_media: Optional[Any], max_count: int = 4) -> List[int]:
        if featured_media is None:
            return []

        result: List[int] = []
        candidates = featured_media if isinstance(featured_media, (list, tuple)) else [featured_media]

        for item in candidates:
            if len(result) >= max_count:
                break
            resolved = self._resolve_featured_media(item)
            if resolved is None or not isinstance(resolved, int) or resolved <= 0:
                continue
            if resolved not in result:
                result.append(resolved)
        return result

    def _upload_media_file(self, file_path: str) -> int:
        if not os.path.isfile(file_path):
            raise FileNotFoundError(f"Featured media file not found: {file_path}")
        response = self.wp.upload_media(file_path=file_path)
        return int(response["id"])

    def _upload_media_url(self, file_url: str) -> int:
        response = self.wp.upload_media_from_url(file_url)
        return int(response["id"])

    def _update_media_alt_text(self, media_ids: Sequence[int], alt_text: str) -> None:
        cleaned = re.sub(r"\s+", " ", str(alt_text or "").strip())
        if not cleaned:
            return
        for media_id in media_ids:
            if not isinstance(media_id, int) or media_id <= 0:
                continue
            try:
                self.wp.update_media(media_id, alt_text=cleaned)
            except Exception as exc:
                self.logger.warning("media alt text update failed media_id=%s error=%s", media_id, exc)
