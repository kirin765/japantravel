"""WordPress REST API wrapper."""

from __future__ import annotations

import mimetypes
import os
from io import BytesIO
from pathlib import Path
from urllib.parse import urlparse
from typing import Any, Dict, Optional

from requests.auth import HTTPBasicAuth

from ..config.settings import Settings
from .base import BaseClient

try:
    from PIL import Image
except ModuleNotFoundError:  # pragma: no cover - optional runtime dependency
    Image = None  # type: ignore[assignment]


class WordPressClient(BaseClient):
    def __init__(
        self,
        base_url: Optional[str] = None,
        username: Optional[str] = None,
        app_password: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
        retry_attempts: Optional[int] = None,
    ):
        settings = Settings()
        wp_base_url = base_url or settings.wordpress_base_url
        wp_username = username or settings.wordpress_username
        wp_password = app_password or settings.wordpress_app_password

        if not wp_base_url or not wp_username or not wp_password:
            raise ValueError(
                "WORDPRESS_BASE_URL, WORDPRESS_USERNAME, WORDPRESS_APP_PASSWORD are required."
            )

        super().__init__(
            base_url=f"{wp_base_url.rstrip('/')}/wp-json/{settings.wordpress_rest_api_version}",
            timeout_seconds=timeout_seconds or settings.request_timeout_seconds,
            headers={"Accept": "application/json"},
            retry_attempts=retry_attempts or settings.http_retry_count,
            retry_min_wait=settings.http_retry_backoff,
            retry_max_wait=settings.http_retry_backoff * 4,
        )

        self.session.auth = HTTPBasicAuth(wp_username, wp_password)

    def create_post(self, title: str, content: str, status: str = "draft", **extra: Any) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "title": title,
            "content": content,
            "status": status,
        }
        payload.update(extra)
        return self.json_request("POST", "/posts", json=payload)

    def create_page(self, title: str, content: str, status: str = "draft", **extra: Any) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "title": title,
            "content": content,
            "status": status,
        }
        payload.update(extra)
        return self.json_request("POST", "/pages", json=payload)

    def update_post(self, post_id: int, **fields: Any) -> Dict[str, Any]:
        return self.json_request("POST", f"/posts/{post_id}", json=fields)

    def get_post(self, post_id: int, **params: Any) -> Dict[str, Any]:
        return self.json_request("GET", f"/posts/{post_id}", params=params or None)

    def list_posts(self, **params: Any) -> list[Dict[str, Any]]:
        return self.json_request("GET", "/posts", params=params)

    def list_pages(self, **params: Any) -> list[Dict[str, Any]]:
        return self.json_request("GET", "/pages", params=params)

    def get_page(self, page_id: int, **params: Any) -> Dict[str, Any]:
        return self.json_request("GET", f"/pages/{page_id}", params=params or None)

    def update_page(self, page_id: int, **fields: Any) -> Dict[str, Any]:
        return self.json_request("POST", f"/pages/{page_id}", json=fields)

    def upload_media(self, file_path: str, filename: Optional[str] = None) -> Dict[str, Any]:
        file_name = filename or file_path.split("/")[-1]
        with open(file_path, "rb") as media_file:
            files = {"file": (file_name, media_file, "application/octet-stream")}
            return self.json_request("POST", "/media", files=files)

    def upload_media_from_url(self, url: str, filename: Optional[str] = None) -> Dict[str, Any]:
        parsed = urlparse(url)
        if not parsed.scheme.startswith("http"):
            raise ValueError(f"Invalid media URL: {url}")

        response = self.session.get(url, timeout=self.timeout_seconds)
        if response.status_code >= 400:
            raise ValueError(f"Failed to download media from URL: {url}")

        if not response.content:
            raise ValueError(f"Empty media content from URL: {url}")

        inferred_name = filename or Path(parsed.path).name
        if not inferred_name:
            inferred_name = "media"
        content_type = response.headers.get("content-type", "application/octet-stream").split(";")[0].strip()
        if "." not in inferred_name:
            extension = mimetypes.guess_extension(content_type)
            if extension:
                inferred_name = f"{inferred_name}{extension}"
        file_bytes = response.content
        converted = self._convert_to_webp_if_possible(file_bytes, inferred_name, content_type)
        if converted is not None:
            file_bytes, inferred_name, content_type = converted
        files = {
            "file": (inferred_name, file_bytes, content_type)
        }
        return self.json_request("POST", "/media", files=files)

    def get_media(self, media_id: int, **params: Any) -> Dict[str, Any]:
        return self.json_request("GET", f"/media/{media_id}", params=params or None)

    def update_media(self, media_id: int, **fields: Any) -> Dict[str, Any]:
        return self.json_request("POST", f"/media/{media_id}", json=fields)

    def list_categories(self, **params: Any) -> list[Dict[str, Any]]:
        return self.json_request("GET", "/categories", params=params)

    def create_category(self, name: str, slug: Optional[str] = None, parent: Optional[int] = None) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"name": name}
        if slug:
            payload["slug"] = slug
        if parent is not None:
            payload["parent"] = parent
        return self.json_request("POST", "/categories", json=payload)

    def list_tags(self, **params: Any) -> list[Dict[str, Any]]:
        return self.json_request("GET", "/tags", params=params)

    def create_tag(self, name: str, slug: Optional[str] = None) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"name": name}
        if slug:
            payload["slug"] = slug
        return self.json_request("POST", "/tags", json=payload)

    def _convert_to_webp_if_possible(
        self,
        file_bytes: bytes,
        filename: str,
        content_type: str,
    ) -> tuple[bytes, str, str] | None:
        if Image is None:
            return None
        lower_name = filename.lower()
        if "webp" in content_type or lower_name.endswith(".webp"):
            return None
        if not content_type.startswith("image/") and not lower_name.endswith((".jpg", ".jpeg", ".png")):
            return None

        try:
            with Image.open(BytesIO(file_bytes)) as image:
                converted = image.convert("RGB")
                buffer = BytesIO()
                converted.save(buffer, format="WEBP", quality=85, method=6)
                stem = Path(filename).stem or "media"
                return buffer.getvalue(), f"{stem}.webp", "image/webp"
        except Exception:  # pragma: no cover - best-effort media optimization
            return None
