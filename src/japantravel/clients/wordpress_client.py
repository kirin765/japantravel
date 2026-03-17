"""WordPress REST API wrapper."""

from __future__ import annotations

import mimetypes
import os
from pathlib import Path
from urllib.parse import urlparse
from typing import Any, Dict, Optional

from requests.auth import HTTPBasicAuth

from ..config.settings import Settings
from .base import BaseClient


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

    def update_post(self, post_id: int, **fields: Any) -> Dict[str, Any]:
        return self.json_request("POST", f"/posts/{post_id}", json=fields)

    def get_post(self, post_id: int) -> Dict[str, Any]:
        return self.json_request("GET", f"/posts/{post_id}")

    def list_posts(self, **params: Any) -> Dict[str, Any]:
        return self.json_request("GET", "/posts", params=params)

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
        if "." not in inferred_name:
            extension = mimetypes.guess_extension(response.headers.get("content-type", "application/octet-stream").split(";")[0].strip())
            if extension:
                inferred_name = f"{inferred_name}{extension}"
        files = {
            "file": (inferred_name, response.content, response.headers.get("content-type", "application/octet-stream"))
        }
        return self.json_request("POST", "/media", files=files)

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
