"""Common HTTP client base for external services."""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, Optional

import requests

from ..shared.exceptions import ExternalServiceError


class BaseClient:
    def __init__(
        self,
        base_url: str,
        timeout_seconds: int = 20,
        headers: Optional[Dict[str, str]] = None,
        retry_attempts: int = 3,
        retry_min_wait: float = 1.0,
        retry_max_wait: float = 5.0,
    ):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.headers = headers or {}
        self.retry_attempts = retry_attempts
        self.retry_min_wait = retry_min_wait
        self.retry_max_wait = retry_max_wait
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        url = f"{self.base_url}/{path.lstrip('/')}"
        kwargs["timeout"] = kwargs.pop("timeout", self.timeout_seconds)

        for attempt in range(1, self.retry_attempts + 1):
            self.logger.debug(
                "HTTP request start: method=%s url=%s attempt=%s",
                method.upper(),
                url,
                attempt,
            )
            try:
                response = self.session.request(method=method, url=url, **kwargs)
                response.raise_for_status()
                self.logger.info(
                    "HTTP request success: method=%s url=%s status=%s",
                    method.upper(),
                    url,
                    response.status_code,
                )
                return response
            except requests.RequestException as exc:
                self.logger.warning(
                    "HTTP request failed: method=%s url=%s attempt=%s error=%s",
                    method.upper(),
                    url,
                    attempt,
                    exc,
                )
                if attempt >= self.retry_attempts:
                    raise ExternalServiceError(f"Request failed: {exc}") from exc
                time.sleep(min(self.retry_max_wait, self.retry_min_wait * (2 ** (attempt - 1))))

    def json_request(self, method: str, path: str, **kwargs: Any) -> Dict[str, Any]:
        response = self.request(method=method, path=path, **kwargs)
        try:
            return response.json()
        except ValueError as exc:
            raise ExternalServiceError("Invalid JSON response") from exc
