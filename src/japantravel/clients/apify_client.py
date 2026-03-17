"""Apify client wrapper."""

from __future__ import annotations

from typing import Any, Dict, Optional

from ..config.settings import Settings
from .base import BaseClient


class ApifyClient(BaseClient):
    def __init__(
        self,
        token: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
        retry_attempts: Optional[int] = None,
    ):
        settings = Settings()
        api_token = token or settings.apify_token
        if not api_token:
            raise ValueError("APIFY_TOKEN is required.")

        super().__init__(
            base_url="https://api.apify.com/v2",
            timeout_seconds=timeout_seconds or settings.request_timeout_seconds,
            headers={"Authorization": f"Bearer {api_token}"},
            retry_attempts=retry_attempts or settings.http_retry_count,
            retry_min_wait=settings.http_retry_backoff,
            retry_max_wait=settings.http_retry_backoff * 4,
        )
        self.actor_id = settings.apify_actor_id

    def run_actor(self, actor_id: Optional[str], payload: Dict[str, Any], use_raw_payload: bool = False) -> Dict[str, Any]:
        target_actor_id = actor_id or self.actor_id
        if not target_actor_id:
            raise ValueError("actor_id is required for run_actor.")
        target_actor_id = self._normalize_actor_id(target_actor_id)
        body = payload if use_raw_payload else {"input": payload}
        return self.json_request(
            "POST",
            f"/acts/{target_actor_id}/runs",
            json=body,
        )

    @staticmethod
    def _normalize_actor_id(actor_id: str) -> str:
        return actor_id.replace("/", "~")

    def get_actor_run(self, run_id: str) -> Dict[str, Any]:
        return self.json_request("GET", f"/actor-runs/{run_id}")

    def list_actor_runs(self, actor_id: Optional[str] = None, limit: int = 20) -> Dict[str, Any]:
        target_actor_id = actor_id or self.actor_id
        if target_actor_id:
            target_actor_id = self._normalize_actor_id(target_actor_id)
        params = {"limit": limit}
        if target_actor_id:
            return self.json_request("GET", f"/acts/{target_actor_id}/runs", params=params)
        return self.json_request("GET", "/actor-runs", params=params)

    def get_run_items(self, run_id: str) -> Dict[str, Any]:
        # Apify run dataset results endpoint
        return self.json_request("GET", f"/actor-runs/{run_id}/dataset/items")

    def get_run_dataset(self, run_id: str) -> Dict[str, Any]:
        return self.json_request("GET", f"/actor-runs/{run_id}/dataset")

    def get_dataset_items(
        self,
        dataset_id: str,
        limit: Optional[int] = 1000,
        offset: int = 0,
        clean: bool = True,
    ) -> Dict[str, Any]:
        """Read items from an existing Apify dataset."""
        if not dataset_id:
            raise ValueError("dataset_id is required for get_dataset_items.")
        params: Dict[str, Any] = {
            "clean": str(clean).lower(),
            "offset": offset,
            "limit": limit,
        }
        return self.json_request("GET", f"/datasets/{dataset_id}/items", params=params)
