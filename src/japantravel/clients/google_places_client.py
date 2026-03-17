"""Google Places client wrapper."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from ..config.settings import Settings
from .base import BaseClient


class GooglePlacesClient(BaseClient):
    def __init__(
        self,
        api_key: Optional[str] = None,
        language: Optional[str] = None,
        region: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
        retry_attempts: Optional[int] = None,
    ):
        settings = Settings()
        key = api_key or settings.google_places_api_key
        if not key:
            raise ValueError("GOOGLE_PLACES_API_KEY is required.")

        super().__init__(
            base_url="https://maps.googleapis.com/maps/api/place",
            timeout_seconds=timeout_seconds or settings.request_timeout_seconds,
            headers={"Accept": "application/json"},
            retry_attempts=retry_attempts or settings.http_retry_count,
            retry_min_wait=settings.http_retry_backoff,
            retry_max_wait=settings.http_retry_backoff * 4,
        )
        self.api_key = key
        self.language = language or settings.google_places_language
        self.region = region or settings.google_places_region

    def text_search(self, query: str, **params: Any) -> List[Dict[str, Any]]:
        response = self.json_request(
            "GET",
            "/textsearch/json",
            params={
                "query": query,
                "language": self.language,
                "region": self.region,
                "key": self.api_key,
                **params,
            },
        )
        return response.get("results", [])

    def nearby_search(
        self,
        latitude: float,
        longitude: float,
        radius: int = 3000,
        keyword: Optional[str] = None,
        place_type: Optional[str] = None,
        **params: Any,
    ) -> List[Dict[str, Any]]:
        request_params: Dict[str, Any] = {
            "location": f"{latitude},{longitude}",
            "radius": radius,
            "language": self.language,
            "region": self.region,
            "key": self.api_key,
            **params,
        }
        if keyword:
            request_params["keyword"] = keyword
        if place_type:
            request_params["type"] = place_type

        response = self.json_request("GET", "/nearbysearch/json", params=request_params)
        return response.get("results", [])

    def place_details(self, place_id: str, fields: Optional[str] = None) -> Dict[str, Any]:
        response = self.json_request(
            "GET",
            "/details/json",
            params={
                "place_id": place_id,
                "fields": fields,
                "language": self.language,
                "key": self.api_key,
            },
        )
        return response.get("result", {})
