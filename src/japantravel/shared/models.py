"""Shared pydantic models."""

from __future__ import annotations

from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class GenerationInput(BaseModel):
    region: str
    context: Optional[Dict[str, Any]] = None


class PlaceCandidate(BaseModel):
    source: str = "google_map_scraper"
    source_id: str = ""
    place_id: str = ""
    name: str = ""
    city: str = ""
    country: str = ""
    category: str = ""
    subcategories: list[str] = Field(default_factory=list)
    address: str = ""
    lat: float = 0.0
    lng: float = 0.0
    rating: float = 0.0
    review_count: int = 0
    price_level: str = ""
    opening_hours: str = ""
    business_status: str = ""
    website: str = ""
    phone: str = ""
    maps_url: str = ""
    image_urls: list[str] = Field(default_factory=list)
    review_snippets: list[str] = Field(default_factory=list)
    last_verified_at: str = ""
    collected_at: str = ""
    raw_payload: dict = Field(default_factory=dict)


class ArticleCandidate(BaseModel):
    topic_key: str = ""
    city: str = ""
    country: str = ""
    scenario: str = "solo_travel"
    place_type: str = ""
    audience: str = "korean_traveler"
    query_text: str = ""
    candidate_place_ids: list[str] = Field(default_factory=list)
    ranking_version: str = "v1"
    status: str = "draft"


class PublishedArticle(BaseModel):
    wp_post_id: int = 0
    slug: str = ""
    title: str = ""
    topic_key: str = ""
    candidate_place_ids: list[str] = Field(default_factory=list)
    place_snapshots: list[Dict[str, Any]] = Field(default_factory=list)
    business_status: str = ""
    published_at: str = ""
    needs_refresh: bool = False
    last_content_reviewed_at: str = ""
    last_data_verified_at: str = ""
