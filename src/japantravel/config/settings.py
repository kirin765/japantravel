"""Load environment-based settings."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    environment: str = os.getenv("ENVIRONMENT", "development")
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    timezone: str = os.getenv("TIMEZONE", "Asia/Seoul")
    request_timeout_seconds: int = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "20"))
    http_retry_count: int = int(os.getenv("HTTP_RETRY_COUNT", "3"))
    http_retry_backoff: float = float(os.getenv("HTTP_RETRY_BACKOFF", "1.5"))

    apify_token: Optional[str] = os.getenv("APIFY_TOKEN")
    apify_actor_id: Optional[str] = os.getenv("APIFY_ACTOR_ID")
    apify_actor_timeout: int = int(os.getenv("APIFY_ACTOR_TIMEOUT", "900"))
    apify_discovery_actor_id: Optional[str] = os.getenv("APIFY_DISCOVERY_ACTOR_ID")
    apify_discovery_timeout: int = int(os.getenv("APIFY_DISCOVERY_TIMEOUT", "900"))
    apify_dataset_id: Optional[str] = os.getenv("APIFY_DATASET_ID")
    apify_dataset_item_limit: int = int(os.getenv("APIFY_DATASET_ITEM_LIMIT", "2000"))
    discovery_sync_max_results: int = int(os.getenv("DISCOVERY_SYNC_MAX_RESULTS", "120"))
    apify_max_concurrent_requests: int = int(os.getenv("APIFY_MAX_CONCURRENT_REQUESTS", "3"))
    apify_search_strings: str = os.getenv("APIFY_SEARCH_STRINGS", "")
    apify_location_query: str = os.getenv("APIFY_LOCATION_QUERY", "")
    apify_language: str = os.getenv("APIFY_LANGUAGE", os.getenv("GOOGLE_PLACES_LANGUAGE", "en"))
    apify_max_crawled_per_search: int = int(os.getenv("APIFY_MAX_CRAWLED_PER_SEARCH", os.getenv("DISCOVERY_SYNC_MAX_RESULTS", "120")))

    google_places_api_key: Optional[str] = os.getenv("GOOGLE_PLACES_API_KEY")
    google_places_language: str = os.getenv("GOOGLE_PLACES_LANGUAGE", "en")
    google_places_region: str = os.getenv("GOOGLE_PLACES_REGION", "JP")

    openai_api_key: Optional[str] = os.getenv("OPENAI_API_KEY")
    openai_model: str = os.getenv("OPENAI_MODEL", "gpt-4.1")
    openai_temperature: float = float(os.getenv("OPENAI_TEMPERATURE", "0.2"))
    openai_max_tokens: int = int(os.getenv("OPENAI_MAX_TOKENS", "1200"))

    wordpress_base_url: Optional[str] = os.getenv("WORDPRESS_BASE_URL")
    wordpress_username: Optional[str] = os.getenv("WORDPRESS_USERNAME")
    wordpress_app_password: Optional[str] = os.getenv("WORDPRESS_APP_PASSWORD")
    wordpress_rest_api_version: str = os.getenv("WORDPRESS_REST_API_VERSION", "wp/v2")

    db_url: Optional[str] = os.getenv("DB_URL")
    place_cache_ttl_days: int = int(os.getenv("PLACE_CACHE_TTL_DAYS", "30"))
    place_cache_min_count: int = int(os.getenv("PLACE_CACHE_MIN_COUNT", "20"))
    place_cache_fetch_limit: int = int(os.getenv("PLACE_CACHE_FETCH_LIMIT", "120"))
    place_cache_strict_fields: bool = os.getenv("PLACE_CACHE_STRICT_FIELDS", "true").lower() == "true"
    apify_force_refresh: bool = os.getenv("APIFY_FORCE_REFRESH", "false").lower() == "true"
    apify_min_new_run_interval_minutes: int = int(os.getenv("APIFY_MIN_NEW_RUN_INTERVAL_MINUTES", "240"))

    top_n_candidates: int = int(os.getenv("TOP_N_CANDIDATES", "30"))
    rank_threshold: float = float(os.getenv("RANK_THRESHOLD", "0.65"))
    enable_review_check: bool = os.getenv("ENABLE_REVIEW_CHECK", "true").lower() == "true"
    generation_retry_count: int = int(os.getenv("GENERATION_RETRY_COUNT", "2"))
    scheduler_timezone: str = os.getenv("SCHEDULER_TIMEZONE", "Asia/Seoul")
    scheduler_store_url: Optional[str] = os.getenv("SCHEDULER_STORE_URL")
    scheduler_max_instances: int = int(os.getenv("SCHEDULER_MAX_INSTANCES", "1"))
