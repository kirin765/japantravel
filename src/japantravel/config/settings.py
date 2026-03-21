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
    apify_dataset_ids: str = os.getenv("APIFY_DATASET_IDS", "")
    apify_dataset_item_limit: int = int(os.getenv("APIFY_DATASET_ITEM_LIMIT", "2000"))
    discovery_sync_max_results: int = int(os.getenv("DISCOVERY_SYNC_MAX_RESULTS", "120"))
    apify_max_concurrent_requests: int = int(os.getenv("APIFY_MAX_CONCURRENT_REQUESTS", "3"))
    apify_search_strings: str = os.getenv("APIFY_SEARCH_STRINGS", "")
    apify_location_query: str = os.getenv("APIFY_LOCATION_QUERY", "")
    apify_language: str = os.getenv("APIFY_LANGUAGE", os.getenv("GOOGLE_PLACES_LANGUAGE", "en"))
    apify_max_crawled_per_search: int = int(os.getenv("APIFY_MAX_CRAWLED_PER_SEARCH", os.getenv("DISCOVERY_SYNC_MAX_RESULTS", "120")))
    trend_source: str = os.getenv("TREND_SOURCE", "google_then_curated")
    trend_core_regions: str = os.getenv(
        "TREND_CORE_REGIONS",
        "Tokyo,Japan|Osaka,Japan|Kyoto,Japan|Fukuoka,Japan|Okinawa,Japan|Hokkaido,Japan",
    )
    trend_seed_keywords: str = os.getenv(
        "TREND_SEED_KEYWORDS",
        "맛집,카페,온천,야경,전망대,벚꽃,단풍,쇼핑,시장,박물관,미술관,가족여행,혼자여행,비오는날,료칸,라멘",
    )
    trend_region_query_limit: int = int(os.getenv("TREND_REGION_QUERY_LIMIT", "6"))
    trend_google_timeframe: str = os.getenv("TREND_GOOGLE_TIMEFRAME", "now 7-d")
    trend_max_crawled_per_search: int = int(os.getenv("TREND_MAX_CRAWLED_PER_SEARCH", "30"))

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
    recent_published_exclude_count: int = int(os.getenv("RECENT_PUBLISHED_EXCLUDE_COUNT", "5"))
    recent_title_token_threshold: float = float(os.getenv("RECENT_TITLE_TOKEN_THRESHOLD", "0.6"))
    recent_place_target_count: int = int(os.getenv("RECENT_PLACE_TARGET_COUNT", "6"))
    recent_place_min_count: int = int(os.getenv("RECENT_PLACE_MIN_COUNT", "2"))
    recent_place_force_apify_on_exhaust: bool = os.getenv("RECENT_PLACE_FORCE_APIFY_ON_EXHAUST", "true").lower() == "true"
    enable_review_check: bool = os.getenv("ENABLE_REVIEW_CHECK", "true").lower() == "true"
    generation_retry_count: int = int(os.getenv("GENERATION_RETRY_COUNT", "2"))
    scheduler_timezone: str = os.getenv("SCHEDULER_TIMEZONE", "Asia/Seoul")
    scheduler_store_url: Optional[str] = os.getenv("SCHEDULER_STORE_URL")
    scheduler_max_instances: int = int(os.getenv("SCHEDULER_MAX_INSTANCES", "1"))
    scheduler_enable_apify_collect: bool = os.getenv("SCHEDULER_ENABLE_APIFY_COLLECT", "false").lower() == "true"
    scheduler_enable_trend_collect: bool = os.getenv("SCHEDULER_ENABLE_TREND_COLLECT", "true").lower() == "true"
    scheduler_trend_collect_interval_hours: int = int(os.getenv("SCHEDULER_TREND_COLLECT_INTERVAL_HOURS", "48"))
    scheduler_content_interval_hours: int = int(os.getenv("SCHEDULER_CONTENT_INTERVAL_HOURS", "8"))
