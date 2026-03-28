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

    google_places_api_key: Optional[str] = os.getenv("GOOGLE_PLACES_API_KEY")
    google_places_language: str = os.getenv("GOOGLE_PLACES_LANGUAGE", "en")
    google_places_region: str = os.getenv("GOOGLE_PLACES_REGION", "JP")
    google_map_scraper_path: str = os.getenv("GOOGLE_MAP_SCRAPER_PATH", "../google-map-scraper")
    google_map_scraper_timeout: int = int(os.getenv("GOOGLE_MAP_SCRAPER_TIMEOUT", os.getenv("REQUEST_TIMEOUT_SECONDS", "20")))
    google_map_scraper_todo_dir: str = os.getenv("GOOGLE_MAP_SCRAPER_TODO_DIR", "../google-map-scraper")
    place_collect_search_strings: str = os.getenv("PLACE_COLLECT_SEARCH_STRINGS", "")
    place_collect_location_query: str = os.getenv("PLACE_COLLECT_LOCATION_QUERY", "")
    place_collect_location_queries: str = os.getenv("PLACE_COLLECT_LOCATION_QUERIES", "")
    place_collect_language: str = os.getenv("PLACE_COLLECT_LANGUAGE", os.getenv("GOOGLE_PLACES_LANGUAGE", "en"))
    place_collect_max_results_per_search: int = int(
        os.getenv(
            "PLACE_COLLECT_MAX_RESULTS_PER_SEARCH",
            "120",
        )
    )

    openai_api_key: Optional[str] = os.getenv("OPENAI_API_KEY")
    openai_model: str = os.getenv("OPENAI_MODEL", "gpt-4.1")
    openai_temperature: float = float(os.getenv("OPENAI_TEMPERATURE", "0.2"))
    openai_max_tokens: int = int(os.getenv("OPENAI_MAX_TOKENS", "1200"))

    wordpress_base_url: Optional[str] = os.getenv("WORDPRESS_BASE_URL")
    wordpress_username: Optional[str] = os.getenv("WORDPRESS_USERNAME")
    wordpress_app_password: Optional[str] = os.getenv("WORDPRESS_APP_PASSWORD")
    wordpress_rest_api_version: str = os.getenv("WORDPRESS_REST_API_VERSION", "wp/v2")
    wordpress_meta_title_key: Optional[str] = os.getenv("WORDPRESS_META_TITLE_KEY")
    wordpress_meta_description_key: Optional[str] = os.getenv("WORDPRESS_META_DESCRIPTION_KEY")
    wordpress_meta_keywords_key: Optional[str] = os.getenv("WORDPRESS_META_KEYWORDS_KEY")
    wordpress_meta_canonical_key: Optional[str] = os.getenv("WORDPRESS_META_CANONICAL_KEY")
    seo_archive_noindex_taxonomies: bool = os.getenv("SEO_ARCHIVE_NOINDEX_TAXONOMIES", "true").lower() == "true"
    seo_single_post_category: str = os.getenv("SEO_SINGLE_POST_CATEGORY", "japan")
    seo_disable_feed_discovery: bool = os.getenv("SEO_DISABLE_FEED_DISCOVERY", "true").lower() == "true"
    seo_validate_remote_images: bool = os.getenv("SEO_VALIDATE_REMOTE_IMAGES", "true").lower() == "true"
    seo_block_unstable_image_hosts: str = os.getenv(
        "SEO_BLOCK_UNSTABLE_IMAGE_HOSTS",
        "streetviewpixels-pa.googleapis.com",
    )

    db_url: Optional[str] = os.getenv("DB_URL")
    place_cache_ttl_days: int = int(os.getenv("PLACE_CACHE_TTL_DAYS", "30"))
    place_cache_min_count: int = int(os.getenv("PLACE_CACHE_MIN_COUNT", "20"))
    place_cache_fetch_limit: int = int(os.getenv("PLACE_CACHE_FETCH_LIMIT", "120"))
    place_cache_strict_fields: bool = os.getenv("PLACE_CACHE_STRICT_FIELDS", "true").lower() == "true"

    top_n_candidates: int = int(os.getenv("TOP_N_CANDIDATES", "30"))
    rank_threshold: float = float(os.getenv("RANK_THRESHOLD", "0.65"))
    recent_published_exclude_count: int = int(os.getenv("RECENT_PUBLISHED_EXCLUDE_COUNT", "10"))
    recent_title_token_threshold: float = float(os.getenv("RECENT_TITLE_TOKEN_THRESHOLD", "0.6"))
    recent_place_target_count: int = int(os.getenv("RECENT_PLACE_TARGET_COUNT", "6"))
    recent_place_min_count: int = int(os.getenv("RECENT_PLACE_MIN_COUNT", "2"))
    enable_review_check: bool = os.getenv("ENABLE_REVIEW_CHECK", "true").lower() == "true"
    generation_retry_count: int = int(os.getenv("GENERATION_RETRY_COUNT", "2"))
    scheduler_timezone: str = os.getenv("SCHEDULER_TIMEZONE", "Asia/Seoul")
    scheduler_store_url: Optional[str] = os.getenv("SCHEDULER_STORE_URL")
    scheduler_max_instances: int = int(os.getenv("SCHEDULER_MAX_INSTANCES", "1"))
    scheduler_content_interval_hours: int = int(os.getenv("SCHEDULER_CONTENT_INTERVAL_HOURS", "8"))
