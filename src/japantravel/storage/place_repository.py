"""Persistent place cache repository backed by PostgreSQL."""

from __future__ import annotations

import json
import hashlib
import logging
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping, Optional
from decimal import Decimal

try:
    from psycopg import connect
    from psycopg.rows import dict_row
    from psycopg.types.json import Json
except ModuleNotFoundError:
    connect = None  # type: ignore[assignment]
    dict_row = None  # type: ignore[assignment]
    Json = None  # type: ignore[assignment]


@dataclass
class PlaceRepositoryResult:
    fetched_count: int
    inserted_count: int
    reused_count: int
    errors: list[str]
    skipped_count: int = 0


class PlaceRepository:
    """Read/write wrapper for the `place` table."""

    def __init__(self, db_url: str):
        if connect is None:
            raise ImportError("psycopg is required for DB persistence.")
        self.db_url = db_url
        self.logger = logging.getLogger(self.__class__.__name__)
        self.enabled = bool(db_url)
        self._place_columns_cache: Optional[set[str]] = None
        self._place_source_values_cache: Optional[set[str]] = None

    def fetch_reusable_candidates(
        self,
        city: str = "",
        country: str = "",
        limit: int = 120,
        stale_days: int = 30,
        strict_fields: bool = True,
    ) -> list[dict[str, Any]]:
        if not self.enabled:
            return []

        stale_days = max(stale_days, 1)
        filters = [
            "p.is_active IS TRUE",
            "COALESCE(p.updated_at, p.created_at) >= NOW() AT TIME ZONE 'UTC' - make_interval(days => %s)",
            "COALESCE(TRIM(p.name), '') <> ''",
        ]

        args: list[Any] = [stale_days]

        if city:
            filters.append("LOWER(COALESCE(p.region, '')) = LOWER(%s)")
            args.append(city.strip())
        if country:
            filters.append("LOWER(COALESCE(p.country, '')) = LOWER(%s)")
            args.append(country.strip())
        args.append(limit)

        if strict_fields:
            filters.append("p.external_place_id IS NOT NULL")
            filters.append("p.rating IS NOT NULL")

        sql = f"""
            SELECT
                p.id,
                p.source,
                p.external_place_id,
                p.google_place_id,
                p.name,
                p.description,
                p.address,
                p.region,
                p.country,
                p.latitude,
                p.longitude,
                p.category,
                p.rating,
                p.review_count,
                p.price_level,
                p.is_open,
                p.raw_payload,
                p.updated_at,
                p.created_at
            FROM place p
            WHERE {" AND ".join(filters)}
            ORDER BY p.updated_at DESC, p.review_count DESC NULLS LAST, p.rating DESC NULLS LAST
            LIMIT %s
        """

        result: list[dict[str, Any]] = []
        with connect(self.db_url) as connection:
            connection.row_factory = dict_row
            with connection.cursor() as cursor:
                cursor.execute(sql, tuple(args))
                rows = cursor.fetchall()

        for row in rows:
            payload = self._row_to_payload(row)
            if payload:
                result.append(payload)

        return result

    def reset_all_data(self) -> None:
        if not self.enabled:
            return

        with connect(self.db_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    TRUNCATE TABLE
                        error_logs,
                        publish_logs,
                        published_article,
                        article_candidate,
                        place
                    RESTART IDENTITY CASCADE
                    """
                )
            connection.commit()

    def upsert_places(
        self,
        raw_places: list[Mapping[str, Any]],
        source: str = "google_map_scraper",
        conflict_mode: str = "update",
    ) -> PlaceRepositoryResult:
        if not self.enabled or not raw_places:
            return PlaceRepositoryResult(fetched_count=0, inserted_count=0, reused_count=0, errors=[])
        if conflict_mode not in {"update", "skip"}:
            raise ValueError("conflict_mode must be either 'update' or 'skip'.")

        inserted_count = 0
        skipped_count = 0
        errors: list[str] = []
        with connect(self.db_url) as connection:
            connection.row_factory = dict_row
            place_columns = self._place_columns(connection)
            sql = self._place_upsert_sql(conflict_mode, place_columns)
            resolved_source = self._resolve_supported_source(connection, source)
            with connection.cursor() as cursor:
                for raw in raw_places:
                    normalized = self._normalize_raw(
                        raw,
                        source=resolved_source,
                    )
                    if not normalized:
                        continue
                    params = normalized["params"]
                    try:
                        cursor.execute(sql, params)
                        row = cursor.fetchone()
                        if row:
                            inserted_count += 1
                        else:
                            skipped_count += 1
                    except Exception as exc:  # pragma: no cover - defensive
                        connection.rollback()
                        errors.append(f"{normalized.get('external_place_id', 'unknown')}: {exc}")
                        self.logger.warning("upsert place failed: %s", exc)
                        continue
            connection.commit()

        return PlaceRepositoryResult(
            fetched_count=len(raw_places),
            inserted_count=inserted_count,
            reused_count=skipped_count,
            errors=errors,
            skipped_count=skipped_count,
        )

    def count_active_candidates(self, city: str = "", country: str = "", stale_days: int = 30) -> int:
        if not self.enabled:
            return 0

        stale_days = max(stale_days, 1)
        filters = [
            "is_active IS TRUE",
            "COALESCE(updated_at, created_at) >= NOW() AT TIME ZONE 'UTC' - make_interval(days => %s)",
        ]
        params: list[Any] = [stale_days]
        if city:
            filters.append("LOWER(COALESCE(region, '')) = LOWER(%s)")
            params.append(city.strip())
        if country:
            filters.append("LOWER(COALESCE(country, '')) = LOWER(%s)")
            params.append(country.strip())

        with connect(self.db_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT COUNT(*) FROM place WHERE {' AND '.join(filters)}",
                    tuple(params),
                )
                total = cursor.fetchone()

        return int(total[0]) if total else 0

    def has_recent_collection(self, interval_minutes: int = 240, source: str = "google_map_scraper") -> bool:
        if interval_minutes <= 0:
            return False
        threshold = datetime.now(timezone.utc) - timedelta(minutes=interval_minutes)
        with connect(self.db_url) as connection:
            resolved_source = self._resolve_supported_source(connection, source)
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM place
                        WHERE source = %s
                          AND updated_at >= %s
                    )
                    """,
                    (resolved_source, threshold),
                )
                row = cursor.fetchone()

        return bool(row and row[0])

    def fetch_recent_published_place_keys(self, limit: int = 10, status: str = "published") -> set[str]:
        if not self.enabled or limit <= 0:
            return set()

        sql = """
            SELECT
                pa.place_id AS db_place_id,
                p.external_place_id,
                p.google_place_id,
                pa.raw_publish_response
            FROM published_article pa
            JOIN place p ON p.id = pa.place_id
            WHERE pa.status = %s
            ORDER BY COALESCE(pa.published_at, pa.created_at) DESC
            LIMIT %s
        """

        with connect(self.db_url) as connection:
            connection.row_factory = dict_row
            with connection.cursor() as cursor:
                cursor.execute(sql, (status, limit))
                rows = cursor.fetchall()

        keys: set[str] = set()
        for row in rows:
            self._add_place_key(keys, row.get("db_place_id"))
            self._add_place_key(keys, row.get("external_place_id"))
            self._add_place_key(keys, row.get("google_place_id"))

            payload = row.get("raw_publish_response") or {}
            if isinstance(payload, str):
                with suppress(Exception):
                    payload = json.loads(payload)
            if not isinstance(payload, Mapping):
                continue

            for value in payload.get("candidate_place_ids", []):
                self._add_place_key(keys, value)
            for value in payload.get("candidate_db_place_ids", []):
                self._add_place_key(keys, value)
            for snapshot in payload.get("place_snapshots", []):
                self._add_place_keys_from_mapping(keys, snapshot)

        return keys

    def fetch_published_topic_metadata_by_post_ids(
        self,
        post_ids: Iterable[int],
        status: str = "published",
    ) -> dict[int, dict[str, Any]]:
        normalized_ids: list[int] = []
        for value in post_ids:
            with suppress(Exception):
                parsed = int(value)
                if parsed > 0:
                    normalized_ids.append(parsed)
        if not self.enabled or not normalized_ids:
            return {}

        sql = """
            SELECT
                wp_post_id,
                raw_publish_response
            FROM published_article
            WHERE status = %s
              AND wp_post_id = ANY(%s)
            ORDER BY COALESCE(published_at, created_at) DESC
        """

        with connect(self.db_url) as connection:
            connection.row_factory = dict_row
            with connection.cursor() as cursor:
                cursor.execute(sql, (status, normalized_ids))
                rows = cursor.fetchall()

        metadata_by_post_id: dict[int, dict[str, Any]] = {}
        for row in rows:
            post_id = row.get("wp_post_id")
            if not isinstance(post_id, int) or post_id <= 0:
                continue
            payload = row.get("raw_publish_response") or {}
            if isinstance(payload, str):
                with suppress(Exception):
                    payload = json.loads(payload)
            if not isinstance(payload, Mapping):
                continue
            metadata_by_post_id[post_id] = dict(payload)

        return metadata_by_post_id

    def fetch_place_summaries_by_keys(self, keys: Iterable[str]) -> list[dict[str, Any]]:
        normalized_keys = [str(value).strip() for value in keys if str(value).strip()]
        if not self.enabled or not normalized_keys:
            return []

        sql = """
            SELECT
                id,
                external_place_id,
                google_place_id,
                name,
                region,
                country,
                address
            FROM place
            WHERE external_place_id = ANY(%s)
               OR google_place_id = ANY(%s)
            ORDER BY updated_at DESC NULLS LAST, created_at DESC
        """

        with connect(self.db_url) as connection:
            connection.row_factory = dict_row
            with connection.cursor() as cursor:
                cursor.execute(sql, (normalized_keys, normalized_keys))
                rows = cursor.fetchall()

        return [dict(row) for row in rows]

    def update_published_article_status(
        self,
        *,
        wp_post_id: int,
        status: str,
        published_at: Optional[datetime] = None,
    ) -> bool:
        if not self.enabled or wp_post_id <= 0:
            return False

        with connect(self.db_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE published_article
                    SET
                        status = %s,
                        published_at = %s,
                        updated_at = NOW()
                    WHERE wp_post_id = %s
                    """,
                    (status, published_at, wp_post_id),
                )
                updated = cursor.rowcount > 0
            connection.commit()
        return updated

    def save_published_article(
        self,
        *,
        primary_place_id: int,
        wp_post_id: int,
        title: str,
        slug: str,
        content_html: str,
        excerpt: str = "",
        status: str = "draft",
        raw_publish_response: Optional[Mapping[str, Any]] = None,
        media_urls: Optional[list[str]] = None,
        categories: Optional[list[int]] = None,
        tags: Optional[list[str]] = None,
        published_at: Optional[datetime] = None,
        article_candidate_id: Optional[int] = None,
        created_by: str = "scheduler",
    ) -> bool:
        if not self.enabled:
            return False
        if primary_place_id <= 0 or wp_post_id <= 0:
            return False

        payload = dict(raw_publish_response or {})
        content_hash = hashlib.sha256(f"{slug}\n{content_html}".encode("utf-8")).hexdigest()
        payload_value = Json(payload) if Json is not None else json.dumps(payload, ensure_ascii=False)

        with connect(self.db_url) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO published_article (
                        place_id,
                        article_candidate_id,
                        wp_post_id,
                        title,
                        slug,
                        content_html,
                        excerpt,
                        status,
                        published_at,
                        media_urls,
                        categories,
                        tags,
                        content_hash,
                        raw_publish_response,
                        created_by
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (wp_post_id) DO UPDATE
                    SET
                        place_id = EXCLUDED.place_id,
                        article_candidate_id = EXCLUDED.article_candidate_id,
                        title = EXCLUDED.title,
                        slug = EXCLUDED.slug,
                        content_html = EXCLUDED.content_html,
                        excerpt = EXCLUDED.excerpt,
                        status = EXCLUDED.status,
                        published_at = EXCLUDED.published_at,
                        media_urls = EXCLUDED.media_urls,
                        categories = EXCLUDED.categories,
                        tags = EXCLUDED.tags,
                        content_hash = EXCLUDED.content_hash,
                        raw_publish_response = EXCLUDED.raw_publish_response,
                        updated_at = NOW()
                    """,
                    (
                        primary_place_id,
                        article_candidate_id,
                        wp_post_id,
                        title,
                        slug,
                        content_html,
                        excerpt,
                        status,
                        published_at,
                        media_urls or [],
                        categories or [],
                        tags or [],
                        content_hash,
                        payload_value,
                        created_by,
                    ),
                )
            connection.commit()
        return True

    def _row_to_payload(self, row: Mapping[str, Any]) -> dict[str, Any]:
        raw_payload = row.get("raw_payload") or {}
        if isinstance(raw_payload, str):
            with suppress(Exception):
                raw_payload = json.loads(raw_payload)

        rating = self._to_float(row.get("rating"), 0.0)
        review_count = self._to_int(row.get("review_count"), 0)

        category = row.get("category") or []
        if isinstance(category, tuple):
            category = list(category)
        elif category is None:
            category = []
        elif isinstance(category, str):
            category = [category]
        else:
            category = list(category) if isinstance(category, list) else [str(category)]

        address = self._to_str(row.get("address"))
        maps_url = self._extract_from_raw(raw_payload, "maps_url", "googleMapsUrl", "mapsUrl", "placeUrl", "url")
        image_urls = self._extract_image_urls(raw_payload)
        tags = self._to_list(self._extract_from_raw(raw_payload, "tags", "keywords"))

        return {
            "source_id": self._to_str(row.get("external_place_id")),
            "place_id": self._to_str(row.get("google_place_id") or row.get("external_place_id")),
            "id": row.get("id"),
            "name": self._to_str(row.get("name")),
            "city": self._to_str(row.get("region")),
            "country": self._to_str(row.get("country")),
            "category": category[0] if category else "general",
            "subcategories": category[1:] if len(category) > 1 else [],
            "address": address,
            "lat": self._to_float(row.get("latitude"), 0.0),
            "lng": self._to_float(row.get("longitude"), 0.0),
            "rating": rating,
            "review_count": review_count,
            "price_level": self._to_str(row.get("price_level")),
            "opening_hours": self._to_str(self._extract_from_raw(raw_payload, "opening_hours", "openingHours")),
            "business_status": self._business_status(row.get("is_open"), self._extract_from_raw(raw_payload, "business_status")),
            "website": self._to_str(self._extract_from_raw(raw_payload, "website")),
            "phone": self._to_str(self._extract_from_raw(raw_payload, "phone", "phoneNumber", "formatted_phone_number")),
            "maps_url": self._to_str(maps_url),
            "image_urls": image_urls,
            "review_snippets": self._to_list(self._extract_from_raw(raw_payload, "review_snippets", "reviews")),
            "last_verified_at": self._datetime_to_iso(raw_payload.get("last_verified_at") or row.get("updated_at") or row.get("created_at")),
            "collected_at": self._datetime_to_iso(row.get("created_at")),
            "raw_payload": dict(raw_payload) if isinstance(raw_payload, Mapping) else {},
            "source": self._to_str(row.get("source")) or "google_map_scraper",
            "tags": tags,
        }

    @staticmethod
    def _add_place_key(target: set[str], value: Any) -> None:
        if value is None:
            return
        normalized = str(value).strip()
        if normalized:
            target.add(normalized)

    def _add_place_keys_from_mapping(self, target: set[str], payload: Any) -> None:
        if not isinstance(payload, Mapping):
            return

        for field in ("id", "place_id", "source_id", "external_place_id", "google_place_id", "googlePlaceId", "placeId"):
            self._add_place_key(target, payload.get(field))

        raw_payload = payload.get("raw_payload")
        if isinstance(raw_payload, Mapping):
            for field in ("id", "place_id", "source_id", "external_place_id", "google_place_id", "googlePlaceId", "placeId"):
                self._add_place_key(target, raw_payload.get(field))

    def _normalize_raw(
        self,
        raw: Mapping[str, Any],
        source: str = "google_map_scraper",
    ) -> dict[str, Any] | None:
        source_id = self._resolve_place_id(raw)
        if not source_id:
            return None

        raw_payload = dict(raw)
        category = self._to_list(
            raw.get("category")
            or raw.get("categoryName")
            or raw.get("categories")
            or raw.get("types")
        )

        lat_raw = raw.get("lat")
        if lat_raw is None:
            lat_raw = raw.get("latitude")
        if lat_raw is None and isinstance(raw.get("location"), Mapping):
            lat_raw = raw.get("location", {}).get("lat") or raw.get("location", {}).get("latitude")
        if lat_raw is None and isinstance(raw.get("coordinates"), Mapping):
            lat_raw = raw.get("coordinates", {}).get("lat") or raw.get("coordinates", {}).get("latitude")
        lng_raw = raw.get("lng")
        if lng_raw is None:
            lng_raw = raw.get("longitude")
        if lng_raw is None and isinstance(raw.get("location"), Mapping):
            lng_raw = raw.get("location", {}).get("lng") or raw.get("location", {}).get("longitude")
        if lng_raw is None and isinstance(raw.get("coordinates"), Mapping):
            lng_raw = raw.get("coordinates", {}).get("lng") or raw.get("coordinates", {}).get("longitude")

        lat = self._to_float(lat_raw, default=0.0)
        lng = self._to_float(lng_raw, default=0.0)
        rating = self._to_float(raw.get("rating"), self._to_float(raw.get("googleScore"), 0.0))
        review_count = self._to_int(
            raw.get("review_count"),
            self._to_int(raw.get("reviewsCount"), self._to_int(raw.get("user_ratings_total"), 0)),
        )
        rating_count = self._to_int(raw.get("rating_count"), review_count)
        review_count_delta = self._to_int(raw.get("reviewCountDelta"), self._to_int(raw.get("reviewsCountDelta"), 0))

        price_level = self._to_int(raw.get("price_level"), self._to_int(raw_payload.get("priceLevel"), None))
        if price_level is not None:
            price_level = max(0, min(5, price_level))

        is_open = self._to_bool(raw.get("is_open"), raw.get("isOpen") if raw.get("isOpen") is not None else raw_payload.get("open_now"))
        full_address = self._to_str(raw.get("fullAddress") or raw.get("address") or raw.get("formatted_address") or "")
        short_address = self._to_str(raw.get("shortAddress") or raw.get("street") or raw.get("address") or full_address)
        business_status = self._to_str(raw.get("businessStatus") or raw.get("business_status")) or self._business_status(is_open, raw.get("businessStatus"))
        maps_url = self._to_str(
            raw.get("googleMapsUrl")
            or raw.get("google_maps_url")
            or raw.get("mapsUrl")
            or raw.get("googleMaps")
            or raw.get("mapUrl")
            or raw.get("url")
        )
        place_url = self._to_str(raw.get("placeUrl") or raw.get("place_url") or raw.get("url") or maps_url)
        open_hours = self._json_value(raw.get("openHours") or raw.get("openingHours") or raw.get("opening_hours"))
        weekday_hours = self._to_list(
            raw.get("weekdayHours")
            or raw.get("weekday_hours")
            or raw.get("weekday_text")
            or self._nested_value(raw.get("openingHours"), "weekday_text")
        )
        image_urls = self._extract_image_urls(raw_payload)
        collected_at = self._to_optional_str(
            raw.get("scrapedAt")
            or raw.get("collectedAt")
            or raw.get("searchDate")
            or raw.get("updatedAt")
        )

        params = {
            "source": source,
            "external_place_id": source_id,
            "google_place_id": self._to_str(
                raw.get("google_place_id")
                or raw.get("googlePlaceId")
                or raw.get("place_id")
                or raw.get("placeId")
                or source_id
            ),
            "name": self._to_str(raw.get("name") or raw.get("title") or "Unnamed place"),
            "description": self._to_str(
                raw.get("description")
                or raw.get("editorialSummary")
                or raw_payload.get("description", "")
            ),
            "address": full_address or short_address,
            "region": self._to_str(raw.get("region") or raw.get("city") or raw.get("locality") or raw.get("state") or ""),
            "country": self._to_str(raw.get("country") or raw.get("countryName") or raw.get("countryCode") or "JP"),
            "latitude": lat,
            "longitude": lng,
            "category": category,
            "rating": rating,
            "review_count": review_count,
            "price_level": price_level,
            "is_open": is_open,
            "place_url": place_url,
            "google_maps_url": maps_url,
            "name_local": self._to_str(raw.get("nameLocal") or raw.get("name_local")),
            "locality": self._to_str(raw.get("locality") or raw.get("city")),
            "state": self._to_str(raw.get("state") or raw.get("region")),
            "country_code": self._to_str(raw.get("countryCode")),
            "zip": self._to_str(raw.get("zip") or raw.get("postalCode") or raw.get("postal_code")),
            "phone_number": self._to_str(raw.get("phone") or raw.get("phoneNumber") or raw.get("formatted_phone_number")),
            "rating_count": rating_count,
            "review_count_delta": review_count_delta,
            "yelp_rating": self._to_float(raw.get("yelpRating"), self._to_float(raw.get("yelp_rating"), 0.0)),
            "google_score": self._to_float(raw.get("googleScore"), rating),
            "business_status": business_status,
            "open_hours": open_hours,
            "image_urls": image_urls,
            "website": self._to_str(raw.get("website") or raw.get("websiteUrl") or raw.get("website_url")),
            "short_address": short_address,
            "full_address": full_address,
            "weekday_hours": weekday_hours,
            "google_maps_place_id": self._to_str(raw.get("googleMapsPlaceId") or raw.get("google_maps_place_id")),
            "collected_at": collected_at,
            "raw_payload": json.dumps(raw_payload, ensure_ascii=False),
        }
        return {"params": params, "external_place_id": source_id}

    def _place_upsert_sql(self, conflict_mode: str, place_columns: set[str]) -> str:
        ordered_columns = [
            "source",
            "external_place_id",
            "google_place_id",
            "name",
            "description",
            "address",
            "region",
            "country",
            "latitude",
            "longitude",
            "category",
            "rating",
            "review_count",
            "price_level",
            "is_open",
            "place_url",
            "google_maps_url",
            "name_local",
            "locality",
            "state",
            "country_code",
            "zip",
            "phone_number",
            "rating_count",
            "review_count_delta",
            "yelp_rating",
            "google_score",
            "business_status",
            "open_hours",
            "image_urls",
            "website",
            "short_address",
            "full_address",
            "weekday_hours",
            "google_maps_place_id",
            "raw_payload",
            "is_active",
            "updated_at",
        ]
        insert_columns = [column for column in ordered_columns if column in place_columns]
        insert_values = [
            "TRUE" if column == "is_active" else "NOW()" if column == "updated_at" else f"%({column})s"
            for column in insert_columns
        ]

        conflict_clause = """
            ON CONFLICT (source, external_place_id)
            DO NOTHING
            RETURNING id
        """
        if conflict_mode == "update":
            update_assignments: list[str] = []
            for column in insert_columns:
                if column in {"source", "external_place_id"}:
                    continue
                if column == "updated_at":
                    update_assignments.append("updated_at = NOW()")
                elif column == "is_active":
                    update_assignments.append("is_active = TRUE")
                elif column == "google_place_id":
                    update_assignments.append(f"{column} = COALESCE(EXCLUDED.{column}, place.{column})")
                else:
                    update_assignments.append(f"{column} = EXCLUDED.{column}")
            conflict_clause = f"""
                ON CONFLICT (source, external_place_id)
                DO UPDATE SET
                    {", ".join(update_assignments)}
                RETURNING id
            """

        return f"""
            INSERT INTO place (
                {", ".join(insert_columns)}
            ) VALUES (
                {", ".join(insert_values)}
            )
            {conflict_clause}
        """

    def _place_columns(self, connection) -> set[str]:
        if self._place_columns_cache is not None:
            return self._place_columns_cache
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'place'
                """
            )
            self._place_columns_cache = {
                value
                for value in (self._scalar_row_value(row) for row in cursor.fetchall())
                if value
            }
        return self._place_columns_cache

    def _supported_place_sources(self, connection) -> set[str]:
        if self._place_source_values_cache is not None:
            return self._place_source_values_cache
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT e.enumlabel
                FROM pg_type t
                JOIN pg_enum e ON t.oid = e.enumtypid
                WHERE t.typname = 'place_source'
                """
            )
            values = {
                value
                for value in (self._scalar_row_value(row) for row in cursor.fetchall())
                if value
            }
        self._place_source_values_cache = values
        return values

    def _resolve_supported_source(self, connection, source: str) -> str:
        normalized = self._to_str(source) or "manual"
        supported = self._supported_place_sources(connection)
        if not supported or normalized in supported:
            return normalized
        if normalized == "google_map_scraper" and "manual" in supported:
            return "manual"
        if "manual" in supported:
            return "manual"
        return normalized

    def _resolve_place_id(self, raw: Mapping[str, Any]) -> str:
        return (
            self._to_str(raw.get("place_id"))
            or self._to_str(raw.get("placeId"))
            or self._to_str(raw.get("google_place_id"))
            or self._to_str(raw.get("googlePlaceId"))
            or self._to_str(raw.get("googleMapsPlaceId"))
            or self._to_str(raw.get("google_maps_place_id"))
            or self._to_str(raw.get("id"))
        )

    def _scalar_row_value(self, row: Any) -> str:
        if isinstance(row, Mapping):
            for value in row.values():
                text = self._to_str(value)
                if text:
                    return text
            return ""
        if isinstance(row, (tuple, list)) and row:
            return self._to_str(row[0])
        return self._to_str(row)

    def _json_value(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, (Mapping, list, tuple)):
            return json.dumps(value, ensure_ascii=False)
        if isinstance(value, str) and value.strip():
            return json.dumps(value.strip(), ensure_ascii=False)
        return None

    def _nested_value(self, value: Any, key: str) -> Any:
        if isinstance(value, Mapping):
            return value.get(key)
        return None

    def _to_optional_str(self, value: Any) -> Optional[str]:
        text = self._to_str(value)
        return text or None

    def _to_float(self, value: Any, default: float = 0.0) -> float:
        if value is None:
            return default
        if isinstance(value, Decimal):
            return float(value)
        with suppress(Exception):
            return float(value)
        return default

    def _to_int(self, value: Any, default: int = 0) -> int:
        if value is None:
            return default
        with suppress(Exception):
            return int(value)
        return default

    def _to_bool(self, value: Any, fallback: Any = None) -> bool | None:
        if value is None and fallback is None:
            return None
        if value is None:
            return bool(fallback)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "open", "opened", "yes", "y", "on"}
        with suppress(Exception):
            return bool(int(value))
        return bool(value)

    def _to_str(self, value: Any, default: str = "") -> str:
        if value is None:
            return default
        if isinstance(value, str):
            return value.strip()
        return str(value)

    def _to_list(self, value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, list):
            return [self._to_str(item) for item in value if self._to_str(item)]
        if isinstance(value, tuple):
            return [self._to_str(item) for item in value if self._to_str(item)]
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return []
            if "," in stripped:
                return [item.strip() for item in stripped.split(",") if item.strip()]
            return [stripped]
        return []

    def _extract_image_urls(self, raw: Any) -> list[str]:
        if not isinstance(raw, Mapping):
            return []

        candidates: list[str] = []
        primary_keys = (
            "image_urls",
            "images",
            "photos",
            "photo",
            "photoUrl",
            "photo_urls",
            "imageUrl",
            "image",
            "media",
            "mediaUrl",
            "media_urls",
            "mediaUrlList",
            "heroImage",
            "thumbnail",
            "thumbnails",
            "placePhoto",
            "placePhotos",
        )

        for key in primary_keys:
            self._collect_image_candidates(raw.get(key), candidates)

        self._collect_image_candidates(raw.get("location", {}).get("photos") if isinstance(raw.get("location"), Mapping) else None, candidates)
        self._collect_image_candidates(raw.get("location", {}).get("images") if isinstance(raw.get("location"), Mapping) else None, candidates)

        for key in (
            "featuredImage",
            "featureImage",
            "poster",
            "cover",
        ):
            self._collect_image_candidates(raw.get(key), candidates)

        return self._dedupe_urls(candidates)

    def _collect_image_candidates(self, value: Any, collector: list[str]) -> None:
        if value is None:
            return

        if isinstance(value, str):
            if self._is_http_url(value):
                collector.append(value.strip())
            return

        if isinstance(value, Mapping):
            for key in (
                "url",
                "src",
                "photoUrl",
                "imageUrl",
                "thumbnail",
                "uri",
                "link",
                "href",
            ):
                self._collect_image_candidates(value.get(key), collector)

            for key in ("photos", "images", "media", "mediaItems", "assets", "items", "thumbnails"):
                self._collect_image_candidates(value.get(key), collector)
            return

        if isinstance(value, (list, tuple)):
            for item in value:
                self._collect_image_candidates(item, collector)

    @staticmethod
    def _is_http_url(value: str) -> bool:
        lowered = value.lower().strip()
        return lowered.startswith("http://") or lowered.startswith("https://")

    @staticmethod
    def _dedupe_urls(values: list[str]) -> list[str]:
        return list(dict.fromkeys([item.strip() for item in values if isinstance(item, str) and item.strip()]))

    def _extract_from_raw(self, raw: Any, *keys: str) -> Any:
        if not isinstance(raw, Mapping):
            return None
        for key in keys:
            value = raw.get(key)
            if value:
                return value
        return None

    def _business_status(self, is_open: bool | None, source_status: Any = None) -> str:
        if source_status:
            return self._to_str(source_status)
        if is_open is True:
            return "OPERATIONAL"
        if is_open is False:
            return "CLOSED"
        return "unknown"

    def _datetime_to_iso(self, value: Any) -> str:
        if isinstance(value, str):
            return value
        if isinstance(value, datetime):
            return value.isoformat()
        with suppress(Exception):
            if isinstance(value, int | float):
                return datetime.fromtimestamp(value, tz=timezone.utc).isoformat()
        return ""
