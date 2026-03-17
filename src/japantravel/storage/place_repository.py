"""Persistent place cache repository backed by PostgreSQL."""

from __future__ import annotations

import json
import logging
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Optional
from decimal import Decimal

try:
    from psycopg import connect
    from psycopg.rows import dict_row
except ModuleNotFoundError:
    connect = None  # type: ignore[assignment]
    dict_row = None  # type: ignore[assignment]


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
                        place_apify_output_snapshot,
                        place
                    RESTART IDENTITY CASCADE
                    """
                )
            connection.commit()

    def upsert_places(
        self,
        raw_places: list[Mapping[str, Any]],
        source: str = "apify",
        actor_id: Optional[str] = None,
        dataset_id: Optional[str] = None,
        conflict_mode: str = "update",
    ) -> PlaceRepositoryResult:
        if not self.enabled or not raw_places:
            return PlaceRepositoryResult(fetched_count=0, inserted_count=0, reused_count=0, errors=[])
        if conflict_mode not in {"update", "skip"}:
            raise ValueError("conflict_mode must be either 'update' or 'skip'.")

        inserted_count = 0
        skipped_count = 0
        errors: list[str] = []
        sql = self._place_upsert_sql(conflict_mode)

        with connect(self.db_url) as connection:
            connection.row_factory = dict_row
            with connection.cursor() as cursor:
                for raw in raw_places:
                    normalized = self._normalize_raw(
                        raw,
                        source=source,
                        actor_id=actor_id,
                        dataset_id=dataset_id,
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

    def has_recent_collection(self, interval_minutes: int = 240, source: str = "apify") -> bool:
        if interval_minutes <= 0:
            return False
        threshold = datetime.now(timezone.utc) - timedelta(minutes=interval_minutes)
        with connect(self.db_url) as connection:
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
                    (source, threshold),
                )
                row = cursor.fetchone()

        return bool(row and row[0])

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
            "source": self._to_str(row.get("source")) or "apify",
            "tags": tags,
        }

    def _normalize_raw(
        self,
        raw: Mapping[str, Any],
        source: str = "apify",
        actor_id: Optional[str] = None,
        dataset_id: Optional[str] = None,
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
        apify_collected_at = self._to_optional_str(
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
            "apify_actor_id": self._to_str(raw_payload.get("apify_actor_id") or raw.get("actorId") or actor_id),
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
            "apify_output": json.dumps(raw_payload, ensure_ascii=False),
            "apify_collected_at": apify_collected_at,
            "dataset_id": self._to_optional_str(dataset_id),
            "dataset_item_id": self._resolve_dataset_item_id(raw),
            "payload_version": "crawler-google-places-v1",
            "raw_payload": json.dumps(raw_payload, ensure_ascii=False),
        }
        return {"params": params, "external_place_id": source_id}

    def _place_upsert_sql(self, conflict_mode: str) -> str:
        conflict_clause = """
            ON CONFLICT (source, external_place_id)
            DO NOTHING
            RETURNING id
        """
        if conflict_mode == "update":
            conflict_clause = """
                ON CONFLICT (source, external_place_id)
                DO UPDATE SET
                    google_place_id = COALESCE(EXCLUDED.google_place_id, place.google_place_id),
                    apify_actor_id = COALESCE(EXCLUDED.apify_actor_id, place.apify_actor_id),
                    name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    address = EXCLUDED.address,
                    region = EXCLUDED.region,
                    country = EXCLUDED.country,
                    latitude = EXCLUDED.latitude,
                    longitude = EXCLUDED.longitude,
                    category = EXCLUDED.category,
                    rating = EXCLUDED.rating,
                    review_count = EXCLUDED.review_count,
                    price_level = EXCLUDED.price_level,
                    is_open = EXCLUDED.is_open,
                    place_url = EXCLUDED.place_url,
                    google_maps_url = EXCLUDED.google_maps_url,
                    name_local = EXCLUDED.name_local,
                    locality = EXCLUDED.locality,
                    state = EXCLUDED.state,
                    country_code = EXCLUDED.country_code,
                    zip = EXCLUDED.zip,
                    phone_number = EXCLUDED.phone_number,
                    rating_count = EXCLUDED.rating_count,
                    review_count_delta = EXCLUDED.review_count_delta,
                    yelp_rating = EXCLUDED.yelp_rating,
                    google_score = EXCLUDED.google_score,
                    business_status = EXCLUDED.business_status,
                    open_hours = EXCLUDED.open_hours,
                    image_urls = EXCLUDED.image_urls,
                    website = EXCLUDED.website,
                    short_address = EXCLUDED.short_address,
                    full_address = EXCLUDED.full_address,
                    weekday_hours = EXCLUDED.weekday_hours,
                    google_maps_place_id = EXCLUDED.google_maps_place_id,
                    apify_output = EXCLUDED.apify_output,
                    apify_collected_at = COALESCE(EXCLUDED.apify_collected_at, place.apify_collected_at),
                    dataset_id = COALESCE(EXCLUDED.dataset_id, place.dataset_id),
                    dataset_item_id = COALESCE(EXCLUDED.dataset_item_id, place.dataset_item_id),
                    payload_version = COALESCE(EXCLUDED.payload_version, place.payload_version),
                    raw_payload = EXCLUDED.raw_payload,
                    is_active = TRUE,
                    updated_at = NOW()
                RETURNING id
            """

        return f"""
            INSERT INTO place (
                source,
                external_place_id,
                google_place_id,
                apify_actor_id,
                name,
                description,
                address,
                region,
                country,
                latitude,
                longitude,
                category,
                rating,
                review_count,
                price_level,
                is_open,
                place_url,
                google_maps_url,
                name_local,
                locality,
                state,
                country_code,
                zip,
                phone_number,
                rating_count,
                review_count_delta,
                yelp_rating,
                google_score,
                business_status,
                open_hours,
                image_urls,
                website,
                short_address,
                full_address,
                weekday_hours,
                google_maps_place_id,
                apify_output,
                apify_collected_at,
                dataset_id,
                dataset_item_id,
                payload_version,
                raw_payload,
                is_active,
                updated_at
            ) VALUES (
                %(source)s,
                %(external_place_id)s,
                %(google_place_id)s,
                %(apify_actor_id)s,
                %(name)s,
                %(description)s,
                %(address)s,
                %(region)s,
                %(country)s,
                %(latitude)s,
                %(longitude)s,
                %(category)s,
                %(rating)s,
                %(review_count)s,
                %(price_level)s,
                %(is_open)s,
                %(place_url)s,
                %(google_maps_url)s,
                %(name_local)s,
                %(locality)s,
                %(state)s,
                %(country_code)s,
                %(zip)s,
                %(phone_number)s,
                %(rating_count)s,
                %(review_count_delta)s,
                %(yelp_rating)s,
                %(google_score)s,
                %(business_status)s,
                %(open_hours)s,
                %(image_urls)s,
                %(website)s,
                %(short_address)s,
                %(full_address)s,
                %(weekday_hours)s,
                %(google_maps_place_id)s,
                %(apify_output)s,
                %(apify_collected_at)s,
                %(dataset_id)s,
                %(dataset_item_id)s,
                %(payload_version)s,
                %(raw_payload)s,
                TRUE,
                NOW()
            )
            {conflict_clause}
        """

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

    def _resolve_dataset_item_id(self, raw: Mapping[str, Any]) -> Optional[str]:
        return self._to_optional_str(
            raw.get("dataset_item_id")
            or raw.get("datasetItemId")
            or raw.get("itemId")
            or raw.get("_id")
            or raw.get("__itemId")
        )

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
