"""Local google-map-scraper subprocess wrapper."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import tempfile
from typing import Any, Mapping, Optional, cast

from ..config.settings import Settings
from ..shared.exceptions import ExternalServiceError


@dataclass(frozen=True)
class GoogleMapScraperValidationIssue:
    field: str
    message: str


class GoogleMapScraperClient:
    def __init__(
        self,
        scraper_path: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
        todo_dir: Optional[str] = None,
    ):
        settings = Settings()
        self.scraper_path = Path(scraper_path or settings.google_map_scraper_path)
        self.timeout_seconds = timeout_seconds or settings.google_map_scraper_timeout or settings.request_timeout_seconds
        self.todo_dir = Path(todo_dir or settings.google_map_scraper_todo_dir)

    def scrape_places(
        self,
        *,
        location_query: str = "",
        search_strings: list[str] | None = None,
        max_results_per_search: int = 0,
        language: str = "",
    ) -> dict[str, Any]:
        keywords = [str(value or "").strip() for value in (search_strings or []) if str(value or "").strip()]
        queries = self._build_queries(location_query=location_query, search_strings=keywords)
        if not queries:
            return {"items": [], "meta": {"queries": [], "result_count": 0}}

        if not self.scraper_path.exists():
            raise ExternalServiceError(f"google-map-scraper not found at {self.scraper_path}")

        with tempfile.TemporaryDirectory(prefix="google-map-scraper-") as temp_dir:
            temp_path = Path(temp_dir)
            execution = self._build_execution_plan(
                temp_path=temp_path,
                location_query=location_query,
                keywords=keywords,
                queries=queries,
                max_results_per_search=max_results_per_search,
                language=language,
            )

            completed = subprocess.run(
                execution["command"],
                cwd=execution.get("cwd"),
                check=False,
                capture_output=True,
                text=True,
                timeout=max(1, self.timeout_seconds),
            )
            if completed.returncode != 0:
                stderr = (completed.stderr or completed.stdout or "").strip()
                raise ExternalServiceError(
                    f"google-map-scraper failed with exit code {completed.returncode}: {stderr or 'no output'}"
                )

            payload = self._load_result_payload(
                result_path=cast(Path, execution["result_path"]),
                stdout=completed.stdout,
            )
            normalized_items = [self._normalize_item(item) for item in payload if isinstance(item, Mapping)]
            if max_results_per_search > 0:
                normalized_items = normalized_items[: max_results_per_search * max(1, len(queries))]

            issues = self._validate_items(normalized_items)
            if issues:
                todo_path = self._write_todo(issues=issues, location_query=location_query, queries=queries)
                raise ExternalServiceError(
                    f"google-map-scraper output is missing required fields; wrote follow-up to {todo_path}"
                )

            return {
                "items": normalized_items,
                "meta": {
                    "queries": queries,
                    "result_count": len(normalized_items),
                    "command": execution["command"],
                },
            }

    def _build_execution_plan(
        self,
        *,
        temp_path: Path,
        location_query: str,
        keywords: list[str],
        queries: list[str],
        max_results_per_search: int,
        language: str,
    ) -> dict[str, Any]:
        if self.scraper_path.is_dir():
            return self._build_project_command(
                temp_path=temp_path,
                location_query=location_query,
                keywords=keywords,
                queries=queries,
                max_results_per_search=max_results_per_search,
                language=language,
            )

        query_file = temp_path / "queries.txt"
        result_file = temp_path / "results.json"
        query_file.write_text("\n".join(queries), encoding="utf-8")

        command = [
            str(self.scraper_path),
            "-input",
            str(query_file),
            "-results",
            str(result_file),
            "-json",
            "-exit-on-inactivity",
            "3m",
        ]
        resolved_language = (language or "").strip()
        if resolved_language:
            command.extend(["-lang", resolved_language])
        return {"command": command, "result_path": result_file}

    def _build_project_command(
        self,
        *,
        temp_path: Path,
        location_query: str,
        keywords: list[str],
        queries: list[str],
        max_results_per_search: int,
        language: str,
    ) -> dict[str, Any]:
        project_dir = self.scraper_path
        entrypoint = project_dir / "dist" / "src" / "index.js"
        if not entrypoint.exists():
            self._build_project_dist(project_dir)
        if not entrypoint.exists():
            raise ExternalServiceError(f"google-map-scraper build output not found at {entrypoint}")

        output_dir = temp_path / "output"
        command = [
            "node",
            str(entrypoint),
            "--keywords",
            ",".join(keywords),
            "--region",
            location_query,
            "--output",
            str(output_dir),
            "--format",
            "json",
            "--max-places",
            str(max(1, max_results_per_search or len(queries))),
            "--headless",
        ]
        resolved_language = (language or "").strip()
        if resolved_language:
            command.extend(["--locale", resolved_language])
        return {
            "command": command,
            "cwd": str(project_dir),
            "result_path": output_dir,
        }

    def _build_project_dist(self, project_dir: Path) -> None:
        completed = subprocess.run(
            ["npm", "run", "build"],
            cwd=str(project_dir),
            check=False,
            capture_output=True,
            text=True,
            timeout=max(30, self.timeout_seconds),
        )
        if completed.returncode != 0:
            stderr = (completed.stderr or completed.stdout or "").strip()
            raise ExternalServiceError(f"google-map-scraper build failed: {stderr or 'no output'}")

    def _build_queries(self, *, location_query: str, search_strings: list[str]) -> list[str]:
        resolved_location = (location_query or "").strip()
        queries: list[str] = []
        for value in search_strings:
            query = str(value or "").strip()
            if not query:
                continue
            if resolved_location and resolved_location.lower() not in query.lower():
                query = f"{query} {resolved_location}".strip()
            queries.append(query)
        return queries

    def _load_result_payload(self, *, result_path: Path, stdout: str) -> list[Any]:
        if result_path.is_dir():
            candidate_files = sorted(
                path
                for path in result_path.glob("*.json")
                if path.is_file() and not path.name.endswith(".checkpoint.json")
            )
            if not candidate_files:
                raw_text = stdout.strip()
            else:
                raw_text = candidate_files[0].read_text(encoding="utf-8").strip()
        else:
            raw_text = result_path.read_text(encoding="utf-8").strip() if result_path.exists() else stdout.strip()
        if not raw_text:
            return []
        try:
            payload = json.loads(raw_text)
        except json.JSONDecodeError as exc:
            raise ExternalServiceError("google-map-scraper returned invalid JSON output") from exc
        if isinstance(payload, list):
            return payload
        if isinstance(payload, Mapping):
            places = payload.get("places", [])
            if isinstance(places, list):
                search_results = payload.get("searchResults", [])
                reviews = payload.get("reviews", [])
                photos = payload.get("photos", [])
                return self._attach_scrape_context(
                    places,
                    search_results if isinstance(search_results, list) else [],
                    reviews if isinstance(reviews, list) else [],
                    photos if isinstance(photos, list) else [],
                )
        raise ExternalServiceError("google-map-scraper JSON output must be a list or contain a places array")

    def _attach_scrape_context(
        self,
        places: list[Any],
        search_results: list[Any],
        reviews: list[Any],
        photos: list[Any],
    ) -> list[Any]:
        search_result_by_id = {
            str(item.get("id")).strip(): item
            for item in search_results
            if isinstance(item, Mapping) and str(item.get("id", "")).strip()
        }
        review_count_by_place_id: dict[str, int] = {}
        for item in reviews:
            if not isinstance(item, Mapping):
                continue
            place_id = str(item.get("placeId", "")).strip()
            if place_id:
                review_count_by_place_id[place_id] = review_count_by_place_id.get(place_id, 0) + 1
        photo_urls_by_place_id: dict[str, list[str]] = {}
        for item in photos:
            if not isinstance(item, Mapping):
                continue
            place_id = str(item.get("placeId", "")).strip()
            image_url = str(item.get("imageUrl", "")).strip()
            if place_id and image_url:
                photo_urls_by_place_id.setdefault(place_id, []).append(image_url)

        merged: list[Any] = []
        for place in places:
            if not isinstance(place, Mapping):
                merged.append(place)
                continue
            place_id = str(place.get("id", "")).strip()
            search_result_id = str(place.get("searchResultId", "")).strip()
            search_result = search_result_by_id.get(search_result_id)
            enriched = dict(place)
            if isinstance(search_result, Mapping):
                enriched["searchResult"] = search_result
            if place_id and not enriched.get("reviewCount") and review_count_by_place_id.get(place_id):
                enriched["reviewCount"] = review_count_by_place_id[place_id]
            if place_id and photo_urls_by_place_id.get(place_id):
                enriched["image_urls"] = photo_urls_by_place_id[place_id]
            merged.append(enriched)
        return merged

    def _normalize_item(self, item: Mapping[str, Any]) -> dict[str, Any]:
        raw = dict(item)
        search_result = raw.get("searchResult") if isinstance(raw.get("searchResult"), Mapping) else {}
        place_id = self._pick_first(
            raw,
            "id",
            "place_id",
            "placeId",
            "google_place_id",
            "googlePlaceId",
            "googleMapsPlaceId",
            "google_maps_place_id",
            "cid",
            "data_id",
            "dataId",
            "link",
        )
        rating = self._to_float(self._pick_first(raw, "rating", "review_rating") or self._pick_first(cast(Mapping[str, Any], search_result), "rating"))
        review_count = self._to_int(
            self._pick_first(raw, "review_count", "reviewCount", "reviewsCount", "reviews", "user_ratings_total")
            or self._pick_first(cast(Mapping[str, Any], search_result), "reviewCount", "review_count")
        )
        coordinates = raw.get("coordinates") if isinstance(raw.get("coordinates"), Mapping) else {}
        parsed_coordinates = self._parse_coordinates_from_url(
            self._pick_first(raw, "sourceUrl", "searchUrl", "placeUrl", "googleMapsUrl", "mapsUrl", "link", "url")
            or self._pick_first(cast(Mapping[str, Any], search_result), "placeUrl")
        )
        latitude = self._to_float(
            self._pick_first(raw, "lat", "latitude")
            or self._pick_first(cast(Mapping[str, Any], coordinates), "lat")
            or parsed_coordinates.get("lat")
        )
        longitude = self._to_float(
            self._pick_first(raw, "lng", "longitude")
            or self._pick_first(cast(Mapping[str, Any], coordinates), "lng")
            or parsed_coordinates.get("lng")
        )
        categories = self._to_list(self._pick_first(raw, "categories", "category") or self._pick_first(cast(Mapping[str, Any], search_result), "category"))
        status = self._pick_first(raw, "businessStatus", "business_status", "status")
        raw_name = self._pick_first(raw, "name", "title")
        fallback_name = self._pick_first(cast(Mapping[str, Any], search_result), "title", "rawLabel")
        name = fallback_name if str(raw_name).strip() in {"Google 지도", "Google Maps"} and fallback_name else raw_name or fallback_name

        normalized = {
            **raw,
            "id": place_id,
            "name": name,
            "category": categories or self._to_list(self._pick_first(raw, "category")),
            "address": self._pick_first(raw, "address", "fullAddress")
            or self._pick_first(cast(Mapping[str, Any], search_result), "snippet"),
            "latitude": latitude,
            "longitude": longitude,
            "rating": rating,
            "review_count": review_count,
            "phone": self._pick_first(raw, "phone", "phone_number"),
            "website": self._pick_first(raw, "website", "websiteUrl"),
            "googleMapsUrl": self._pick_first(raw, "googleMapsUrl", "mapsUrl", "sourceUrl", "link", "url"),
            "businessStatus": status,
            "openHours": self._pick_first(raw, "openHours", "open_hours"),
            "image_urls": self._to_list(self._pick_first(raw, "image_urls", "images", "thumbnail")),
            "raw_payload": raw,
            "scrapedAt": datetime.now(timezone.utc).isoformat(),
        }
        return normalized

    def _validate_items(self, items: list[Mapping[str, Any]]) -> list[GoogleMapScraperValidationIssue]:
        if not items:
            return [GoogleMapScraperValidationIssue(field="items", message="No places were returned.")]

        valid_items = 0
        missing_address = 0
        missing_rating = 0
        for item in items:
            if self._pick_first(item, "id") and self._pick_first(item, "name") and self._has_coordinates(item):
                valid_items += 1
            if not self._pick_first(item, "address"):
                missing_address += 1
            if self._to_float(item.get("rating")) <= 0 and self._to_int(item.get("review_count")) <= 0:
                missing_rating += 1

        issues: list[GoogleMapScraperValidationIssue] = []
        if valid_items == 0:
            issues.append(
                GoogleMapScraperValidationIssue(
                    field="identity/location",
                    message="No returned place had a stable identifier, name, and coordinates together.",
                )
            )
        if missing_address == len(items):
            issues.append(GoogleMapScraperValidationIssue(field="address", message="All returned places are missing addresses."))
        if missing_rating == len(items):
            issues.append(
                GoogleMapScraperValidationIssue(
                    field="rating/review_count",
                    message="All returned places are missing rating and review count data.",
                )
            )
        return issues

    def _write_todo(
        self,
        *,
        issues: list[GoogleMapScraperValidationIssue],
        location_query: str,
        queries: list[str],
    ) -> Path:
        todo_dir = self.todo_dir
        todo_dir.mkdir(parents=True, exist_ok=True)
        todo_path = todo_dir / "TODO.md"
        lines = [
            "# TODO",
            "",
            "google-map-scraper output did not satisfy the japantravel place collection contract.",
            "",
            "## Missing or insufficient data",
        ]
        for issue in issues:
            lines.append(f"- `{issue.field}`: {issue.message}")
        lines.extend(
            [
                "",
                "## Context",
                f"- location_query: `{location_query or '(empty)'}`",
                f"- queries: `{', '.join(queries) or '(none)'}`",
                "",
                "## Expected minimum contract",
                "- At least one place must include a stable identifier, a name, and coordinates.",
                "- Returned places should include address data for article generation.",
                "- Returned places should include rating or review count data for ranking and filtering.",
            ]
        )
        todo_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return todo_path

    @staticmethod
    def _pick_first(payload: Mapping[str, Any], *keys: str) -> Any:
        for key in keys:
            value = payload.get(key)
            if value is None:
                continue
            if isinstance(value, str):
                if value.strip():
                    return value.strip()
                continue
            return value
        return ""

    @staticmethod
    def _to_int(value: Any) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _to_float(value: Any) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _to_list(value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            stripped = value.strip()
            return [stripped] if stripped else []
        if isinstance(value, Mapping):
            return [str(item).strip() for item in value.values() if str(item).strip()]
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        return [str(value).strip()] if str(value).strip() else []

    @staticmethod
    def _has_coordinates(payload: Mapping[str, Any]) -> bool:
        try:
            return bool(float(payload.get("latitude"))) and bool(float(payload.get("longitude")))
        except (TypeError, ValueError):
            return False

    @staticmethod
    def _parse_coordinates_from_url(value: Any) -> dict[str, float]:
        text = str(value or "").strip()
        if not text:
            return {}
        marker = "!3d"
        try:
            if marker in text and "!4d" in text:
                lat_part = text.split("!3d", 1)[1].split("!4d", 1)[0]
                lng_part = text.split("!4d", 1)[1].split("!", 1)[0]
                return {"lat": float(lat_part), "lng": float(lng_part)}
        except (TypeError, ValueError):
            return {}
        return {}
