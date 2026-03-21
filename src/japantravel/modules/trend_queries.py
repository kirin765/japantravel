"""Trend-backed search query planning for Apify collection."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Protocol, Sequence

try:
    from pytrends.request import TrendReq
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    TrendReq = None  # type: ignore[assignment]


REGION_DISPLAY_NAME_MAP = {
    "tokyo": "도쿄",
    "osaka": "오사카",
    "kyoto": "교토",
    "fukuoka": "후쿠오카",
    "okinawa": "오키나와",
    "hokkaido": "홋카이도",
}


@dataclass(frozen=True)
class RegionCollectTarget:
    location_query: str
    display_name: str
    region_key: str


@dataclass(frozen=True)
class TrendQueryPlan:
    location_query: str
    display_name: str
    queries: list[str]
    providers_attempted: list[str]
    providers_used: list[str]
    errors: list[str]

    def to_payload(self) -> dict[str, Any]:
        return {
            "location_query": self.location_query,
            "display_name": self.display_name,
            "queries": list(self.queries),
            "providers_attempted": list(self.providers_attempted),
            "providers_used": list(self.providers_used),
            "errors": list(self.errors),
        }


class TrendQueryProvider(Protocol):
    name: str

    def build_queries(self, target: RegionCollectTarget, limit: int) -> list[str]:
        ...


class GoogleTrendsProvider:
    name = "google_trends"

    def __init__(
        self,
        *,
        timeframe: str = "now 7-d",
        geo: str = "KR",
        trend_client: Any = None,
    ) -> None:
        self.timeframe = timeframe
        self.geo = geo
        self.trend_client = trend_client

    def build_queries(self, target: RegionCollectTarget, limit: int) -> list[str]:
        client = self.trend_client or self._build_client()
        seed_keyword = f"{target.display_name} 여행"
        raw_queries: list[str] = []

        client.build_payload([seed_keyword], timeframe=self.timeframe, geo=self.geo)
        related = client.related_queries() or {}
        related_block = related.get(seed_keyword) if isinstance(related, dict) else None
        if isinstance(related_block, dict):
            raw_queries.extend(self._frame_queries(related_block.get("top")))
            raw_queries.extend(self._frame_queries(related_block.get("rising")))

        suggestions_fn = getattr(client, "suggestions", None)
        if callable(suggestions_fn):
            for item in suggestions_fn(keyword=seed_keyword) or []:
                if not isinstance(item, dict):
                    continue
                suggestion = str(item.get("title") or item.get("query") or "").strip()
                if suggestion:
                    raw_queries.append(suggestion)

        return _dedupe_queries(
            _normalize_query_for_region(query, target.display_name)
            for query in raw_queries
        )[:limit]

    @staticmethod
    def _frame_queries(frame: Any) -> list[str]:
        if frame is None:
            return []
        try:
            values = frame["query"].tolist()
        except Exception:
            return []
        return [str(value).strip() for value in values if str(value).strip()]

    @staticmethod
    def _build_client() -> Any:
        if TrendReq is None:
            raise RuntimeError("pytrends is not installed")
        return TrendReq(hl="ko-KR", tz=540)


class CuratedSeedProvider:
    name = "curated_seed"

    def __init__(self, seeds: Sequence[str]) -> None:
        self.seeds = [str(seed).strip() for seed in seeds if str(seed).strip()]

    def build_queries(self, target: RegionCollectTarget, limit: int) -> list[str]:
        queries = [
            _normalize_query_for_region(f"{target.display_name} {seed}", target.display_name)
            for seed in self.seeds
        ]
        return _dedupe_queries(queries)[:limit]


def build_trend_query_providers(
    *,
    trend_source: str,
    seed_keywords: Sequence[str],
    timeframe: str,
    trend_client: Any = None,
) -> list[TrendQueryProvider]:
    normalized_source = str(trend_source or "").strip().lower()
    if normalized_source == "curated_only":
        return [CuratedSeedProvider(seed_keywords)]

    providers: list[TrendQueryProvider] = []
    if normalized_source in {"google_then_curated", "mixed", ""}:
        providers.append(GoogleTrendsProvider(timeframe=timeframe, geo="KR", trend_client=trend_client))
    providers.append(CuratedSeedProvider(seed_keywords))
    return providers


def build_region_trend_query_plan(
    target: RegionCollectTarget,
    *,
    providers: Sequence[TrendQueryProvider],
    limit: int,
) -> TrendQueryPlan:
    merged: list[str] = []
    providers_attempted: list[str] = []
    providers_used: list[str] = []
    errors: list[str] = []

    for provider in providers:
        providers_attempted.append(provider.name)
        try:
            queries = provider.build_queries(target, limit=limit)
        except Exception as exc:
            errors.append(f"{provider.name}: {exc}")
            continue

        if not queries:
            continue

        providers_used.append(provider.name)
        for query in queries:
            if query and query not in merged:
                merged.append(query)

    return TrendQueryPlan(
        location_query=target.location_query,
        display_name=target.display_name,
        queries=merged[:limit],
        providers_attempted=providers_attempted,
        providers_used=providers_used,
        errors=errors,
    )


def parse_core_region_targets(raw_value: str) -> list[RegionCollectTarget]:
    entries = [item.strip() for item in str(raw_value or "").split("|") if item.strip()]
    targets: list[RegionCollectTarget] = []
    seen: set[str] = set()
    for entry in entries:
        normalized_entry = re.sub(r"\s+", " ", entry).replace(", ", ",").strip()
        region_key = _location_region_key(normalized_entry)
        if not normalized_entry or region_key in seen:
            continue
        seen.add(region_key)
        targets.append(
            RegionCollectTarget(
                location_query=normalized_entry,
                display_name=_display_name_for_location(normalized_entry),
                region_key=region_key,
            )
        )
    return targets


def _location_region_key(location_query: str) -> str:
    first_segment = str(location_query or "").split(",")[0].strip().lower()
    return re.sub(r"[^a-z0-9가-힣]+", "_", first_segment).strip("_")


def _display_name_for_location(location_query: str) -> str:
    first_segment = str(location_query or "").split(",")[0].strip()
    if not first_segment:
        return "일본"
    mapped = REGION_DISPLAY_NAME_MAP.get(first_segment.lower())
    return mapped or first_segment


def _normalize_query_for_region(query: str, display_name: str) -> str:
    normalized = re.sub(r"\s+", " ", str(query or "")).strip()
    if not normalized:
        return ""
    if display_name and display_name not in normalized:
        normalized = f"{display_name} {normalized}".strip()
    return re.sub(r"\s+", " ", normalized)


def _dedupe_queries(values: Sequence[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        normalized = str(value).strip()
        if not normalized:
            continue
        key = normalized.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(normalized)
    return deduped
