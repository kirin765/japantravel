"""Job definitions for the automation pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import logging
from urllib.parse import quote_plus
import time
from typing import Any, Callable, Dict, List, Mapping, Optional

from ..clients import ApifyClient, GooglePlacesClient, OpenAIClient, WordPressClient
from ..config.settings import Settings
from ..storage import PlaceRepository
from ..modules.generation import GenerationPipeline
from ..modules.refresh import RefreshPipeline
from ..modules.ranking import selectors
from ..modules.ranking import scorer
from ..modules.publish import PublishPipeline
from ..modules.review import ReviewPipeline
from ..shared.exceptions import ExternalServiceError
from ..shared.models import ArticleCandidate, PublishedArticle


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Job:
    name: str
    handler: Callable[..., Dict[str, Any]]


@dataclass
class PipelineContext:
    """In-memory runtime context for scheduled jobs."""

    settings: Settings = field(default_factory=Settings)
    apify: Optional[ApifyClient] = None
    google_places: Optional[GooglePlacesClient] = None
    openai: Optional[OpenAIClient] = None
    wp: Optional[WordPressClient] = None
    place_repo: Optional[PlaceRepository] = None

    raw_collections: List[Dict[str, Any]] = field(default_factory=list)
    article_candidates: List[ArticleCandidate] = field(default_factory=list)
    generated_articles: List[Dict[str, Any]] = field(default_factory=list)
    published_articles: List[PublishedArticle] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.apify = self.apify or _init_client("apify", ApifyClient)
        self.google_places = self.google_places or _init_client("google_places", GooglePlacesClient)
        self.openai = self.openai or _init_client("openai", OpenAIClient)
        self.wp = self.wp or _init_client("wordpress", WordPressClient)
        self.place_repo = self.place_repo or _init_client("place_repo", self._build_place_repository)

    def _build_place_repository(self) -> Optional[PlaceRepository]:
        db_url = self.settings.db_url
        if not db_url:
            return None
        return PlaceRepository(db_url)


def _init_client(name: str, constructor):
    try:
        return constructor()
    except Exception as exc:
        logger.warning("Could not initialize %s client: %s", name, exc)
        return None


def collect_job(context: PipelineContext | None = None) -> Dict[str, Any]:
    """Collect raw places from Apify and keep candidates in context."""
    ctx = _ensure_context(context)
    logger.info("collect_job started")

    city, country = _parse_location(ctx.settings.apify_location_query)
    cached_count = 0
    cached_items: list[Dict[str, Any]] = []

    if ctx.place_repo is not None:
        cached_items = ctx.place_repo.fetch_reusable_candidates(
            city=city,
            country=country,
            limit=ctx.settings.place_cache_fetch_limit,
            stale_days=ctx.settings.place_cache_ttl_days,
            strict_fields=ctx.settings.place_cache_strict_fields,
        )
        cached_count = len(cached_items)
        if cached_count >= ctx.settings.place_cache_min_count and not ctx.settings.apify_force_refresh:
            ctx.raw_collections = cached_items
            logger.info("collect_job skipped Apify; cached candidates=%s", cached_count)
            return {
                "job": "collect",
                "status": "ok_cached",
                "count": cached_count,
                "source": "db",
                "run_result": {},
            }

        if not ctx.settings.apify_force_refresh and ctx.place_repo.has_recent_collection(
            interval_minutes=ctx.settings.apify_min_new_run_interval_minutes,
        ):
            if cached_items:
                ctx.raw_collections = cached_items
            logger.info("collect_job skipped Apify due to run interval policy; cached candidates=%s", cached_count)
            return {
                "job": "collect",
                "status": "ok_cached",
                "count": cached_count,
                "source": "db_recent_only",
                "run_result": {},
            }

    if ctx.apify is None or not ctx.settings.apify_actor_id:
        if cached_items:
            ctx.raw_collections = cached_items
            return {"job": "collect", "status": "ok_cached", "count": cached_count, "source": "db"}
        message = "APIFY client not configured; collect skipped."
        logger.warning(message)
        return {"job": "collect", "status": "skipped", "message": message}

    payload = _build_apify_input(ctx.settings)
    run_result: Dict[str, Any] = {}
    try:
        settled = _run_apify_with_fallback(ctx.apify, ctx.settings.apify_actor_id, payload)
        if not settled.get("succeeded", False):
            if cached_items:
                ctx.raw_collections = cached_items
                return {
                    "job": "collect",
                    "status": "ok_cached",
                    "count": len(cached_items),
                    "source": "db_fallback",
                    "run_error": settled.get("error"),
                    "run_result": settled.get("run", {}),
                }
            return {
                "job": "collect",
                "status": "error",
                "error": settled.get("error", "Apify run did not succeed."),
                "run_result": settled.get("run", {}),
            }
        run_result = settled.get("run", {})
        run_id = run_result.get("id")
        if not run_id:
            raise ExternalServiceError("Apify run id not returned")

        raw_items = _apify_payload(ctx.apify.get_run_items(run_id))
        if isinstance(raw_items, dict):
            items = raw_items.get("items", [])
        else:
            items = raw_items

        if ctx.place_repo is not None and isinstance(items, list):
            upsert = ctx.place_repo.upsert_places(
                items,
                source="apify",
                actor_id=ctx.settings.apify_actor_id,
                dataset_id=_run_dataset_id(run_result),
                conflict_mode="update",
            )
            logger.info(
                "collect_job upserted places count=%s inserted=%s skipped=%s errors=%s",
                upsert.fetched_count,
                upsert.inserted_count,
                upsert.skipped_count,
                len(upsert.errors),
            )

        if ctx.place_repo is not None:
            cached_items = ctx.place_repo.fetch_reusable_candidates(
                city=city,
                country=country,
                limit=ctx.settings.place_cache_fetch_limit,
                stale_days=ctx.settings.place_cache_ttl_days,
                strict_fields=ctx.settings.place_cache_strict_fields,
            )
            ctx.raw_collections = cached_items
        else:
            ctx.raw_collections = list(items or [])

        status = "ok" if ctx.raw_collections else "empty"
        logger.info("collect_job finished; count=%s", len(ctx.raw_collections))
        return {
            "job": "collect",
            "status": status,
            "count": len(ctx.raw_collections),
            "source": "apify_to_db" if ctx.place_repo is not None else "apify",
            "run_result": run_result,
            "cached_before": cached_count,
        }
    except Exception as exc:  # defensive; keep scheduler alive
        logger.error("collect_job failed: %s", exc)
        if cached_items:
            ctx.raw_collections = cached_items
            return {
                "job": "collect",
                "status": "ok_cached",
                "count": len(cached_items),
                "source": "db_fallback",
                "error": str(exc),
                "run_result": run_result,
            }
        return {"job": "collect", "status": "error", "error": str(exc), "run_result": run_result}


def collect_from_apify_dataset(
    context: PipelineContext | None = None,
    dataset_id: Optional[str] = None,
    limit: Optional[int] = None,
    conflict_mode: str = "skip",
) -> Dict[str, Any]:
    """Load existing Apify Storage dataset items and persist them into DB."""
    ctx = _ensure_context(context)
    target_dataset_id = (dataset_id or ctx.settings.apify_dataset_id or "").strip()
    if not target_dataset_id:
        return {"job": "collect_dataset", "status": "skipped", "error": "APIFY_DATASET_ID is not configured"}

    if ctx.apify is None:
        return {"job": "collect_dataset", "status": "skipped", "error": "Apify client is not configured"}

    if ctx.place_repo is None:
        return {"job": "collect_dataset", "status": "error", "error": "Place repository is not configured (DB_URL missing)"}

    city, country = _parse_location(ctx.settings.apify_location_query)
    limit = limit if limit is not None else ctx.settings.apify_dataset_item_limit
    try:
        payload = ctx.apify.get_dataset_items(target_dataset_id, limit=limit, clean=True)
        raw_items = _apify_payload(payload)
        if isinstance(raw_items, Mapping):
            items = raw_items.get("items", [])
        else:
            items = raw_items
        if not isinstance(items, list):
            return {
                "job": "collect_dataset",
                "status": "error",
                "error": "Unexpected dataset items format",
                "dataset_id": target_dataset_id,
            }

        upsert = ctx.place_repo.upsert_places(
            items,
            source="apify",
            actor_id=ctx.settings.apify_actor_id,
            dataset_id=target_dataset_id,
            conflict_mode=conflict_mode,
        )

        ctx.raw_collections = ctx.place_repo.fetch_reusable_candidates(
            city=city,
            country=country,
            limit=ctx.settings.place_cache_fetch_limit,
            stale_days=ctx.settings.place_cache_ttl_days,
            strict_fields=ctx.settings.place_cache_strict_fields,
        )

        return {
            "job": "collect_dataset",
            "status": "ok",
            "count": len(ctx.raw_collections),
            "dataset_id": target_dataset_id,
            "upsert": {
                "fetched_count": upsert.fetched_count,
                "inserted_count": upsert.inserted_count,
                "reused_count": upsert.reused_count,
                "skipped_count": upsert.skipped_count,
                "errors": upsert.errors,
            },
        }
    except Exception as exc:  # defensive
        logger.error("collect_from_apify_dataset failed: %s", exc)
        return {"job": "collect_dataset", "status": "error", "error": str(exc), "dataset_id": target_dataset_id}


def reset_and_collect_from_apify_datasets(
    context: PipelineContext | None = None,
    dataset_ids: Optional[List[str] | str] = None,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    """Reset existing content tables and rebuild places from configured Apify datasets."""
    ctx = _ensure_context(context)
    if ctx.place_repo is None:
        return {"job": "reset_collect_datasets", "status": "error", "error": "Place repository is not configured (DB_URL missing)"}
    if ctx.apify is None:
        return {"job": "reset_collect_datasets", "status": "error", "error": "Apify client is not configured"}

    targets = _resolve_dataset_ids(dataset_ids if dataset_ids is not None else ctx.settings.apify_dataset_ids or ctx.settings.apify_dataset_id)
    if not targets:
        return {"job": "reset_collect_datasets", "status": "skipped", "error": "No Apify dataset ids configured"}

    ctx.place_repo.reset_all_data()

    results: List[Dict[str, Any]] = []
    saved_count = 0
    skipped_count = 0
    error_count = 0

    for target_dataset_id in targets:
        result = collect_from_apify_dataset(
            context=ctx,
            dataset_id=target_dataset_id,
            limit=limit,
            conflict_mode="skip",
        )
        results.append(result)
        upsert = result.get("upsert", {})
        saved_count += int(upsert.get("inserted_count", 0) or 0)
        skipped_count += int(upsert.get("skipped_count", upsert.get("reused_count", 0)) or 0)
        if result.get("status") != "ok":
            error_count += 1

    status = "ok"
    if error_count and saved_count:
        status = "partial"
    elif error_count and not saved_count:
        status = "error"

    return {
        "job": "reset_collect_datasets",
        "status": status,
        "dataset_ids": targets,
        "datasets_processed": len(results),
        "saved_count": saved_count,
        "skipped_duplicates": skipped_count,
        "error_count": error_count,
        "results": results,
    }


def content_cycle_job(context: PipelineContext | None = None, scenario: str = "solo_travel") -> Dict[str, Any]:
    """Run generate -> review -> publish sequentially from the latest DB-backed place cache."""
    ctx = _ensure_context(context)
    logger.info("content_cycle_job started")

    # Always reload from DB so manual Apify collections are picked up by the next scheduled cycle.
    ctx.raw_collections = []
    ctx.article_candidates = []
    ctx.generated_articles = []

    generate_result = generate_job(context=ctx, scenario=scenario)
    if generate_result.get("status") != "ok":
        return {
            "job": "content_cycle",
            "status": generate_result.get("status", "error"),
            "generate": generate_result,
            "review": {"job": "review", "status": "skipped", "reason": "generation failed or skipped"},
            "publish": {"job": "publish", "status": "skipped", "reason": "generation failed or skipped"},
        }

    review_result = review_job(context=ctx)
    if review_result.get("status") != "ok":
        return {
            "job": "content_cycle",
            "status": review_result.get("status", "error"),
            "generate": generate_result,
            "review": review_result,
            "publish": {"job": "publish", "status": "skipped", "reason": "review failed or skipped"},
        }

    publish_result = publish_job(context=ctx)
    status = "ok" if publish_result.get("status") == "ok" else publish_result.get("status", "error")
    return {
        "job": "content_cycle",
        "status": status,
        "generate": generate_result,
        "review": review_result,
        "publish": publish_result,
    }


def generate_job(context: PipelineContext | None = None, scenario: str = "solo_travel") -> Dict[str, Any]:
    """Score and generate article candidates from last collected data."""
    ctx = _ensure_context(context)
    logger.info("generate_job started")

    if not ctx.raw_collections and ctx.place_repo is not None:
        cached = _load_reusable_candidates(ctx, scenario=scenario)
        ctx.raw_collections = cached

    if not ctx.raw_collections:
        return {"job": "generate", "status": "skipped", "reason": "no collected raw data"}

    if ctx.openai is None:
        return {"job": "generate", "status": "error", "error": "OpenAI client is not configured"}

    try:
        normalized_places = [_normalize_place(item) for item in ctx.raw_collections]
        ranked = scorer.score_candidates(normalized_places, scenario=scenario)
        top = selectors.top_k(ranked, ctx.settings.top_n_candidates)
        top_payloads = [item.payload for item in top]

        ctx.article_candidates = [
            _candidate_from_ranking(item, scenario=scenario, city=item.payload.get("city", ""), country=item.payload.get("country", ""))
            for item in top
        ]

        # generate two variants for A/B selection
        candidate_payload = top_payloads
        region = (ctx.article_candidates[0].city or "Tokyo")
        generator = GenerationPipeline(openai_client=ctx.openai, scenario=scenario, max_sections=min(len(candidate_payload), 6))
        variants = []
        for variant_id, tone in [("A", "friendly"), ("B", "warm")]:
            article = generator.generate_article(
                places=candidate_payload,
                region=region,
                duration_days=2,
                tone=tone,
                extra_context={"scenario": scenario, "variant_id": variant_id},
            )
            payload = article.to_payload()
            payload["variant_id"] = variant_id
            payload["generation_tone"] = tone
            variants.append({"variant_id": variant_id, "payload": payload})

        ctx.generated_articles = [
            {
                "candidate_topic": _topic_key(ctx.article_candidates[0]),
                "variants": variants,
                "payload": variants[0]["payload"],
                "places": top_payloads,
            }
        ]
        return {"job": "generate", "status": "ok", "generated": len(ctx.generated_articles)}
    except Exception as exc:
        logger.error("generate_job failed: %s", exc)
        return {"job": "generate", "status": "error", "error": str(exc)}


def review_job(context: PipelineContext | None = None) -> Dict[str, Any]:
    """Run rule+LLM review for generated articles."""
    ctx = _ensure_context(context)
    logger.info("review_job started")

    if not ctx.generated_articles:
        return {"job": "review", "status": "skipped", "reason": "no generated articles"}

    reviewer = ReviewPipeline(openai_client=ctx.openai, allow_place_issues=True)
    results = []
    try:
        for generated in ctx.generated_articles:
            variants = generated.get("variants")
            if isinstance(variants, list) and variants:
                reviewed_variants: List[Dict[str, Any]] = []
                for variant in variants:
                    review = reviewer.review(variant["payload"])
                    variant["review"] = review
                    variant["ab_score"] = _ab_score(review)
                    reviewed_variants.append(variant)

                reviewed_variants.sort(key=lambda item: item.get("ab_score", 0), reverse=True)
                winner = reviewed_variants[0]
                generated["variants"] = reviewed_variants
                generated["payload"] = winner["payload"]
                generated["review"] = winner["review"]
                generated["selected_variant_id"] = winner.get("variant_id")
                results.append(winner["review"])
            else:
                result = reviewer.review(generated["payload"])
                generated["review"] = result
                results.append(result)
        return {"job": "review", "status": "ok", "count": len(results)}
    except Exception as exc:
        logger.error("review_job failed: %s", exc)
        return {"job": "review", "status": "error", "error": str(exc)}


def publish_job(context: PipelineContext | None = None) -> Dict[str, Any]:
    """Publish reviewed articles to WordPress."""
    ctx = _ensure_context(context)
    logger.info("publish_job started")

    if not ctx.generated_articles or ctx.wp is None:
        return {"job": "publish", "status": "skipped", "reason": "no generated articles or no WP client"}

    publisher = PublishPipeline(wp_client=ctx.wp)
    published: list[Dict[str, Any]] = []
    try:
        for generated in ctx.generated_articles:
            places = generated.get("places") or []
            review = generated.get("review") or {}
            status = "pending_review"
            if review and review.get("pass", False):
                status = "publish"

            payload = generated["payload"]
            title = payload.get("title") or "Untitled"
            featured_media_urls = _collect_featured_images(generated.get("places", []), payload)
            result = publisher.publish(
                title=title,
                content=_format_article_content(payload),
                status=status,
                excerpt=payload.get("summary", "")[:190],
                featured_media_urls=featured_media_urls,
                categories=["japan", payload.get("region", "asia")],
                tags=[generated.get("candidate_topic", "travel"), "korean_traveler"],
                dry_run=False,
            )

            post = PublishedArticle(
                wp_post_id=result.get("post_id") or 0,
                slug=result.get("slug", ""),
                title=title,
                topic_key=generated.get("candidate_topic", ""),
                candidate_place_ids=[
                    str(item.get("place_id"))
                    for item in places
                    if isinstance(item, Mapping) and item.get("place_id")
                ] or [
                    str(section.get("place_id") or section.get("id"))
                    for section in payload.get("place_sections", [])
                    if isinstance(section, Mapping) and (section.get("place_id") or section.get("id"))
                ],
                published_at=str(datetime.now(timezone.utc)),
                place_snapshots=places if isinstance(places, list) else [],
                business_status=_derive_business_status(places),
                needs_refresh=False,
                last_content_reviewed_at=str(datetime.now(timezone.utc)),
                last_data_verified_at=str(datetime.now(timezone.utc)),
            )
            ctx.published_articles.append(post)
            published.append({"post_id": post.wp_post_id, "status": status, "result": result})
        return {"job": "publish", "status": "ok", "published": published}
    except Exception as exc:
        logger.error("publish_job failed: %s", exc)
        return {"job": "publish", "status": "error", "error": str(exc)}


def refresh_job(context: PipelineContext | None = None) -> Dict[str, Any]:
    """Mark published posts requiring refresh."""
    ctx = _ensure_context(context)
    logger.info("refresh_job started")

    if not ctx.published_articles:
        return {"job": "refresh", "status": "skipped", "reason": "no published articles"}

    evaluator = RefreshPipeline()
    changed = 0
    for published in ctx.published_articles:
        payload = published.model_dump()
        payload["places"] = payload.get("place_snapshots", payload.get("candidate_place_ids", []))
        decision = evaluator.evaluate(payload)
        if decision.get("needs_refresh"):
            published.needs_refresh = True  # type: ignore[attr-defined]
            changed += 1
    return {"job": "refresh", "status": "ok", "needs_refresh_count": changed}


def _normalize_place(raw: Mapping[str, Any]) -> Dict[str, Any]:
    payload = dict(raw)
    payload.setdefault("id", payload.get("place_id") or payload.get("placeId") or payload.get("id"))
    payload.setdefault("rating", _to_float(payload.get("rating", 0.0)))
    payload.setdefault("review_count", _to_int(payload.get("user_ratings_total", payload.get("review_count", 0))))
    payload.setdefault("name", payload.get("name", ""))
    payload.setdefault("city", payload.get("city", payload.get("address", "")))
    payload.setdefault("country", payload.get("country", ""))
    payload.setdefault("business_status", payload.get("business_status", "unknown"))
    payload.setdefault("image_urls", _collect_image_urls(payload))
    payload.setdefault("maps_url", _collect_maps_url(payload))
    payload.setdefault("maps_embed_url", _infer_maps_embed_url(payload))
    return payload


def _load_reusable_candidates(ctx: PipelineContext, scenario: str) -> list[dict[str, Any]]:
    city, country = _parse_location(ctx.settings.apify_location_query)
    if ctx.place_repo is None:
        return []

    strict_items = ctx.place_repo.fetch_reusable_candidates(
        city=city,
        country=country,
        limit=max(ctx.settings.top_n_candidates, ctx.settings.place_cache_fetch_limit),
        stale_days=ctx.settings.place_cache_ttl_days,
        strict_fields=ctx.settings.place_cache_strict_fields,
    )
    if strict_items:
        logger.info("generate_job loaded %s strict cached candidates", len(strict_items))
        return strict_items

    relaxed_items = ctx.place_repo.fetch_reusable_candidates(
        city=city,
        country=country,
        limit=max(ctx.settings.top_n_candidates, ctx.settings.place_cache_fetch_limit),
        stale_days=ctx.settings.place_cache_ttl_days * 2,
        strict_fields=False,
    )
    if relaxed_items:
        logger.info("generate_job loaded %s relaxed cached candidates for scenario=%s", len(relaxed_items), scenario)
        return relaxed_items

    # Stored place values may use district names / country codes that do not exactly match APIFY_LOCATION_QUERY.
    if city or country:
        fallback_items = ctx.place_repo.fetch_reusable_candidates(
            city="",
            country="",
            limit=max(ctx.settings.top_n_candidates, ctx.settings.place_cache_fetch_limit),
            stale_days=ctx.settings.place_cache_ttl_days * 2,
            strict_fields=False,
        )
        logger.info(
            "generate_job loaded %s global fallback cached candidates for scenario=%s location=%s,%s",
            len(fallback_items),
            scenario,
            city,
            country,
        )
        return fallback_items

    logger.info("generate_job loaded %s relaxed cached candidates for scenario=%s", len(relaxed_items), scenario)
    return relaxed_items


def _parse_location(raw: str) -> tuple[str, str]:
    if not raw:
        return "", ""
    parts = [segment.strip() for segment in raw.split(",") if segment.strip()]
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[1]


def _candidate_from_ranking(
    rank_item: "scorer.RankItem",
    scenario: str,
    city: str,
    country: str,
) -> ArticleCandidate:
    normalized_country = country.strip().lower() if isinstance(country, str) and country.strip() else "unknown"
    return ArticleCandidate(
        topic_key=f"{normalized_country}-{scenario}-{rank_item.payload.get('place_type', 'general')}",
        city=city,
        country=country,
        scenario=scenario,
        place_type=rank_item.payload.get("category", "general"),
        audience="korean_traveler",
        query_text=f"{city or country} {scenario} 장소 추천",
        candidate_place_ids=[str(rank_item.place_id)] if rank_item.place_id else [],
        ranking_version="v1",
        status="draft",
    )


def _topic_key(candidate: ArticleCandidate) -> str:
    return candidate.topic_key or f"{candidate.country.lower()}-{candidate.scenario}-{candidate.place_type}"


def _collect_featured_images(places: Any, payload: Mapping[str, Any]) -> list[str]:
    images: list[str] = []
    for place in places:
        if isinstance(place, Mapping):
            images.extend(_collect_image_urls(place))
    for section in payload.get("place_sections", []):
        if isinstance(section, Mapping):
            images.extend(_collect_image_urls(section))
    return list(dict.fromkeys([img for img in images if _is_http_url(img)]))


def _collect_image_urls(obj: Mapping[str, Any]) -> list[str]:
    raw_urls: list[str] = []
    for key in (
        "image_urls",
        "images",
        "photos",
        "photo",
        "photosUrl",
        "photoUrl",
        "imageUrl",
        "src",
        "url",
        "thumbnail",
        "thumbnails",
    ):
        value = obj.get(key)
        if value is None:
            continue
        if isinstance(value, str):
            raw_urls.append(value.strip())
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, str):
                    raw_urls.append(item.strip())
                elif isinstance(item, Mapping) and isinstance(item.get("url"), str):
                    raw_urls.append(item.get("url", "").strip())
                elif isinstance(item, Mapping):
                    for fallback_key in ("src", "photoUrl", "imageUrl", "thumbnail", "link", "href"):
                        fallback = item.get(fallback_key)
                        if isinstance(fallback, str):
                            raw_urls.append(fallback.strip())
    return list(dict.fromkeys(raw_urls))


def _collect_maps_url(obj: Mapping[str, Any]) -> str:
    for key in ("maps_url", "googleMapsUrl", "mapsUrl", "placeUrl", "url", "googleMaps", "mapUrl"):
        value = obj.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _infer_maps_embed_url(obj: Mapping[str, Any]) -> str:
    maps_url = _collect_maps_url(obj)
    if maps_url and "output=embed" in maps_url:
        return maps_url

    latitude = obj.get("lat") or obj.get("latitude") or (obj.get("location", {}).get("lat") if isinstance(obj.get("location"), Mapping) else None)
    longitude = obj.get("lng") or obj.get("longitude") or (obj.get("location", {}).get("lng") if isinstance(obj.get("location"), Mapping) else None)
    if _is_number(latitude) and _is_number(longitude):
        return f"https://maps.google.com/maps?q={latitude},{longitude}&output=embed"

    name = obj.get("name") or obj.get("title") or ""
    address = obj.get("address") or ""
    if name or address:
        query = f"{name} {address}".strip()
        return f"https://maps.google.com/maps?q={quote_plus(str(query))}&output=embed"

    if maps_url:
        return f"{maps_url}&output=embed" if "?" in maps_url else f"{maps_url}?output=embed"
    return ""


def _is_http_url(value: Any) -> bool:
    return isinstance(value, str) and value.lower().startswith(("http://", "https://"))


def _is_number(value: Any) -> bool:
    try:
        float(value)
        return True
    except (TypeError, ValueError):
        return False


def _ab_score(review: Mapping[str, Any]) -> int:
    base = int(review.get("score", 0) or 0)
    naturalness = int(review.get("naturalness_score", 0) or 0)
    ab_score = int(base * 0.75 + naturalness * 0.25)
    if not bool(review.get("pass", False)):
        ab_score -= 20
    return ab_score


def _format_article_content(payload: Mapping[str, Any]) -> str:
    from ..modules.generation.formatter import format_markdown_payload

    return format_markdown_payload(payload, include_map_iframe=True)


def _wait_for_run_result(apify: ApifyClient, run_id: str, timeout_seconds: int = 900) -> Dict[str, Any]:
    terminal_states = {"SUCCEEDED", "FAILED", "ABORTED", "TIMED_OUT", "CANCELLED"}
    deadline = time.time() + max(1, timeout_seconds)
    last_status = None

    while time.time() < deadline:
        run_data = _apify_payload(apify.get_actor_run(run_id))
        status = str(run_data.get("status", "")).upper()
        last_status = status
        if status in terminal_states:
            if status == "SUCCEEDED":
                return {"succeeded": True, "run": run_data}
            return {
                "succeeded": False,
                "run": run_data,
                "error": f"Apify run finished with status: {status}",
            }
        time.sleep(2)

    return {
        "succeeded": False,
        "run": {"status": last_status or "UNKNOWN"},
        "error": f"Apify run timeout after {timeout_seconds}s",
    }


def _run_apify_with_fallback(apify: ApifyClient, actor_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """Run Apify actor; retry with raw payload when wrapped input is rejected."""
    attempts: List[tuple[bool, Dict[str, Any]]] = []

    wrapped = apify.run_actor(actor_id, payload=payload, use_raw_payload=False)
    attempts.append((False, wrapped))
    settled = _wait_for_run_result(apify, _apify_payload(wrapped).get("id", ""), timeout_seconds=60)

    if settled.get("succeeded"):
        return settled

    run_data = settled.get("run", {})
    error_message = str(run_data.get("statusMessage", "")).lower()
    if "invalid input" in error_message and "searchstringsarray" in error_message:
        raw = apify.run_actor(actor_id, payload=payload, use_raw_payload=True)
        attempts.append((True, raw))
        retry_settled = _wait_for_run_result(apify, _apify_payload(raw).get("id", ""),  timeout_seconds=180)
        if retry_settled.get("succeeded"):
            return retry_settled
        return {
            "succeeded": False,
            "run": retry_settled.get("run", raw),
            "error": retry_settled.get("error", "Apify run did not succeed after fallback"),
            "attempts": attempts,
        }

    return {"succeeded": False, "run": run_data, "error": settled.get("error", "Apify run did not succeed."), "attempts": attempts}


def _apify_payload(payload: Any) -> Any:
    if not isinstance(payload, Mapping):
        return payload if isinstance(payload, list) else {}
    return payload.get("data", payload)


def _run_dataset_id(run_result: Mapping[str, Any]) -> Optional[str]:
    if not isinstance(run_result, Mapping):
        return None
    value = run_result.get("defaultDatasetId") or run_result.get("datasetId")
    text = str(value).strip() if value is not None else ""
    return text or None


def _resolve_dataset_ids(raw_value: Any) -> List[str]:
    if raw_value is None:
        return []
    if isinstance(raw_value, list):
        items = [str(item).strip() for item in raw_value]
        return [item for item in items if item]
    text = str(raw_value).replace(",", "\n")
    return [item.strip() for item in text.splitlines() if item.strip()]


def _build_apify_input(settings: Settings) -> Dict[str, Any]:
    search_strings = [s.strip() for s in (settings.apify_search_strings or "").split(",") if s.strip()]
    if not search_strings:
        search_strings = ["popular attractions"]

    default_search = search_strings[0]
    payload: Dict[str, Any] = {
        "searchString": default_search,
        "searchStringsArray": search_strings,
        "maxCrawledPlacesPerSearch": settings.apify_max_crawled_per_search,
        "maxCrawledPlaces": settings.apify_max_crawled_per_search,
        "language": settings.apify_language,
        "proxyConfig": {
            "useApifyProxy": True,
        },
    }

    if settings.apify_location_query:
        payload["locationQuery"] = settings.apify_location_query

    return payload


def _derive_business_status(places: Any) -> str:
    if not isinstance(places, list) or not places:
        return "unknown"
    statuses = []
    for item in places:
        if not isinstance(item, Mapping):
            continue
        status = str(item.get("business_status") or item.get("businessStatus") or "").strip().lower()
        if status:
            statuses.append(status)
    if not statuses:
        return "unknown"
    if all(status == statuses[0] for status in statuses):
        return statuses[0]
    return "mixed"


def _to_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _to_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _ensure_context(context: PipelineContext | None) -> PipelineContext:
    if context is not None:
        return context
    return PipelineContext()
