"""Job definitions for the automation pipeline."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
import html
import logging
import re
from urllib.parse import quote_plus, unquote, urlparse
from typing import Any, Callable, Dict, List, Mapping, Optional

import requests

from ..clients import GoogleMapScraperClient, GooglePlacesClient, OpenAIClient, WordPressClient
from ..config.settings import Settings
from ..modules.generation.formatter import build_post_featured_media_alt_text, build_post_meta_description
from ..modules.generation.topic_planner import TopicPlan, select_topic_plan
from ..modules.generation.seo import to_plain_text
from ..storage import PlaceRepository
from ..modules.generation import GenerationPipeline
from ..modules.refresh import RefreshPipeline
from ..modules.ranking import scorer
from ..modules.publish import PublishPipeline
from ..modules.publish.sitemap import verify_post_url_in_sitemap
from ..modules.review import ReviewPipeline
from ..shared.models import ArticleCandidate, PublishedArticle


logger = logging.getLogger(__name__)
_IMAGE_URL_VALIDATION_CACHE: dict[str, bool] = {}
DEFAULT_JAPAN_TOURIST_LOCATION_QUERIES: tuple[str, ...] = (
    "Tokyo, Japan",
    "Osaka, Japan",
    "Kyoto, Japan",
    "Hokkaido, Japan",
    "Fukuoka, Japan",
    "Hiroshima, Japan",
    "Okinawa, Japan",
    "Nara, Japan",
    "Kanazawa, Japan",
    "Nagoya, Japan",
    "Yokohama, Japan",
    "Kobe, Japan",
    "Sapporo, Japan",
    "Sendai, Japan",
    "Kamakura, Japan",
    "Hakone, Japan",
    "Nikko, Japan",
    "Takayama, Japan",
    "Matsumoto, Japan",
    "Beppu, Japan",
)
DEFAULT_TOURIST_SEARCH_STRINGS: tuple[str, ...] = (
    "tourist attractions",
    "sightseeing",
    "popular attractions",
    "landmarks",
    "viewpoints",
    "temples",
    "shrines",
    "castles",
    "museums",
    "scenic spots",
)


@dataclass(frozen=True)
class Job:
    name: str
    handler: Callable[..., Dict[str, Any]]


@dataclass
class PipelineContext:
    """In-memory runtime context for scheduled jobs."""

    settings: Settings = field(default_factory=Settings)
    google_map_scraper: Optional[GoogleMapScraperClient] = None
    google_places: Optional[GooglePlacesClient] = None
    openai: Optional[OpenAIClient] = None
    wp: Optional[WordPressClient] = None
    place_repo: Optional[PlaceRepository] = None

    raw_collections: List[Dict[str, Any]] = field(default_factory=list)
    article_candidates: List[ArticleCandidate] = field(default_factory=list)
    generated_articles: List[Dict[str, Any]] = field(default_factory=list)
    published_articles: List[PublishedArticle] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.google_map_scraper = self.google_map_scraper or _init_client("google_map_scraper", GoogleMapScraperClient)
        self.google_places = self.google_places or _init_client("google_places", GooglePlacesClient)
        self.openai = self.openai or _init_client("openai", OpenAIClient)
        self.wp = self.wp or _init_client("wordpress", WordPressClient)
        self.place_repo = self.place_repo or _init_client("place_repo", self._build_place_repository)

    def _build_place_repository(self) -> Optional[PlaceRepository]:
        db_url = self.settings.db_url
        if not db_url:
            return None
        return PlaceRepository(db_url)


@dataclass
class RecentPostSignature:
    post_id: int
    title: str
    slug: str
    link: str = ""
    title_tokens: set[str] = field(default_factory=set)
    place_keys: set[str] = field(default_factory=set)
    region_key: str = ""
    region_label: str = ""
    plan_key: str = ""
    title_family: str = ""
    title_family_label: str = ""
    content_angle_key: str = ""
    content_angle_label: str = ""
    audience_key: str = ""
    audience_label: str = ""
    duration_days: int = 0


@dataclass
class RegionCluster:
    region_key: str
    region_label: str
    ranked_items: List["scorer.RankItem"] = field(default_factory=list)


TITLE_TOKEN_STOPWORDS = {
    "trip",
    "travel",
    "itinerary",
    "guide",
    "plan",
    "route",
    "solo",
    "journey",
    "여행",
    "여행지",
    "일정",
    "추천",
    "가이드",
    "플랜",
    "동선",
    "코스",
    "명소",
    "정리",
    "핵심",
    "가볼만한",
    "가볼",
    "좋은",
    "떠나는",
    "즐기는",
    "만끽하는",
    "혼자",
    "혼자만의",
    "하루",
    "주말",
    "리스트",
}
TITLE_TOKEN_SUFFIXES = (
    "에서의",
    "에서",
    "으로의",
    "으로",
    "와의",
    "와",
    "과의",
    "과",
    "만의",
    "만",
    "에는",
    "에서만",
    "까지",
    "부터",
    "에게",
    "께",
    "의",
    "은",
    "는",
    "을",
    "를",
    "이",
    "가",
    "도",
)


def _init_client(name: str, constructor):
    try:
        return constructor()
    except Exception as exc:
        logger.warning("Could not initialize %s client: %s", name, exc)
        return None


def content_cycle_job(context: PipelineContext | None = None, scenario: str = "solo_travel") -> Dict[str, Any]:
    """Run generate -> review -> publish with freshly scraped place data."""
    ctx = _ensure_context(context)
    logger.info("content_cycle_job started scenario=%s", scenario)

    # Reset in-memory state so each scheduled cycle is driven by a fresh scraper run.
    ctx.raw_collections = []
    ctx.article_candidates = []
    ctx.generated_articles = []

    generate_result = generate_job(context=ctx, scenario=scenario)
    if generate_result.get("status") != "ok":
        result = {
            "job": "content_cycle",
            "status": generate_result.get("status", "error"),
            "generate": generate_result,
            "review": {"job": "review", "status": "skipped", "reason": "generation failed or skipped"},
            "publish": {"job": "publish", "status": "skipped", "reason": "generation failed or skipped"},
        }
        logger.info("content_cycle_job finished status=%s generate=%s", result["status"], generate_result.get("status"))
        return result

    review_result = review_job(context=ctx)
    if review_result.get("status") != "ok":
        result = {
            "job": "content_cycle",
            "status": review_result.get("status", "error"),
            "generate": generate_result,
            "review": review_result,
            "publish": {"job": "publish", "status": "skipped", "reason": "review failed or skipped"},
        }
        logger.info(
            "content_cycle_job finished status=%s generate=%s review=%s",
            result["status"],
            generate_result.get("status"),
            review_result.get("status"),
        )
        return result

    publish_result = publish_job(context=ctx)
    status = "ok" if publish_result.get("status") == "ok" else publish_result.get("status", "error")
    result = {
        "job": "content_cycle",
        "status": status,
        "generate": generate_result,
        "review": review_result,
        "publish": publish_result,
    }
    logger.info(
        "content_cycle_job finished status=%s generate=%s review=%s publish=%s published=%s",
        status,
        generate_result.get("status"),
        review_result.get("status"),
        publish_result.get("status"),
        len(publish_result.get("published", [])) if isinstance(publish_result.get("published"), list) else 0,
    )
    return result


def generate_job(context: PipelineContext | None = None, scenario: str = "solo_travel") -> Dict[str, Any]:
    """Scrape fresh places, persist them, then generate article candidates."""
    ctx = _ensure_context(context)
    logger.info("generate_job started")

    if ctx.google_map_scraper is None:
        return {
            "job": "generate",
            "status": "error",
            "reason": "google_map_scraper_not_configured",
        }

    collect_result = _force_collect_fresh_places(ctx, scenario=scenario)
    if collect_result.get("status") not in {"ok", "partial"}:
        return {
            "job": "generate",
            "status": "error",
            "reason": "place_collection_failed",
            "collect": collect_result,
        }
    if collect_result.get("status") == "partial":
        logger.warning("generate_job continuing with partial place collection: %s", collect_result.get("failed_locations", []))

    if not ctx.raw_collections:
        return {
            "job": "generate",
            "status": "skipped",
            "reason": "no collected raw data",
            "collect": collect_result,
        }

    if ctx.openai is None:
        return {"job": "generate", "status": "error", "error": "OpenAI client is not configured"}

    try:
        target_count = max(1, min(ctx.settings.recent_place_target_count, ctx.settings.top_n_candidates))
        min_count = max(2, ctx.settings.recent_place_min_count)
        excluded_keys = _load_recent_excluded_place_keys(ctx)
        filtered_collections = _filter_places_by_excluded_keys(ctx.raw_collections, excluded_keys)

        if len(filtered_collections) < min_count:
            return {
                "job": "generate",
                "status": "skipped",
                "reason": "insufficient_unique_places",
                "excluded_place_count": len(excluded_keys),
                "available_count": len(filtered_collections),
                "collect": collect_result,
            }

        ctx.raw_collections = filtered_collections
        normalized_places = [_normalize_place(item) for item in filtered_collections]
        ranked = scorer.score_candidates(normalized_places, scenario=scenario)
        recent_signatures = _load_recent_post_signatures(ctx)
        selected_cluster = _select_region_cluster(
            ranked_items=ranked,
            recent_signatures=recent_signatures,
            target_count=target_count,
            min_count=min_count,
            title_threshold=ctx.settings.recent_title_token_threshold,
        )

        if selected_cluster is None:
            return {
                "job": "generate",
                "status": "skipped_duplicate_topic",
                "reason": "recent_region_or_title_conflict",
                "excluded_place_count": len(excluded_keys),
                "available_count": len(filtered_collections),
                "recent_post_count": len(recent_signatures),
                "collect": collect_result,
            }

        top = selected_cluster.ranked_items[:target_count]
        top_payloads = [item.payload for item in top]
        region = selected_cluster.region_label or _place_region_label(top_payloads[0]) or "Tokyo"
        topic_plan = select_topic_plan(
            recent_signatures=recent_signatures,
            region_key=selected_cluster.region_key,
            scenario=scenario,
        )

        if not top_payloads:
            return {"job": "generate", "status": "skipped", "reason": "no ranked candidates"}

        ctx.article_candidates = [
            _candidate_from_ranking(
                item,
                scenario=scenario,
                city=region,
                country=item.payload.get("country", ""),
                audience=topic_plan.audience_key,
                content_angle=topic_plan.content_angle_label,
                duration_days=topic_plan.duration_days,
            )
            for item in top
        ]

        # generate two variants for A/B selection
        candidate_payload = top_payloads
        generator = GenerationPipeline(openai_client=ctx.openai, scenario=scenario, max_sections=min(len(candidate_payload), 6))
        variants = []
        for variant_id, tone in [("A", "friendly"), ("B", "warm")]:
            article = generator.generate_article(
                places=candidate_payload,
                region=region,
                duration_days=topic_plan.duration_days,
                tone=tone,
                extra_context={
                    "scenario": scenario,
                    "variant_id": variant_id,
                    **topic_plan.to_context(),
                },
            )
            payload = article.to_payload()
            payload["variant_id"] = variant_id
            payload["generation_tone"] = tone
            payload["region"] = region
            payload["region_key"] = selected_cluster.region_key
            payload["scenario"] = scenario
            payload["topic_plan_key"] = topic_plan.plan_key
            payload["topic_plan"] = topic_plan.to_payload()
            payload["title_family"] = topic_plan.title_family
            payload["title_family_label"] = topic_plan.title_family_label
            payload["content_angle"] = topic_plan.content_angle_label
            payload["content_angle_key"] = topic_plan.content_angle_key
            payload["audience"] = topic_plan.audience_label
            payload["audience_key"] = topic_plan.audience_key
            payload["duration_days"] = topic_plan.duration_days
            payload["title_hook"] = topic_plan.title_hook
            variants.append({"variant_id": variant_id, "payload": payload})

        ctx.generated_articles = [
            {
                "candidate_topic": _topic_key(ctx.article_candidates[0]),
                "region_key": selected_cluster.region_key,
                "topic_plan": topic_plan.to_payload(),
                "variants": variants,
                "payload": variants[0]["payload"],
                "places": top_payloads,
            }
        ]
        return {
            "job": "generate",
            "status": "ok",
            "generated": len(ctx.generated_articles),
            "excluded_place_count": len(excluded_keys),
            "selected_place_count": len(top_payloads),
            "selected_region_key": selected_cluster.region_key,
            "topic_plan_key": topic_plan.plan_key,
            "collect": collect_result,
        }
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
        recent_signatures = _load_recent_post_signatures(ctx)
        duplicate_skips = 0
        for generated in ctx.generated_articles:
            places = generated.get("places") or []
            review = generated.get("review") or {}
            status = "pending_review"
            if review and review.get("pass", False):
                status = "publish"

            payload = generated["payload"]
            title = payload.get("title") or "Untitled"
            region_key = _payload_region_key(payload, places)
            duplicate_match, duplicate_reason = _find_recent_duplicate_signature(
                title=title,
                region_key=region_key,
                recent_signatures=recent_signatures,
                threshold=ctx.settings.recent_title_token_threshold,
                topic_metadata=_extract_topic_metadata(payload),
            )
            if duplicate_match is not None:
                duplicate_skips += 1
                logger.info(
                    "publish_job skipped duplicate topic title=%s region_key=%s reason=%s recent_post_id=%s",
                    title,
                    region_key,
                    duplicate_reason,
                    duplicate_match.post_id,
                )
                published.append(
                    {
                        "post_id": None,
                        "status": "skipped_duplicate_topic",
                        "reason": duplicate_reason,
                        "title": title,
                        "recent_post_id": duplicate_match.post_id,
                    }
                )
                continue

            seo = payload.get("seo") if isinstance(payload.get("seo"), Mapping) else {}
            taxonomy_terms = _build_public_taxonomy_terms(payload, generated, ctx.settings)
            term_ids = publisher.resolve_term_ids(
                categories=taxonomy_terms["categories"],
                tags=taxonomy_terms["tags"],
            )
            payload = dict(payload)
            payload = _sanitize_article_payload_images(payload, ctx.settings)
            internal_links = _select_internal_links(
                ctx.wp,
                region_tag_id=_resolved_term_id(
                    taxonomy_terms["tags"],
                    term_ids["tags"],
                    taxonomy_terms.get("region_tag", ""),
                ),
                content_tag_id=_resolved_term_id(
                    taxonomy_terms["tags"],
                    term_ids["tags"],
                    taxonomy_terms.get("content_tag", ""),
                ),
                exclude_title=title,
            )
            payload["internal_links"] = internal_links
            payload["related_posts"] = internal_links.get("same_region", []) + internal_links.get("same_category", [])
            content_html = _format_article_content(payload)
            featured_media_urls = _collect_featured_images(generated.get("places", []), payload, ctx.settings)
            excerpt = build_post_meta_description(payload)
            featured_media_alt_text = build_post_featured_media_alt_text(payload)
            result = publisher.publish(
                title=title,
                content=content_html,
                status=status,
                excerpt=excerpt,
                featured_media_urls=featured_media_urls,
                featured_media_alt_text=featured_media_alt_text,
                categories=term_ids["categories"],
                tags=term_ids["tags"],
                meta_fields=_build_wp_meta_fields(payload, ctx.settings),
                dry_run=False,
            )
            sitemap_verification: Dict[str, Any] = {}
            if result.get("actual_status") == "publish" and result.get("post_url") and ctx.settings.wordpress_base_url:
                sitemap_verification = verify_post_url_in_sitemap(
                    ctx.settings.wordpress_base_url,
                    str(result.get("post_url")),
                ).to_payload()
                if not sitemap_verification.get("found", False):
                    logger.warning(
                        "published post missing from sitemap post_id=%s url=%s checked=%s error=%s",
                        result.get("post_id"),
                        result.get("post_url"),
                        sitemap_verification.get("checked_urls", []),
                        sitemap_verification.get("error", ""),
                    )
            if sitemap_verification:
                result["sitemap_verification"] = sitemap_verification

            primary_place_db_id = _first_place_db_id(places)
            if ctx.place_repo is not None and primary_place_db_id is not None:
                db_status = _map_wp_status_to_db_status(result.get("actual_status", status))
                raw_publish_response = dict(result)
                raw_publish_response["candidate_place_ids"] = _candidate_place_keys(places, payload)
                raw_publish_response["candidate_db_place_ids"] = _candidate_db_place_ids(places)
                raw_publish_response["place_snapshots"] = places if isinstance(places, list) else []
                raw_publish_response["selected_variant_id"] = generated.get("selected_variant_id")
                raw_publish_response["review"] = review
                raw_publish_response["region_key"] = region_key
                raw_publish_response["dedupe_title_tokens"] = sorted(_title_tokens(title, result.get("slug", "")))
                raw_publish_response["topic_plan"] = _extract_topic_metadata(payload)
                saved = ctx.place_repo.save_published_article(
                    primary_place_id=primary_place_db_id,
                    wp_post_id=int(result.get("post_id") or 0),
                    title=title,
                    slug=result.get("slug", ""),
                    content_html=content_html,
                    excerpt=excerpt,
                    status=db_status,
                    raw_publish_response=raw_publish_response,
                    media_urls=result.get("featured_media_ids", []),
                    categories=result.get("term_ids", {}).get("categories", []),
                    tags=[str(tag) for tag in taxonomy_terms["tags"] if str(tag).strip()],
                    published_at=datetime.now(timezone.utc) if result.get("actual_status") == "publish" else None,
                )
                if not saved:
                    logger.warning("publish_job could not persist published_article for wp_post_id=%s", result.get("post_id"))

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
            recent_signatures.append(
                RecentPostSignature(
                    post_id=int(result.get("post_id") or 0),
                    title=title,
                    slug=str(result.get("slug", "")),
                    link=str(result.get("post_url", "")),
                    title_tokens=_title_tokens(title, str(result.get("slug", ""))),
                    place_keys=set(_candidate_place_keys(places, payload)),
                    region_key=region_key,
                    region_label=str(payload.get("region", "")),
                    plan_key=str(payload.get("topic_plan_key") or _extract_topic_metadata(payload).get("plan_key", "")),
                    title_family=str(payload.get("title_family", "")),
                    title_family_label=str(payload.get("title_family_label", "")),
                    content_angle_key=str(payload.get("content_angle_key", "")),
                    content_angle_label=str(payload.get("content_angle", "")),
                    audience_key=str(payload.get("audience_key", "")),
                    audience_label=str(payload.get("audience", "")),
                    duration_days=_coerce_positive_int(payload.get("duration_days")),
                )
            )

        final_status = "ok"
        if duplicate_skips and not any(item.get("post_id") for item in published):
            final_status = "skipped_duplicate_topic"
        elif duplicate_skips:
            final_status = "partial"
        return {"job": "publish", "status": final_status, "published": published}
    except Exception as exc:
        logger.error("publish_job failed: %s", exc)
        return {"job": "publish", "status": "error", "error": str(exc)}


def _load_recent_excluded_place_keys(ctx: PipelineContext) -> set[str]:
    limit = max(ctx.settings.recent_published_exclude_count, 0)
    if limit <= 0:
        return set()

    keys: set[str] = set()
    if ctx.place_repo is not None:
        try:
            keys.update(ctx.place_repo.fetch_recent_published_place_keys(limit=limit, status="published"))
        except Exception as exc:
            logger.warning("could not load recent published place keys from DB: %s", exc)

    if ctx.wp is not None:
        try:
            posts = ctx.wp.list_posts(per_page=limit, orderby="date", order="desc", status="publish")
            keys.update(_extract_place_keys_from_wp_posts(posts))
        except Exception as exc:
            logger.warning("could not load recent published posts from WordPress: %s", exc)

    return {str(value).strip() for value in keys if str(value).strip()}


def _load_recent_post_signatures(ctx: PipelineContext) -> list[RecentPostSignature]:
    limit = max(ctx.settings.recent_published_exclude_count, 0)
    if limit <= 0 or ctx.wp is None:
        return []

    try:
        posts = ctx.wp.list_posts(per_page=limit, orderby="date", order="desc", status="publish")
    except Exception as exc:
        logger.warning("could not load recent post signatures from WordPress: %s", exc)
        return []

    return _build_recent_post_signatures(posts, ctx.place_repo)


def _build_recent_post_signatures(
    posts: Any,
    place_repo: PlaceRepository | None = None,
) -> list[RecentPostSignature]:
    if not isinstance(posts, list):
        return []

    place_rows_by_key: dict[str, Mapping[str, Any]] = {}
    all_keys: set[str] = set()
    post_ids: list[int] = []
    for post in posts:
        if not isinstance(post, Mapping):
            continue
        post_id = post.get("id")
        if isinstance(post_id, int) and post_id > 0:
            post_ids.append(post_id)
        content = (post.get("content") or {}).get("rendered", "")
        all_keys.update(_extract_place_key_sequence_from_wp_content(content))

    if place_repo is not None and all_keys:
        try:
            for row in place_repo.fetch_place_summaries_by_keys(sorted(all_keys)):
                if not isinstance(row, Mapping):
                    continue
                for key in (row.get("external_place_id"), row.get("google_place_id")):
                    normalized = str(key).strip() if key is not None else ""
                    if normalized and normalized not in place_rows_by_key:
                        place_rows_by_key[normalized] = row
        except Exception as exc:
            logger.warning("could not resolve recent post place keys to place rows: %s", exc)

    topic_metadata_by_post_id: dict[int, Mapping[str, Any]] = {}
    if place_repo is not None and post_ids:
        try:
            topic_metadata_by_post_id = place_repo.fetch_published_topic_metadata_by_post_ids(post_ids, status="published")
        except Exception as exc:
            logger.warning("could not resolve recent post topic metadata from DB: %s", exc)

    signatures: list[RecentPostSignature] = []
    for post in posts:
        if not isinstance(post, Mapping):
            continue
        post_id = post.get("id")
        if not isinstance(post_id, int):
            continue
        title = to_plain_text((post.get("title") or {}).get("rendered") if isinstance(post.get("title"), Mapping) else post.get("title"))
        slug = to_plain_text(post.get("slug"))
        link = to_plain_text(post.get("link"))
        content = (post.get("content") or {}).get("rendered", "")
        place_key_sequence = _extract_place_key_sequence_from_wp_content(content)
        place_keys = set(place_key_sequence)
        region_key, region_label = _infer_recent_post_region(place_key_sequence, title, slug, place_rows_by_key)
        topic_metadata = _extract_topic_metadata(topic_metadata_by_post_id.get(post_id, {}))
        signatures.append(
            RecentPostSignature(
                post_id=post_id,
                title=title,
                slug=slug,
                link=link,
                title_tokens=_title_tokens(title, slug),
                place_keys=place_keys,
                region_key=region_key,
                region_label=region_label,
                plan_key=str(topic_metadata.get("plan_key", "")),
                title_family=str(topic_metadata.get("title_family", "")),
                title_family_label=str(topic_metadata.get("title_family_label", "")),
                content_angle_key=str(topic_metadata.get("content_angle_key", "")),
                content_angle_label=str(topic_metadata.get("content_angle_label", "")),
                audience_key=str(topic_metadata.get("audience_key", "")),
                audience_label=str(topic_metadata.get("audience_label", "")),
                duration_days=_coerce_positive_int(topic_metadata.get("duration_days")),
            )
        )

    return signatures


def _infer_recent_post_region(
    place_keys: list[str],
    title: str,
    slug: str,
    place_rows_by_key: Mapping[str, Mapping[str, Any]],
) -> tuple[str, str]:
    rows = [place_rows_by_key[key] for key in place_keys if key in place_rows_by_key]
    if not rows:
        return "", ""

    title_tokens = _title_tokens(title, slug)
    region_counts: Counter[str] = Counter()
    label_by_key: dict[str, str] = {}
    similarity_by_key: dict[str, float] = {}
    first_region_key = ""

    for row in rows:
        region_label = _place_region_label(row)
        region_key = _normalize_region_key(region_label)
        if not region_key:
            continue
        if not first_region_key:
            first_region_key = region_key
        region_counts[region_key] += 1
        label_by_key.setdefault(region_key, region_label)
        similarity_by_key[region_key] = max(
            similarity_by_key.get(region_key, 0.0),
            _token_overlap(title_tokens, _title_tokens(region_label)),
        )

    if not region_counts:
        return "", ""

    selected_key = max(
        region_counts,
        key=lambda key: (
            key == first_region_key,
            similarity_by_key.get(key, 0.0),
            region_counts[key],
            len(label_by_key.get(key, "")),
        ),
    )
    return selected_key, label_by_key.get(selected_key, "")


def _select_region_cluster(
    ranked_items: list["scorer.RankItem"],
    recent_signatures: list[RecentPostSignature],
    target_count: int,
    min_count: int,
    title_threshold: float,
) -> RegionCluster | None:
    clusters: dict[str, RegionCluster] = {}
    for item in ranked_items:
        region_label = _place_region_label(item.payload)
        region_key = _normalize_region_key(region_label)
        if not region_key:
            continue
        cluster = clusters.setdefault(region_key, RegionCluster(region_key=region_key, region_label=region_label))
        cluster.ranked_items.append(item)

    recent_region_counts = Counter(signature.region_key for signature in recent_signatures if signature.region_key)
    ordered = sorted(
        clusters.values(),
        key=lambda cluster: _cluster_selection_score(
            cluster=cluster,
            recent_signatures=recent_signatures,
            recent_region_counts=recent_region_counts,
            target_count=target_count,
            title_threshold=title_threshold,
        ),
        reverse=True,
    )
    for cluster in ordered:
        if len(cluster.ranked_items) < min_count:
            continue
        return cluster
    return None


def _cluster_selection_score(
    cluster: RegionCluster,
    recent_signatures: list[RecentPostSignature],
    recent_region_counts: Counter[str],
    target_count: int,
    title_threshold: float,
) -> float:
    base_score = sum(item.score for item in cluster.ranked_items[:target_count])
    recent_region_penalty = float(recent_region_counts.get(cluster.region_key, 0)) * 25.0
    title_overlap_penalty = _cluster_title_overlap_penalty(cluster, recent_signatures, title_threshold)
    return base_score - recent_region_penalty - title_overlap_penalty


def _cluster_title_overlap_penalty(
    cluster: RegionCluster,
    recent_signatures: list[RecentPostSignature],
    title_threshold: float,
) -> float:
    region_tokens = _title_tokens(cluster.region_label)
    if not region_tokens:
        return 0.0

    max_overlap = 0.0
    for signature in recent_signatures:
        if not signature.title_tokens:
            continue
        overlap = _token_overlap(region_tokens, signature.title_tokens)
        if overlap >= title_threshold:
            max_overlap = max(max_overlap, overlap)
    return max_overlap * 30.0


def _find_recent_duplicate_signature(
    title: str,
    region_key: str,
    recent_signatures: list[RecentPostSignature],
    threshold: float,
    topic_metadata: Mapping[str, Any] | None = None,
) -> tuple[RecentPostSignature | None, str]:
    normalized_region_key = _normalize_region_key(region_key)
    if normalized_region_key:
        for signature in recent_signatures:
            if signature.region_key and signature.region_key == normalized_region_key:
                return signature, "recent_region"

    title_tokens = _title_tokens(title)
    normalized_topic = _extract_topic_metadata(topic_metadata or {})
    for signature in recent_signatures:
        if not title_tokens or not signature.title_tokens:
            continue
        overlap = _token_overlap(title_tokens, signature.title_tokens)
        if overlap < threshold:
            continue
        if _topic_signature_matches(normalized_topic, signature):
            return signature, "recent_topic_signature"
        if not signature.plan_key and not signature.title_family and not signature.content_angle_key:
            return signature, "recent_title_similarity"
    return None, ""


def _payload_region_key(payload: Mapping[str, Any], places: Any) -> str:
    region_key = _normalize_region_key(payload.get("region_key") or payload.get("region"))
    if region_key:
        return region_key
    if isinstance(places, list):
        for item in places:
            if isinstance(item, Mapping):
                region_key = _normalize_region_key(_place_region_label(item))
                if region_key:
                    return region_key
    return ""


def _extract_place_keys_from_wp_posts(posts: Any) -> set[str]:
    if not isinstance(posts, list):
        return set()

    keys: set[str] = set()
    for post in posts:
        if not isinstance(post, Mapping):
            continue
        content = (post.get("content") or {}).get("rendered", "")
        keys.update(_extract_place_keys_from_wp_content(content))
    return keys


def _extract_place_keys_from_wp_content(content: Any) -> set[str]:
    return set(_extract_place_key_sequence_from_wp_content(content))


def _extract_place_key_sequence_from_wp_content(content: Any) -> list[str]:
    if not content:
        return []
    normalized = html.unescape(str(content))
    matches = re.findall(r"query_place_id=([^&\"'<\s]+)", normalized)
    return list(dict.fromkeys([match.strip() for match in matches if match.strip()]))


def _filter_places_by_excluded_keys(items: list[dict[str, Any]], excluded_keys: set[str]) -> list[dict[str, Any]]:
    if not excluded_keys:
        return list(items)

    filtered: list[dict[str, Any]] = []
    skipped = 0
    for item in items:
        item_keys = _place_keys(item)
        if item_keys and item_keys.intersection(excluded_keys):
            skipped += 1
            continue
        filtered.append(item)

    logger.info("generate_job filtered %s places using %s recent published keys", skipped, len(excluded_keys))
    return filtered


def _place_keys(item: Mapping[str, Any]) -> set[str]:
    keys: set[str] = set()
    raw_payload = item.get("raw_payload") or {}
    if not isinstance(raw_payload, Mapping):
        raw_payload = {}

    for value in (
        item.get("id"),
        item.get("place_id"),
        item.get("source_id"),
        item.get("external_place_id"),
        item.get("google_place_id"),
        raw_payload.get("id"),
        raw_payload.get("place_id"),
        raw_payload.get("placeId"),
        raw_payload.get("google_place_id"),
        raw_payload.get("googlePlaceId"),
    ):
        if value is None:
            continue
        normalized = str(value).strip()
        if normalized:
            keys.add(normalized)
    return keys


def _place_region_label(item: Mapping[str, Any]) -> str:
    raw_payload = item.get("raw_payload") or {}
    if not isinstance(raw_payload, Mapping):
        raw_payload = {}

    candidates = [
        item.get("city"),
        item.get("region"),
        item.get("locality"),
        item.get("state"),
        raw_payload.get("locality"),
        raw_payload.get("city"),
        raw_payload.get("region"),
        raw_payload.get("state"),
        _address_locality(item.get("address")),
        item.get("country"),
        raw_payload.get("country"),
    ]
    for value in candidates:
        cleaned = _clean_region_text(value)
        if cleaned:
            return cleaned
    return ""


def _address_locality(address: Any) -> str:
    text = to_plain_text(address)
    if not text:
        return ""

    parts = [part.strip() for part in text.split(",") if part and part.strip()]
    middle_parts = parts[1:-1] if len(parts) > 2 else parts
    for pool in (middle_parts, parts):
        for part in pool:
            cleaned = _clean_region_text(part)
            if cleaned:
                return cleaned
    return ""


def _clean_region_text(value: Any) -> str:
    text = to_plain_text(value)
    if not text:
        return ""
    text = re.sub(r"〒\s*\d{3}-\d{4}", " ", text)
    text = re.sub(r"\b\d{3}-\d{4}\b", " ", text)
    text = re.sub(r"\b\d+[A-Za-z\-]*\b", " ", text)
    text = re.sub(r"\s+", " ", text).strip(" ,")
    lowered = text.lower()
    if lowered in {"japan", "jp", "일본"}:
        return ""
    return text


def _normalize_region_key(value: Any) -> str:
    text = _clean_region_text(value)
    if not text:
        return ""
    text = re.sub(r"[^0-9a-zA-Z가-힣一-龥ぁ-ゔァ-ヴー々〆〤]+", " ", text.lower())
    return re.sub(r"\s+", " ", text).strip()


def _title_tokens(title: Any, slug: str = "") -> set[str]:
    title_text = to_plain_text(title)
    slug_text = to_plain_text(unquote(slug).replace("-", " "))
    combined = f"{title_text} {slug_text}".strip().lower()
    if not combined:
        return set()
    normalized_tokens: set[str] = set()
    for raw in re.findall(r"[0-9a-zA-Z가-힣一-龥ぁ-ゔァ-ヴー々〆〤]+", combined):
        token = _normalize_title_token(raw)
        if token:
            normalized_tokens.add(token)
    return normalized_tokens


def _token_overlap(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    return len(left.intersection(right)) / max(1, min(len(left), len(right)))


def _normalize_title_token(token: str) -> str:
    normalized = str(token or "").strip().lower()
    if not normalized:
        return ""

    for suffix in TITLE_TOKEN_SUFFIXES:
        while normalized.endswith(suffix) and len(normalized) > len(suffix) + 1:
            normalized = normalized[: -len(suffix)].strip()

    if not normalized:
        return ""
    if normalized in TITLE_TOKEN_STOPWORDS:
        return ""
    if re.fullmatch(r"\d+일", normalized):
        return ""
    if re.fullmatch(r"\d+일간", normalized):
        return ""
    if re.fullmatch(r"\d+일간의", normalized):
        return ""
    if re.fullmatch(r"\d+박\d+일", normalized):
        return ""
    if re.fullmatch(r"\d+곳", normalized):
        return ""
    if re.fullmatch(r"\d+선", normalized):
        return ""
    if len(normalized) <= 1 and not normalized.isdigit():
        return ""
    return normalized


def _extract_topic_metadata(payload: Mapping[str, Any]) -> dict[str, Any]:
    topic_plan = payload.get("topic_plan") if isinstance(payload, Mapping) else {}
    if not isinstance(topic_plan, Mapping):
        topic_plan = {}

    def _value(*names: str) -> str:
        for name in names:
            if name in topic_plan:
                text = to_plain_text(topic_plan.get(name))
            else:
                text = to_plain_text(payload.get(name)) if isinstance(payload, Mapping) else ""
            if text:
                return text
        return ""

    return {
        "plan_key": _value("plan_key", "topic_plan_key"),
        "title_family": _value("title_family"),
        "title_family_label": _value("title_family_label"),
        "content_angle_key": _value("content_angle_key"),
        "content_angle_label": _value("content_angle_label", "content_angle"),
        "audience_key": _value("audience_key"),
        "audience_label": _value("audience_label", "audience"),
        "duration_days": _coerce_positive_int(topic_plan.get("duration_days") or payload.get("duration_days")),
        "title_hook": _value("title_hook"),
    }


def _topic_signature_matches(topic_metadata: Mapping[str, Any], signature: RecentPostSignature) -> bool:
    if not topic_metadata:
        return False

    matches = 0
    if topic_metadata.get("title_family") and topic_metadata.get("title_family") == signature.title_family:
        matches += 1
    if topic_metadata.get("content_angle_key") and topic_metadata.get("content_angle_key") == signature.content_angle_key:
        matches += 1
    if topic_metadata.get("audience_key") and topic_metadata.get("audience_key") == signature.audience_key:
        matches += 1
    if topic_metadata.get("duration_days") and int(topic_metadata.get("duration_days") or 0) == signature.duration_days:
        matches += 1
    return matches >= 2


def _coerce_positive_int(value: Any) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return 0
    return parsed if parsed > 0 else 0


def _force_collect_fresh_places(ctx: PipelineContext, scenario: str) -> Dict[str, Any]:
    if ctx.google_map_scraper is None:
        return {"status": "skipped", "reason": "google-map-scraper not configured"}

    location_queries = _resolve_place_collect_location_queries(ctx.settings)
    if not location_queries:
        return {"status": "error", "error": "No place collection locations configured"}

    aggregated_items: list[dict[str, Any]] = []
    run_results: list[dict[str, Any]] = []
    failed_locations: list[str] = []
    try:
        for location_query in location_queries:
            payload = _build_google_map_scraper_request(ctx.settings, location_query=location_query)
            try:
                scrape_result = ctx.google_map_scraper.scrape_places(**payload)
            except Exception as exc:
                failed_locations.append(location_query)
                logger.warning("force fresh google-map-scraper collection failed location=%s error=%s", location_query, exc)
                run_results.append(
                    {
                        "location_query": location_query,
                        "status": "error",
                        "error": str(exc),
                        "query_count": len(payload.get("search_strings", [])),
                        "queries": payload.get("search_strings", []),
                    }
                )
                continue

            items = scrape_result.get("items", []) if isinstance(scrape_result, Mapping) else []
            if not isinstance(items, list):
                failed_locations.append(location_query)
                run_results.append(
                    {
                        "location_query": location_query,
                        "status": "error",
                        "error": "Unexpected scraper items format",
                        "query_count": len(payload.get("search_strings", [])),
                        "queries": payload.get("search_strings", []),
                    }
                )
                continue

            aggregated_items.extend(items)
            run_results.append(
                {
                    "location_query": location_query,
                    "status": "ok",
                    "query_count": len(payload.get("search_strings", [])),
                    "queries": payload.get("search_strings", []),
                    **(scrape_result.get("meta", {}) if isinstance(scrape_result, Mapping) else {}),
                }
            )

        if not aggregated_items:
            return {
                "status": "error",
                "error": "No place results returned from any configured location",
                "run_result": {"locations": run_results},
            }

        upsert_summary: Dict[str, Any] = {}
        if ctx.place_repo is not None:
            upsert = ctx.place_repo.upsert_places(
                aggregated_items,
                source="google_map_scraper",
                conflict_mode="update",
            )
            upsert_summary = {
                "fetched_count": upsert.fetched_count,
                "inserted_count": upsert.inserted_count,
                "skipped_count": upsert.skipped_count,
                "errors": upsert.errors,
            }
        ctx.raw_collections = list(aggregated_items)

        status = "ok" if not failed_locations else "partial"
        return {
            "status": status,
            "count": len(ctx.raw_collections),
            "locations_processed": len(location_queries),
            "failed_locations": failed_locations,
            "run_result": {"locations": run_results},
            "upsert": upsert_summary,
        }
    except Exception as exc:
        logger.warning("force fresh google-map-scraper collection failed: %s", exc)
        return {"status": "error", "error": str(exc), "run_result": {"locations": run_results}}


def _first_place_db_id(places: Any) -> int | None:
    if not isinstance(places, list):
        return None
    for item in places:
        if not isinstance(item, Mapping):
            continue
        value = item.get("id")
        if isinstance(value, int) and value > 0:
            return value
        try:
            parsed = int(str(value).strip())
        except (TypeError, ValueError):
            continue
        if parsed > 0:
            return parsed
    return None


def _candidate_place_keys(places: Any, payload: Mapping[str, Any]) -> list[str]:
    keys: list[str] = []
    seen: set[str] = set()

    if isinstance(places, list):
        for item in places:
            if not isinstance(item, Mapping):
                continue
            for key in _place_keys(item):
                if key not in seen:
                    seen.add(key)
                    keys.append(key)

    for section in payload.get("place_sections", []):
        if not isinstance(section, Mapping):
            continue
        value = section.get("place_id") or section.get("id")
        if value is None:
            continue
        normalized = str(value).strip()
        if normalized and normalized not in seen:
            seen.add(normalized)
            keys.append(normalized)

    return keys


def _candidate_db_place_ids(places: Any) -> list[int]:
    ids: list[int] = []
    seen: set[int] = set()
    if not isinstance(places, list):
        return ids

    for item in places:
        if not isinstance(item, Mapping):
            continue
        value = item.get("id")
        try:
            parsed = int(str(value).strip())
        except (TypeError, ValueError):
            continue
        if parsed > 0 and parsed not in seen:
            seen.add(parsed)
            ids.append(parsed)
    return ids


def _map_wp_status_to_db_status(status: str) -> str:
    normalized = str(status or "").strip().lower()
    if normalized == "publish":
        return "published"
    if normalized in {"future", "pending"}:
        return "scheduled"
    if normalized == "draft":
        return "draft"
    return "failed"


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
    raw_payload = payload.get("raw_payload") or {}
    if not isinstance(raw_payload, Mapping):
        raw_payload = {}
    payload.setdefault("id", payload.get("place_id") or payload.get("placeId") or payload.get("id"))
    payload.setdefault("rating", _to_float(payload.get("rating", 0.0)))
    payload.setdefault("review_count", _to_int(payload.get("user_ratings_total", payload.get("review_count", 0))))
    payload.setdefault("name", payload.get("name", ""))
    payload.setdefault("city", payload.get("city") or payload.get("region") or raw_payload.get("city") or raw_payload.get("locality") or "")
    payload.setdefault("region", payload.get("region") or raw_payload.get("region") or raw_payload.get("state") or "")
    payload.setdefault("locality", payload.get("locality") or raw_payload.get("locality") or raw_payload.get("city") or "")
    payload.setdefault("state", payload.get("state") or raw_payload.get("state") or raw_payload.get("region") or "")
    payload.setdefault("country", payload.get("country", ""))
    payload.setdefault("business_status", payload.get("business_status", "unknown"))
    payload.setdefault("image_urls", _collect_image_urls(payload))
    payload.setdefault("maps_url", _collect_maps_url(payload))
    payload.setdefault("maps_embed_url", _infer_maps_embed_url(payload))
    return payload


def _parse_location(raw: str) -> tuple[str, str]:
    if not raw:
        return "", ""
    parts = [segment.strip() for segment in raw.split(",") if segment.strip()]
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[1]


def _dedupe_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        normalized = str(value or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def _candidate_from_ranking(
    rank_item: "scorer.RankItem",
    scenario: str,
    city: str,
    country: str,
    audience: str = "korean_traveler",
    content_angle: str = "",
    duration_days: int = 0,
) -> ArticleCandidate:
    normalized_country = country.strip().lower() if isinstance(country, str) and country.strip() else "unknown"
    normalized_place_type = _normalize_rank_item_place_type(rank_item.payload)
    duration_hint = f" {duration_days}일" if duration_days > 0 else ""
    angle_hint = f" {content_angle}" if content_angle else ""
    return ArticleCandidate(
        topic_key=f"{normalized_country}-{scenario}-{normalized_place_type}",
        city=city,
        country=country,
        scenario=scenario,
        place_type=normalized_place_type,
        audience=audience or "korean_traveler",
        query_text=f"{city or country} {scenario}{duration_hint}{angle_hint} 장소 추천".strip(),
        candidate_place_ids=[str(rank_item.place_id)] if rank_item.place_id else [],
        ranking_version="v1",
        status="draft",
    )


def _normalize_rank_item_place_type(payload: Mapping[str, Any]) -> str:
    for value in (payload.get("place_type"), payload.get("category")):
        normalized = _normalize_place_type_value(value)
        if normalized:
            return normalized
    return "general"


def _normalize_place_type_value(value: Any) -> str:
    if isinstance(value, str):
        normalized = value.strip()
        return normalized or ""
    if isinstance(value, Mapping):
        for key in ("name", "slug", "type", "category"):
            normalized = _normalize_place_type_value(value.get(key))
            if normalized:
                return normalized
        return ""
    if isinstance(value, (list, tuple, set)):
        for item in value:
            normalized = _normalize_place_type_value(item)
            if normalized:
                return normalized
        return ""
    if value is None or isinstance(value, bool):
        return ""
    normalized = str(value).strip()
    return normalized or ""


def _topic_key(candidate: ArticleCandidate) -> str:
    return candidate.topic_key or f"{candidate.country.lower()}-{candidate.scenario}-{candidate.place_type}"


def _select_related_posts(
    wp: WordPressClient,
    category_ids: list[int],
    tag_ids: list[int],
    exclude_title: str = "",
    exclude_slug: str = "",
    limit: int = 3,
) -> list[dict[str, str]]:
    selected: list[dict[str, str]] = []
    seen: set[str] = set()
    normalized_title = to_plain_text(exclude_title).lower()
    normalized_slug = to_plain_text(exclude_slug)

    queries: list[dict[str, Any]] = []
    if category_ids and tag_ids:
        queries.append({"categories": ",".join(str(item) for item in category_ids), "tags": ",".join(str(item) for item in tag_ids)})
    if category_ids:
        queries.append({"categories": ",".join(str(item) for item in category_ids)})
    if tag_ids:
        queries.append({"tags": ",".join(str(item) for item in tag_ids)})
    queries.append({})

    for query in queries:
        try:
            posts = wp.list_posts(
                per_page=max(limit * 3, 6),
                orderby="date",
                order="desc",
                status="publish",
                **query,
            )
        except Exception as exc:
            logger.warning("related post query failed params=%s error=%s", query, exc)
            continue

        for post in posts:
            normalized = _normalize_related_post(post)
            if normalized is None:
                continue
            if (
                normalized["slug"] in seen
                or normalized["title"].lower() == normalized_title
                or (normalized_slug and normalized["slug"] == normalized_slug)
            ):
                continue
            if _looks_like_placeholder_content(normalized["title"], normalized["slug"]):
                continue
            seen.add(normalized["slug"])
            selected.append(normalized)
            if len(selected) >= limit:
                return selected

    return selected


def _select_internal_links(
    wp: WordPressClient,
    *,
    region_tag_id: int | None = None,
    content_tag_id: int | None = None,
    exclude_title: str = "",
    exclude_slug: str = "",
) -> dict[str, list[dict[str, str]]]:
    same_region = _select_related_posts(
        wp,
        category_ids=[],
        tag_ids=[region_tag_id] if region_tag_id else [],
        exclude_title=exclude_title,
        exclude_slug=exclude_slug,
        limit=5,
    )
    seen = {item["slug"] for item in same_region}
    same_category = []
    for item in _select_related_posts(
        wp,
        category_ids=[],
        tag_ids=[content_tag_id] if content_tag_id else [],
        exclude_title=exclude_title,
        exclude_slug=exclude_slug,
        limit=6,
    ):
        if item["slug"] in seen:
            continue
        seen.add(item["slug"])
        same_category.append(item)
        if len(same_category) >= 3:
            break
    return {"same_region": same_region[:5], "same_category": same_category[:3]}


def _build_public_taxonomy_terms(
    payload: Mapping[str, Any],
    generated: Mapping[str, Any],
    settings: Settings,
) -> dict[str, Any]:
    seo = payload.get("seo") if isinstance(payload.get("seo"), Mapping) else {}
    region_value = to_plain_text(payload.get("region"))
    content_value = to_plain_text(seo.get("content_category")) or "travel"

    region_tag = _build_prefixed_tag("region", region_value)
    content_tag = _build_prefixed_tag("content", content_value)
    scenario_tag = _build_prefixed_tag("scenario", to_plain_text(payload.get("scenario")))
    audience_tag = _build_prefixed_tag("audience", to_plain_text(payload.get("audience_key")))
    angle_tag = _build_prefixed_tag("angle", to_plain_text(payload.get("content_angle_key")))
    family_tag = _build_prefixed_tag("family", to_plain_text(payload.get("title_family")))

    tag_names = [
        region_tag,
        content_tag,
        scenario_tag,
        audience_tag,
        angle_tag,
        family_tag,
        _normalize_tag_name(generated.get("candidate_topic")),
        "korean_traveler",
    ]
    tags = [item for item in dict.fromkeys(tag_names) if item]
    category = to_plain_text(settings.seo_single_post_category) or "japan"
    return {
        "categories": [category],
        "tags": tags,
        "region_tag": region_tag,
        "content_tag": content_tag,
    }


def _build_prefixed_tag(prefix: str, value: Any) -> str:
    normalized = _normalize_tag_name(value)
    if not normalized:
        return ""
    return f"{prefix}-{normalized}"


def _normalize_tag_name(value: Any) -> str:
    text = unquote(to_plain_text(value)).strip().lower()
    if not text:
        return ""
    text = re.sub(r"[^a-z0-9가-힣]+", "-", text)
    text = re.sub(r"-{2,}", "-", text).strip("-")
    return text


def _resolved_term_id(term_values: list[str], resolved_ids: list[int], target_value: str) -> int | None:
    if not target_value:
        return None
    ordered: list[str] = []
    for value in term_values:
        normalized = to_plain_text(value)
        if normalized and normalized not in ordered:
            ordered.append(normalized)
    if target_value not in ordered:
        return None
    target_index = ordered.index(target_value)
    if target_index >= len(resolved_ids):
        return None
    return resolved_ids[target_index]


def _build_wp_meta_fields(payload: Mapping[str, Any], settings: Settings) -> dict[str, Any]:
    seo = payload.get("seo")
    if not isinstance(seo, Mapping):
        return {}

    meta_fields: dict[str, Any] = {}
    if settings.wordpress_meta_title_key:
        title_tag = to_plain_text(seo.get("title_tag") or payload.get("title"))
        if title_tag:
            meta_fields[settings.wordpress_meta_title_key] = title_tag
    if settings.wordpress_meta_description_key:
        description = build_post_meta_description(payload)
        if description:
            meta_fields[settings.wordpress_meta_description_key] = description
    if settings.wordpress_meta_keywords_key:
        keywords = ", ".join(to_plain_text(item) for item in seo.get("keywords", []) if to_plain_text(item))
        if keywords:
            meta_fields[settings.wordpress_meta_keywords_key] = keywords
    if settings.wordpress_meta_canonical_key:
        canonical_path = to_plain_text(seo.get("canonical_path"))
        if canonical_path and settings.wordpress_base_url:
            meta_fields[settings.wordpress_meta_canonical_key] = f"{settings.wordpress_base_url.rstrip('/')}{canonical_path}"
    return meta_fields


def _normalize_related_post(post: Mapping[str, Any]) -> dict[str, str] | None:
    title = to_plain_text((post.get("title") or {}).get("rendered") if isinstance(post.get("title"), Mapping) else post.get("title"))
    url = to_plain_text(post.get("link"))
    slug = to_plain_text(post.get("slug"))
    if not title or not url:
        return None
    return {"title": title, "url": url, "slug": slug}


def _looks_like_placeholder_content(title: str, slug: str) -> bool:
    normalized_title = to_plain_text(title).lower()
    normalized_slug = to_plain_text(slug).lower()
    if not normalized_title:
        return True
    if normalized_slug in {"hello-world", "sample-page"}:
        return True
    if normalized_title in {"안녕하세요", "예제 페이지"}:
        return True
    if "smoke-test" in normalized_slug or "smoke test" in normalized_title:
        return True
    if re.fullmatch(r"\d+(?:-\d+)?", normalized_slug):
        return True
    return False


def _sanitize_article_payload_images(payload: Mapping[str, Any], settings: Settings) -> dict[str, Any]:
    sanitized = dict(payload)
    sections = payload.get("place_sections", [])
    if not isinstance(sections, list):
        return sanitized

    updated_sections: list[dict[str, Any]] = []
    for section in sections:
        if not isinstance(section, Mapping):
            continue
        updated_section = dict(section)
        updated_section["image_urls"] = _filter_valid_image_urls(_collect_image_urls(section), settings)
        updated_sections.append(updated_section)
    sanitized["place_sections"] = updated_sections
    return sanitized


def _collect_featured_images(places: Any, payload: Mapping[str, Any], settings: Settings) -> list[str]:
    images: list[str] = []
    for place in places:
        if isinstance(place, Mapping):
            images.extend(_collect_image_urls(place))
    for section in payload.get("place_sections", []):
        if isinstance(section, Mapping):
            images.extend(_collect_image_urls(section))
    return _filter_valid_image_urls(images, settings)


def _filter_valid_image_urls(urls: list[str], settings: Settings) -> list[str]:
    blocked_hosts = {
        host.strip().lower()
        for host in (settings.seo_block_unstable_image_hosts or "").split(",")
        if host.strip()
    }
    filtered: list[str] = []
    for url in dict.fromkeys([item for item in urls if _is_http_url(item)]):
        if _is_excluded_image_url(url, blocked_hosts):
            continue
        if settings.seo_validate_remote_images and not _remote_image_url_responds(url, timeout_seconds=min(settings.request_timeout_seconds, 8)):
            continue
        filtered.append(url)
    return filtered


def _is_excluded_image_url(url: str, blocked_hosts: set[str]) -> bool:
    lowered = url.lower()
    if any(host in lowered for host in blocked_hosts):
        return True
    return False


def _remote_image_url_responds(url: str, timeout_seconds: int = 8) -> bool:
    cached = _IMAGE_URL_VALIDATION_CACHE.get(url)
    if cached is not None:
        return cached

    try:
        response = requests.head(url, timeout=timeout_seconds, allow_redirects=True)
        if response.status_code >= 400 or response.status_code in {405, 501}:
            response = requests.get(url, timeout=timeout_seconds, allow_redirects=True, stream=True)
        content_type = str(response.headers.get("content-type") or "").lower()
        is_valid = response.status_code < 400 and (content_type.startswith("image/") or _is_probable_image_url(url))
        _IMAGE_URL_VALIDATION_CACHE[url] = is_valid
        return is_valid
    except requests.RequestException:
        _IMAGE_URL_VALIDATION_CACHE[url] = False
        return False


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
    return list(dict.fromkeys([url for url in raw_urls if _is_probable_image_url(url)]))


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


def _is_probable_image_url(value: Any) -> bool:
    if not _is_http_url(value):
        return False
    lowered = str(value).lower().strip()
    if any(token in lowered for token in ("google.com/maps", "/maps/search", "/search/?api=1", "/place/", "output=embed")):
        return False
    host = urlparse(lowered).netloc
    if host.endswith("streetviewpixels-pa.googleapis.com"):
        return False
    if re.search(r"\.(jpg|jpeg|png|webp|gif)(?:\?|$)", lowered):
        return True
    if any(host in lowered for host in ("googleusercontent.com", "ggpht.com")):
        return True
    return False


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
    from ..modules.generation.formatter import format_wordpress_html_payload

    return format_wordpress_html_payload(payload, include_map_iframe=True)


def _build_google_map_scraper_request(
    settings: Settings,
    *,
    location_query: str | None = None,
    search_strings: List[str] | None = None,
    max_results_per_search: int | None = None,
) -> Dict[str, Any]:
    resolved_search_strings = list(search_strings or [])
    if not resolved_search_strings:
        resolved_search_strings = [s.strip() for s in (settings.place_collect_search_strings or "").split(",") if s.strip()]
    if not resolved_search_strings:
        resolved_search_strings = list(DEFAULT_TOURIST_SEARCH_STRINGS)
    search_strings = _dedupe_preserve_order(resolved_search_strings)

    resolved_location_query = location_query if location_query is not None else settings.place_collect_location_query
    return {
        "location_query": resolved_location_query or "",
        "search_strings": search_strings,
        "max_results_per_search": max_results_per_search or settings.place_collect_max_results_per_search,
        "language": settings.place_collect_language,
    }


def _resolve_place_collect_location_queries(settings: Settings) -> list[str]:
    raw_values = settings.place_collect_location_queries or settings.place_collect_location_query or ""
    if not raw_values.strip():
        return list(DEFAULT_JAPAN_TOURIST_LOCATION_QUERIES)
    normalized = raw_values.replace("|", "\n")
    return _dedupe_preserve_order([line.strip() for line in normalized.splitlines() if line.strip()])


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
