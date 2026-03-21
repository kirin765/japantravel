from japantravel.modules.generation.topic_planner import select_topic_plan
from japantravel.modules.ranking.scorer import RankItem, RankingComponents
from japantravel.scheduler.jobs import (
    RecentPostSignature,
    _extract_place_keys_from_wp_content,
    _infer_recent_post_region,
    _find_recent_duplicate_signature,
    _normalize_region_key,
    _select_region_cluster,
    _title_tokens,
)
from japantravel.scripts.cleanup_duplicate_topics import _find_latest_duplicate


def _rank_item(score: float, name: str, city: str, country: str = "JP") -> RankItem:
    return RankItem(
        place_id=name,
        score=score,
        components=RankingComponents(
            rating=1.0,
            review_count=1.0,
            accessibility=1.0,
            scenario_fitness=1.0,
            stability=1.0,
            risk_penalty=0.0,
            weighted_score=score,
        ),
        payload={"name": name, "city": city, "country": country, "place_type": "travel"},
    )


def test_extract_place_keys_from_wp_content_keeps_full_google_place_ids():
    html = '<a href="https://maps.google.com/?query_place_id=ChIJlfWxscONGGARpqUGevsCnVo" target="_blank">지도</a>'

    keys = _extract_place_keys_from_wp_content(html)

    assert keys == {"ChIJlfWxscONGGARpqUGevsCnVo"}


def test_select_region_cluster_skips_recent_region_cluster():
    ranked = [
        _rank_item(0.99, "a1", "Aogashima"),
        _rank_item(0.95, "a2", "Aogashima"),
        _rank_item(0.91, "o1", "Oshima"),
        _rank_item(0.89, "o2", "Oshima"),
    ]
    recent = [
        RecentPostSignature(
            post_id=80,
            title="아오가시마에서 혼자 떠나는 2일간의 여행",
            slug="aogashima-trip",
            title_tokens=_title_tokens("아오가시마에서 혼자 떠나는 2일간의 여행", "aogashima-trip"),
            region_key=_normalize_region_key("Aogashima"),
            region_label="Aogashima",
        )
    ]

    cluster = _select_region_cluster(
        ranked_items=ranked,
        recent_signatures=recent,
        target_count=2,
        min_count=2,
        title_threshold=0.6,
    )

    assert cluster is not None
    assert cluster.region_key == _normalize_region_key("Oshima")
    assert [item.payload["city"] for item in cluster.ranked_items[:2]] == ["Oshima", "Oshima"]


def test_find_recent_duplicate_signature_matches_title_similarity_without_region_key():
    recent = [
        RecentPostSignature(
            post_id=75,
            title="아오가시마에서의 혼자만의 여행: 자연과 맛을 만끽하는 2일 일정",
            slug="aogashima-solo-trip",
            title_tokens=_title_tokens("아오가시마에서의 혼자만의 여행: 자연과 맛을 만끽하는 2일 일정", "aogashima-solo-trip"),
        )
    ]

    signature, reason = _find_recent_duplicate_signature(
        title="아오가시마에서의 혼자만의 여행: 자연과 맛을 만끽하는 2일 일정 추천",
        region_key="",
        recent_signatures=recent,
        threshold=0.6,
    )

    assert signature is not None
    assert signature.post_id == 75
    assert reason == "recent_title_similarity"


def test_find_recent_duplicate_signature_ignores_generic_title_overlap_for_other_regions():
    recent = [
        RecentPostSignature(
            post_id=59,
            title="아오가시마에서 혼자 떠나는 2일간의 여행 일정 추천",
            slug="aogashima-solo-trip",
            title_tokens=_title_tokens("아오가시마에서 혼자 떠나는 2일간의 여행 일정 추천", "aogashima-solo-trip"),
            region_key=_normalize_region_key("Aogashima"),
            region_label="Aogashima",
        )
    ]

    signature, reason = _find_recent_duplicate_signature(
        title="Oshima에서 혼자 떠나는 2일간의 힐링 여행 추천 일정",
        region_key=_normalize_region_key("Oshima"),
        recent_signatures=recent,
        threshold=0.6,
    )

    assert signature is None
    assert reason == ""


def test_infer_recent_post_region_prefers_first_place_key_over_majority():
    place_rows_by_key = {
        "a1": {"region": "Aogashima", "country": "JP"},
        "o1": {"region": "Oshima", "country": "JP"},
        "o2": {"region": "Oshima", "country": "JP"},
    }

    region_key, region_label = _infer_recent_post_region(
        ["a1", "o1", "o2"],
        title="아오가시마에서 혼자 떠나는 2일간의 여행",
        slug="aogashima-trip",
        place_rows_by_key=place_rows_by_key,
    )

    assert region_key == _normalize_region_key("Aogashima")
    assert region_label == "Aogashima"


def test_cleanup_duplicate_topics_picks_newest_matching_post_only():
    signatures = [
        RecentPostSignature(
            post_id=80,
            title="아오가시마에서 혼자 떠나는 2일간의 여행, 여섯 곳의 매력을 만나다",
            slug="latest-aogashima",
            title_tokens=_title_tokens("아오가시마에서 혼자 떠나는 2일간의 여행, 여섯 곳의 매력을 만나다", "latest-aogashima"),
            region_key=_normalize_region_key("Aogashima"),
            region_label="Aogashima",
        ),
        RecentPostSignature(
            post_id=75,
            title="아오가시마에서의 혼자만의 여행: 자연과 맛을 만끽하는 2일 일정",
            slug="older-aogashima",
            title_tokens=_title_tokens("아오가시마에서의 혼자만의 여행: 자연과 맛을 만끽하는 2일 일정", "older-aogashima"),
            region_key=_normalize_region_key("Aogashima"),
            region_label="Aogashima",
        ),
        RecentPostSignature(
            post_id=10,
            title="오시마에서 걷는 2일 여행",
            slug="oshima-trip",
            title_tokens=_title_tokens("오시마에서 걷는 2일 여행", "oshima-trip"),
            region_key=_normalize_region_key("Oshima"),
            region_label="Oshima",
        ),
    ]

    target, previous, reason = _find_latest_duplicate(signatures, target_region="aogashima", threshold=0.6)

    assert target is not None
    assert previous is not None
    assert target.post_id == 80
    assert previous.post_id == 75
    assert reason == "recent_region"


def test_select_topic_plan_prefers_unused_title_family_and_angle():
    recent = [
        RecentPostSignature(
            post_id=80,
            title="도쿄 첫 방문자를 위한 하루 가이드",
            slug="tokyo-first-visit",
            title_family="core_guide",
            content_angle_key="first_visit_highlights",
            audience_key="first_timer",
            duration_days=1,
        ),
        RecentPostSignature(
            post_id=75,
            title="오사카 첫 방문 하루 가이드",
            slug="osaka-first-visit",
            title_family="core_guide",
            content_angle_key="first_visit_highlights",
            audience_key="first_timer",
            duration_days=1,
        ),
    ]

    plan = select_topic_plan(recent_signatures=recent, region_key="oshima", scenario="solo_travel")

    assert plan.title_family != "core_guide"
    assert plan.content_angle_key != "first_visit_highlights"
