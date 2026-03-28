from __future__ import annotations

from japantravel.modules.generation.pipeline import GenerationPipeline


class _DummyOpenAIClient:
    def generate(self, system_prompt: str, user_prompt: str, context=None) -> str:  # pragma: no cover - helper
        return "ok"


def test_compact_place_excludes_large_raw_payload_fields() -> None:
    pipeline = GenerationPipeline(openai_client=_DummyOpenAIClient())
    place = {
        "id": "place-1",
        "name": "도쿄 타워",
        "region": "Tokyo",
        "country": "Japan",
        "category": "attraction",
        "address": "4 Chome-2-8 Shibakoen, Minato City, Tokyo",
        "rating": 4.6,
        "review_count": 12034,
        "maps_url": "https://maps.example/place-1",
        "image_urls": ["https://img/1.jpg", "https://img/2.jpg", "https://img/3.jpg"],
        "review_snippets": ["리뷰 하나", "리뷰 둘", "리뷰 셋"],
        "raw_payload": {"huge": "x" * 50000},
    }

    compact = pipeline._compact_place(place)

    assert compact["name"] == "도쿄 타워"
    assert compact["review_count"] == 12034
    assert compact["image_urls"] == ["https://img/1.jpg", "https://img/2.jpg"]
    assert compact["review_snippets"] == ["리뷰 하나", "리뷰 둘"]
    assert "raw_payload" not in compact


def test_compact_generation_context_keeps_only_prompt_relevant_fields() -> None:
    pipeline = GenerationPipeline(openai_client=_DummyOpenAIClient())
    context = {
        "region": "Tokyo, Japan",
        "scenario": "solo_travel",
        "duration_days": 1,
        "variant_id": "A",
        "audience": "한국어 여행 독자",
        "title_hook": "현실적인 동선과 포인트",
        "topic_plan": {"huge": "x" * 50000},
        "extra": {"very_large": "y" * 50000},
    }
    selected_places = [
        {
            "id": "place-1",
            "name": "도쿄 타워",
            "raw_payload": {"huge": "x" * 50000},
            "review_snippets": ["리뷰 하나", "리뷰 둘", "리뷰 셋"],
        }
    ]

    compact = pipeline._compact_generation_context(context, selected_places=selected_places)

    assert compact["region"] == "Tokyo, Japan"
    assert compact["variant_id"] == "A"
    assert "topic_plan" not in compact
    assert "extra" not in compact
    assert compact["selected_places"][0]["name"] == "도쿄 타워"
    assert "raw_payload" not in compact["selected_places"][0]


def test_display_rating_hides_zero_values() -> None:
    pipeline = GenerationPipeline(openai_client=_DummyOpenAIClient())

    assert pipeline._to_display_rating({"rating": 0}) == ""
    assert pipeline._to_display_rating({"rating": "0"}) == ""
    assert pipeline._to_display_rating({"rating": 4.6}) == "4.6 / 5.0"
