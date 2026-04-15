from japantravel.config.settings import Settings
from japantravel.scripts.generate_programmatic_seo_pages import _build_meta_fields


def test_build_meta_fields_uses_fallback_meta_description():
    settings = Settings(wordpress_meta_description_key="seo_description")
    payload = {
        "title": "오사카 난바 맛집",
        "summary": "난바에서 식사 장소를 빠르게 고를 수 있게 핵심만 정리했습니다.",
        "intro": "오사카 난바 맛집 후보를 비교하는 여행자용 가이드입니다.",
        "seo": {
            "primary_keyword": "오사카 난바 맛집",
            "keywords": ["오사카 난바 맛집", "오사카 맛집"],
        },
    }

    meta_fields = _build_meta_fields(settings, payload, canonical_url="")

    assert meta_fields["seo_description"]
    assert "오사카 난바 맛집" in meta_fields["seo_description"]
