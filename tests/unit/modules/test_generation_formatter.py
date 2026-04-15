from japantravel.modules.generation.formatter import (
    build_post_featured_media_alt_text,
    build_post_meta_description,
    format_wordpress_html_payload,
    restyle_existing_wordpress_html,
)


def test_format_wordpress_html_payload_uses_h2_h3_and_keyword_alt_text():
    payload = {
        "title": "도쿄 라멘 맛집 정리",
        "region": "도쿄",
        "scenario": "solo_travel",
        "seo": {
            "primary_keyword": "도쿄 라멘 맛집",
            "meta_description": "도쿄 라멘 맛집을 찾는 여행자를 위한 핵심 가이드입니다.",
            "content_category": "맛집",
        },
        "summary": "### 요약\n**도쿄 라멘 맛집**을 빠르게 정리했습니다.",
        "intro": "도쿄에서 라멘을 고를 때 확인할 포인트를 정리했습니다.",
        "place_sections": [
            {
                "place_id": "1",
                "place_name": "이치란",
                "title": "이치란",
                "body": "- 강점: 접근성이 좋습니다.\n- 방문 팁: 식사 시간대를 피하세요.",
                "address": "도쿄 신주쿠 1-1-1",
                "rating": "4.4 / 5.0",
                "review_count": 321,
                "image_urls": ["https://example.com/ramen.jpg"],
                "maps_url": "https://maps.example.com/ichiran",
                "map_embed_url": "https://maps.google.com/maps?q=35.0,139.0&output=embed",
            }
        ],
        "route_suggestion": "1. 시작 루트\n신주쿠역 근처에서 시작하세요.\n\n2. 이동 동선\n점심 혼잡도를 피해서 이동하세요.",
        "checklist": ["운영시간 확인"],
        "faq": [{"question": "언제 가면 좋나요?", "answer": "평일 오전이 비교적 무난합니다."}],
        "internal_links": {
            "same_region": [{"title": "신주쿠 맛집 가이드", "url": "https://example.com/shinjuku-guide", "slug": "shinjuku-guide"}],
            "same_category": [{"title": "도쿄 카페 정리", "url": "https://example.com/tokyo-cafe", "slug": "tokyo-cafe"}],
        },
        "conclusion": "짧은 일정이라면 역세권부터 보는 편이 효율적입니다.",
    }

    rendered = format_wordpress_html_payload(payload)

    assert "<h1" not in rendered
    assert "<h2" in rendered
    assert "<h3" in rendered
    assert "위치 및 기본정보" in rendered
    assert "특징 및 추천 이유" in rendered
    assert "방문 팁" in rendered
    assert "주변 추천 장소" in rendered
    assert "FAQ" in rendered
    assert "결론" in rendered
    assert "주소: 도쿄 신주쿠 1-1-1" in rendered
    assert "평점: 4.4 / 5.0" in rendered
    assert "리뷰 수: 321건" in rendered
    assert "<iframe" in rendered
    assert 'alt="도쿄 라멘 맛집 이치란"' in rendered
    assert "jt-inline-links" in rendered
    assert "jt-related-posts" in rendered
    assert "application/ld+json" not in rendered


def test_restyle_existing_wordpress_html_removes_duplicate_h1_and_repairs_alt_text():
    current = """
    <div class="jt-article">
      <h1>도쿄 라멘 맛집 정리</h1>
      <section><p>기존 본문입니다.</p></section>
      <article>
        <h3>이치란 (리뷰 120개)</h3>
        <img src="https://example.com/ramen.jpg" alt="123" />
      </article>
    </div>
    """

    refreshed = restyle_existing_wordpress_html(
        current,
        primary_keyword="도쿄 라멘 맛집",
        related_posts=[{"title": "신주쿠 맛집 가이드", "url": "https://example.com/shinjuku-guide", "slug": "shinjuku-guide"}],
    )

    assert "<h1" not in refreshed
    assert 'alt="도쿄 라멘 맛집 이치란"' in refreshed
    assert "jt-inline-links" in refreshed
    assert "jt-related-posts" in refreshed
    assert "주변 추천 장소" in refreshed


def test_format_wordpress_html_payload_skips_duplicate_address_text():
    payload = {
        "title": "도쿄 박물관 산책",
        "region": "도쿄",
        "summary": "도쿄 박물관 산책 요약",
        "intro": "인트로",
        "place_sections": [
            {
                "place_id": "1",
                "place_name": "도쿄 국립박물관",
                "title": "도쿄 국립박물관",
                "address": "도쿄 국립박물관",
                "review_count": 12,
                "body": "박물관 설명",
            }
        ],
        "route_suggestion": "동선 제안",
        "checklist": ["체크"],
        "faq": [{"question": "질문", "answer": "답변"}],
        "conclusion": "결론",
        "seo": {"primary_keyword": "도쿄 박물관 산책", "content_category": "여행지"},
    }

    rendered = format_wordpress_html_payload(payload)

    assert "주소: 도쿄 국립박물관" not in rendered
    assert "리뷰 수: 12건" in rendered


def test_post_meta_helpers_strip_markdown_noise():
    payload = {
        "title": "도쿄 라멘 맛집 정리",
        "summary": "### 제목\n**도쿄 라멘 맛집**을 빠르게 찾는 방법입니다.",
        "intro": "인트로입니다.",
        "seo": {"primary_keyword": "도쿄 라멘 맛집"},
        "place_sections": [{"place_name": "이치란"}],
    }

    description = build_post_meta_description(payload)
    featured_alt = build_post_featured_media_alt_text(payload)

    assert "#" not in description
    assert "**" not in description
    assert featured_alt == "도쿄 라멘 맛집 이치란"


def test_build_post_meta_description_prefixes_title_when_summary_misses_keyword():
    payload = {
        "title": "오사카 난바 맛집",
        "summary": "난바에서 식사 장소를 빠르게 고를 수 있게 핵심만 정리했습니다.",
        "intro": "오사카 난바 맛집 후보를 비교하는 여행자용 가이드입니다.",
        "seo": {"primary_keyword": "오사카 난바 맛집"},
    }

    description = build_post_meta_description(payload)

    assert "오사카 난바 맛집" in description
