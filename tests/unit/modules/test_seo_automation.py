from japantravel.modules.seo_automation.assets import render_robots_txt, render_sitemap_xml
from japantravel.modules.seo_automation.landing_pages import build_programmatic_page_payload, select_places_for_keyword
from japantravel.modules.seo_automation.planner import build_keyword_target, expand_core_keyword_targets
from japantravel.modules.seo_automation.structured_data import build_structured_data
from japantravel.modules.seo_automation.renderer import render_full_html_document


def test_build_keyword_target_creates_hierarchical_path_and_keywords():
    target = build_keyword_target("도쿄 신주쿠 카페")

    assert target.region_slug == "tokyo"
    assert target.leaf_slug == "shinjuku-cafe"
    assert target.canonical_path == "/japan/tokyo/shinjuku-cafe"
    assert "도쿄 신주쿠 카페" in target.keywords
    assert target.schema_type == "Restaurant"


def test_expand_core_keyword_targets_respects_limit():
    targets = expand_core_keyword_targets(limit=5)

    assert len(targets) == 5
    assert all(target.canonical_path.startswith("/japan/") for target in targets)


def test_build_programmatic_page_payload_selects_matching_places():
    target = build_keyword_target("도쿄 신주쿠 카페")
    places = [
        {
            "place_id": "1",
            "name": "신주쿠 브루어스 카페",
            "address": "도쿄 신주쿠 1-1-1",
            "city": "도쿄",
            "country": "Japan",
            "category": "cafe",
            "rating": 4.5,
            "review_count": 120,
            "image_urls": ["https://example.com/a.webp"],
            "maps_url": "https://maps.example.com/a",
        },
        {
            "place_id": "2",
            "name": "난바 라멘",
            "address": "오사카 난바 2-2-2",
            "city": "오사카",
            "country": "Japan",
            "category": "restaurant",
            "rating": 4.8,
            "review_count": 500,
            "image_urls": [],
            "maps_url": "",
        },
    ]

    selected = select_places_for_keyword(target, places)
    page = build_programmatic_page_payload(target, places, internal_links={"same_region": [], "same_category": []})

    assert selected[0]["name"] == "신주쿠 브루어스 카페"
    assert page.payload["seo"]["canonical_path"] == "/japan/tokyo/shinjuku-cafe"
    assert page.payload["seo"]["content_category"] == "카페"
    assert page.payload["place_sections"][0]["place_name"] == "신주쿠 브루어스 카페"


def test_render_full_html_document_includes_meta_and_schema():
    target = build_keyword_target("오사카 난바 맛집")
    page = build_programmatic_page_payload(target, [])
    html = render_full_html_document(page.payload, canonical_url="https://www.japantravel.co.kr/japan/osaka/namba-restaurants")
    schema = build_structured_data(page.payload, page_url="https://www.japantravel.co.kr/japan/osaka/namba-restaurants")

    assert "<title>" in html
    assert 'meta name="description"' in html
    assert 'meta name="keywords"' in html
    assert 'meta name="naver-site-verification"' in html
    assert 'rel="canonical"' in html
    assert "application/ld+json" in html
    assert schema["@graph"]


def test_render_robots_and_sitemap_assets():
    robots = render_robots_txt("https://www.japantravel.co.kr")
    sitemap = render_sitemap_xml(
        [
            {"loc": "https://www.japantravel.co.kr/japan/tokyo/shinjuku-cafe", "lastmod": "2026-03-22T00:00:00Z"},
            "https://www.japantravel.co.kr/japan/osaka/namba-restaurants",
        ]
    )

    assert "Sitemap: https://www.japantravel.co.kr/sitemap.xml" in robots
    assert "<urlset" in sitemap
    assert "https://www.japantravel.co.kr/japan/tokyo/shinjuku-cafe" in sitemap
