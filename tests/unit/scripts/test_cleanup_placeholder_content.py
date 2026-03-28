from unittest.mock import patch

from japantravel.scripts.cleanup_placeholder_content import classify_content_item


def test_classify_content_item_marks_placeholder_slug():
    target = classify_content_item(
        "post",
        {
            "id": 9,
            "title": {"rendered": "Smoke Test"},
            "slug": "smoke-test-rainy-day-odiba-shibuya",
            "link": "https://www.japantravel.co.kr/smoke-test-rainy-day-odiba-shibuya/",
        },
    )

    assert target is not None
    assert target.reason == "placeholder"


def test_classify_content_item_marks_frontend_404_publish_url():
    with patch("japantravel.scripts.cleanup_placeholder_content.fetch_frontend_status", return_value=404):
        target = classify_content_item(
            "page",
            {
                "id": 2,
                "title": {"rendered": "커스텀 랜딩"},
                "slug": "custom-broken-page",
                "link": "https://www.japantravel.co.kr/%EC%98%88%EC%A0%9C-%ED%8E%98%EC%9D%B4%EC%A7%80/",
            },
        )

    assert target is not None
    assert target.reason == "frontend_404"
    assert target.frontend_status == 404
