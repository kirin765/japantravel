from japantravel.scripts.normalize_taxonomy_terms import _build_target_tags


def test_build_target_tags_keeps_existing_tags_and_adds_region_content_tags():
    post = {"tags": [30], "categories": [3, 12, 15]}
    category_map = {
        3: {"slug": "japan", "name": "japan"},
        12: {"slug": "rainy_day", "name": "rainy_day"},
        15: {"slug": "oshima", "name": "Oshima"},
    }
    tag_map = {30: {"slug": "jp-solo_travel-general", "name": "jp-solo_travel-general"}}

    tags = _build_target_tags(post, category_map, tag_map)

    assert "jp-solo_travel-general" in tags
    assert "content-rainy-day" in tags
    assert "region-oshima" in tags
