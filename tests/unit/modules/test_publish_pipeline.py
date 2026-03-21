from japantravel.modules.publish.pipeline import PublishPipeline


class FakeWordPressClient:
    def __init__(self):
        self.created_posts = []
        self.created_pages = []
        self.updated_media = []
        self.category_counter = 10
        self.tag_counter = 30

    def list_categories(self, **params):
        return []

    def create_category(self, name, slug=None, parent=None):
        self.category_counter += 1
        return {"id": self.category_counter, "name": name}

    def list_tags(self, **params):
        return []

    def create_tag(self, name, slug=None):
        self.tag_counter += 1
        return {"id": self.tag_counter, "name": name}

    def create_post(self, **payload):
        self.created_posts.append(payload)
        return {
            "id": 99,
            "status": payload.get("status", "draft"),
            "link": "https://example.com/posts/99",
            "slug": payload.get("slug", "post-99"),
        }

    def create_page(self, **payload):
        self.created_pages.append(payload)
        return {
            "id": 109,
            "status": payload.get("status", "draft"),
            "link": "https://example.com/japan/tokyo/page-109",
            "slug": payload.get("slug", "page-109"),
        }

    def update_media(self, media_id, **fields):
        self.updated_media.append((media_id, fields))
        return {"id": media_id, **fields}


def test_publish_pipeline_updates_featured_media_alt_text():
    wp = FakeWordPressClient()
    pipeline = PublishPipeline(wp_client=wp)

    result = pipeline.publish(
        title="도쿄 라멘 맛집 정리",
        content="<div>본문</div>",
        status="publish",
        categories=["japan"],
        tags=["ramen"],
        excerpt="도쿄 라멘 맛집 핵심 요약",
        featured_media=7,
        featured_media_alt_text="도쿄 라멘 맛집 이치란",
    )

    assert result["post_id"] == 99
    assert wp.created_posts[0]["excerpt"] == "도쿄 라멘 맛집 핵심 요약"
    assert wp.updated_media == [(7, {"alt_text": "도쿄 라멘 맛집 이치란"})]


def test_publish_pipeline_accepts_meta_fields_and_page_publish():
    wp = FakeWordPressClient()
    pipeline = PublishPipeline(wp_client=wp)

    post_result = pipeline.publish(
        title="도쿄 신주쿠 카페",
        content="<div>본문</div>",
        status="draft",
        excerpt="도쿄 신주쿠 카페 메타 설명",
        meta_fields={"seo_description": "도쿄 신주쿠 카페 메타 설명"},
    )
    page_result = pipeline.publish_page(
        title="도쿄 신주쿠 카페",
        content="<div>페이지 본문</div>",
        status="draft",
        slug="shinjuku-cafe",
        parent=77,
        excerpt="도쿄 신주쿠 카페 메타 설명",
        meta_fields={"seo_description": "도쿄 신주쿠 카페 메타 설명"},
    )

    assert post_result["post_id"] == 99
    assert wp.created_posts[0]["meta"] == {"seo_description": "도쿄 신주쿠 카페 메타 설명"}
    assert page_result["post_id"] == 109
    assert wp.created_pages[0]["parent"] == 77
    assert wp.created_pages[0]["meta"] == {"seo_description": "도쿄 신주쿠 카페 메타 설명"}
