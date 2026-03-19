from japantravel.modules.publish.pipeline import PublishPipeline


class FakeWordPressClient:
    def __init__(self):
        self.created_posts = []
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
