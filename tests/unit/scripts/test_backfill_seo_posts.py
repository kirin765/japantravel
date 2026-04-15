from japantravel.config.settings import Settings
from japantravel.scripts.backfill_seo_posts import _build_post_updates, _resolve_post_ids
from japantravel.shared.exceptions import ExternalServiceError


def test_build_post_updates_adds_meta_description_for_existing_post():
    settings = Settings(wordpress_meta_description_key="seo_description")
    post = {
        "excerpt": {"rendered": "기존 설명"},
        "meta": {},
    }

    updates = _build_post_updates(
        post=post,
        settings=settings,
        excerpt="새 메타 설명",
        refreshed_content="<div>새 본문</div>",
        current_content="<div>기존 본문</div>",
    )

    assert updates["excerpt"] == "새 메타 설명"
    assert updates["content"] == "<div>새 본문</div>"
    assert updates["meta"] == {"seo_description": "새 메타 설명"}


def test_build_post_updates_skips_unchanged_meta_description():
    settings = Settings(wordpress_meta_description_key="seo_description")
    post = {
        "excerpt": {"rendered": "같은 설명"},
        "meta": {"seo_description": "같은 설명"},
    }

    updates = _build_post_updates(
        post=post,
        settings=settings,
        excerpt="같은 설명",
        refreshed_content="<div>본문</div>",
        current_content="<div>본문</div>",
    )

    assert updates == {}


class FakeWordPressClient:
    def __init__(self, pages, fail_page=None):
        self.pages = pages
        self.fail_page = fail_page

    def list_posts(self, **params):
        page = int(params.get("page", 1))
        if page == self.fail_page:
            raise ExternalServiceError("Request failed: 400 Client Error: rest_post_invalid_page_number")
        return self.pages.get(page, [])


def test_resolve_post_ids_fetches_all_pages_when_requested():
    wp = FakeWordPressClient(
        {
            1: [{"id": 1}, {"id": 2}],
            2: [{"id": 3}],
        }
    )

    post_ids = _resolve_post_ids(wp, requested_ids=[], limit=1, status="publish", fetch_all=True)

    assert post_ids == [1, 2, 3]


def test_resolve_post_ids_stops_on_invalid_page_error_when_fetching_all():
    wp = FakeWordPressClient(
        {
            1: [{"id": 1}, {"id": 2}],
        },
        fail_page=2,
    )

    post_ids = _resolve_post_ids(wp, requested_ids=[], limit=1, status="publish", fetch_all=True)

    assert post_ids == [1, 2]
