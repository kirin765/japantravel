import pytest
from zoneinfo import ZoneInfo

from japantravel.config.settings import Settings
from japantravel.scheduler import jobs
from japantravel.scheduler.jobs import (
    DEFAULT_JAPAN_TOURIST_LOCATION_QUERIES,
    DEFAULT_TOURIST_SEARCH_STRINGS,
    _build_google_map_scraper_request,
    _force_collect_fresh_places,
    _resolve_place_collect_location_queries,
    content_cycle_job,
    generate_job,
)
from japantravel.scheduler.manager import SchedulerManager


def test_build_google_map_scraper_request_uses_override_values():
    settings = Settings(
        place_collect_search_strings="restaurant,attraction",
        place_collect_location_query="Tokyo,Japan",
        place_collect_language="ko",
        place_collect_max_results_per_search=120,
    )

    payload = _build_google_map_scraper_request(
        settings,
        location_query="Osaka,Japan",
        search_strings=["오사카 맛집", "오사카 카페"],
        max_results_per_search=12,
    )

    assert payload["location_query"] == "Osaka,Japan"
    assert payload["search_strings"] == ["오사카 맛집", "오사카 카페"]
    assert payload["max_results_per_search"] == 12
    assert payload["language"] == "ko"


def test_build_google_map_scraper_request_falls_back_to_tourist_search_strings():
    settings = Settings(place_collect_search_strings="")

    payload = _build_google_map_scraper_request(settings, location_query="Tokyo,Japan")

    assert payload["search_strings"] == list(DEFAULT_TOURIST_SEARCH_STRINGS)


def test_resolve_place_collect_location_queries_supports_multiple_values():
    settings = Settings(
        place_collect_location_queries="Tokyo, Japan|Osaka, Japan\nFukuoka, Japan",
        place_collect_location_query="Should,Not,Use",
    )

    assert _resolve_place_collect_location_queries(settings) == [
        "Tokyo, Japan",
        "Osaka, Japan",
        "Fukuoka, Japan",
    ]


def test_resolve_place_collect_location_queries_falls_back_to_default_tourist_regions():
    settings = Settings(place_collect_location_queries="", place_collect_location_query="")

    assert _resolve_place_collect_location_queries(settings) == list(DEFAULT_JAPAN_TOURIST_LOCATION_QUERIES)


def test_generate_job_requires_google_map_scraper():
    settings = Settings()
    context = type(
        "Context",
        (),
        {
            "settings": settings,
            "google_map_scraper": None,
            "raw_collections": [],
        },
    )()

    result = generate_job(context=context)

    assert result["status"] == "error"
    assert result["reason"] == "google_map_scraper_not_configured"


def test_force_collect_fresh_places_runs_every_configured_location():
    class FakeScraper:
        def __init__(self):
            self.calls = []

        def scrape_places(self, *, location_query="", search_strings=None, max_results_per_search=0, language=""):
            self.calls.append(
                {
                    "location_query": location_query,
                    "search_strings": list(search_strings or []),
                    "max_results_per_search": max_results_per_search,
                    "language": language,
                }
            )
            return {
                "items": [
                    {
                        "id": location_query.replace(", ", "-").lower(),
                        "name": f"Place {location_query}",
                        "latitude": 35.0,
                        "longitude": 139.0,
                    }
                ],
                "meta": {"result_count": 1},
            }

    class FakeRepo:
        def __init__(self):
            self.calls = []

        def upsert_places(self, items, source="google_map_scraper", conflict_mode="update"):
            self.calls.append({"items": list(items), "source": source, "conflict_mode": conflict_mode})
            return type(
                "Result",
                (),
                {"fetched_count": len(items), "inserted_count": len(items), "skipped_count": 0, "errors": []},
            )()

    settings = Settings(
        place_collect_location_queries="Tokyo, Japan|Osaka, Japan",
        place_collect_search_strings="restaurant,attraction",
        place_collect_language="ko",
        place_collect_max_results_per_search=5,
    )
    scraper = FakeScraper()
    repo = FakeRepo()
    context = type(
        "Context",
        (),
        {
            "settings": settings,
            "google_map_scraper": scraper,
            "place_repo": repo,
            "raw_collections": [],
        },
    )()

    result = _force_collect_fresh_places(context, scenario="solo_travel")

    assert result["status"] == "ok"
    assert result["locations_processed"] == 2
    assert result["failed_locations"] == []
    assert len(scraper.calls) == 2
    assert [call["location_query"] for call in scraper.calls] == ["Tokyo, Japan", "Osaka, Japan"]
    assert len(repo.calls) == 1
    assert len(repo.calls[0]["items"]) == 2
    assert len(context.raw_collections) == 2


def test_force_collect_fresh_places_uses_default_tourist_regions_when_unconfigured():
    class FakeScraper:
        def __init__(self):
            self.calls = []

        def scrape_places(self, *, location_query="", search_strings=None, max_results_per_search=0, language=""):
            self.calls.append(location_query)
            return {
                "items": [
                    {
                        "id": location_query.replace(", ", "-").lower(),
                        "name": f"Place {location_query}",
                        "latitude": 35.0,
                        "longitude": 139.0,
                    }
                ],
                "meta": {"result_count": 1},
            }

    class FakeRepo:
        def upsert_places(self, items, source="google_map_scraper", conflict_mode="update"):
            return type(
                "Result",
                (),
                {"fetched_count": len(items), "inserted_count": len(items), "skipped_count": 0, "errors": []},
            )()

    settings = Settings(place_collect_location_queries="", place_collect_location_query="")
    scraper = FakeScraper()
    context = type(
        "Context",
        (),
        {
            "settings": settings,
            "google_map_scraper": scraper,
            "place_repo": FakeRepo(),
            "raw_collections": [],
        },
    )()

    result = _force_collect_fresh_places(context, scenario="solo_travel")

    assert result["status"] == "ok"
    assert result["locations_processed"] == len(DEFAULT_JAPAN_TOURIST_LOCATION_QUERIES)
    assert scraper.calls == list(DEFAULT_JAPAN_TOURIST_LOCATION_QUERIES)


def test_scheduler_manager_registers_only_content_and_refresh_jobs():
    settings = Settings(scheduler_content_interval_hours=8)
    manager = SchedulerManager(settings=settings)

    class FakeScheduler:
        def __init__(self):
            self.jobs = []
            self.timezone = ZoneInfo("Asia/Seoul")

        def add_job(self, func, trigger, **kwargs):
            self.jobs.append({"func": func, "trigger": trigger, **kwargs})

        def start(self):
            return None

    manager.scheduler = FakeScheduler()
    manager.bootstrap()

    job_names = [job["func"].__name__ for job in manager.scheduler.jobs]

    assert job_names == ["content_cycle_job", "refresh_job"]


def test_content_cycle_job_uses_generate_review_publish_sequence(monkeypatch: pytest.MonkeyPatch):
    calls: list[str] = []

    def fake_generate_job(*, context, scenario="solo_travel"):
        calls.append(f"generate:{scenario}")
        return {"job": "generate", "status": "ok"}

    def fake_review_job(*, context):
        calls.append("review")
        return {"job": "review", "status": "ok"}

    def fake_publish_job(*, context):
        calls.append("publish")
        return {"job": "publish", "status": "ok", "published": []}

    monkeypatch.setattr(jobs, "generate_job", fake_generate_job)
    monkeypatch.setattr(jobs, "review_job", fake_review_job)
    monkeypatch.setattr(jobs, "publish_job", fake_publish_job)

    context = type(
        "Context",
        (),
        {
            "raw_collections": ["stale"],
            "article_candidates": ["old"],
            "generated_articles": ["old"],
        },
    )()

    result = content_cycle_job(context=context, scenario="solo_travel")

    assert result["status"] == "ok"
    assert calls == ["generate:solo_travel", "review", "publish"]
    assert context.raw_collections == []
    assert context.article_candidates == []
    assert context.generated_articles == []
