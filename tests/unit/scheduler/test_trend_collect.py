from types import SimpleNamespace
from zoneinfo import ZoneInfo

from japantravel.config.settings import Settings
from japantravel.modules.trend_queries import (
    build_region_trend_query_plan,
    build_trend_query_providers,
    parse_core_region_targets,
)
from japantravel.scheduler.jobs import _build_apify_input, trend_collect_job
from japantravel.scheduler.manager import SchedulerManager


class _FakeSeries:
    def __init__(self, values):
        self._values = list(values)

    def tolist(self):
        return list(self._values)


class _FakeFrame:
    def __init__(self, queries):
        self._queries = list(queries)

    def __getitem__(self, key):
        if key != "query":
            raise KeyError(key)
        return _FakeSeries(self._queries)


class FakeTrendClient:
    def __init__(self, top=None, rising=None, suggestions=None, should_fail=False):
        self.top = list(top or [])
        self.rising = list(rising or [])
        self.suggestions_data = list(suggestions or [])
        self.should_fail = should_fail

    def build_payload(self, kw_list, timeframe=None, geo=None):
        if self.should_fail:
            raise RuntimeError("google trends unavailable")

    def related_queries(self):
        if self.should_fail:
            raise RuntimeError("google trends unavailable")
        return {
            "도쿄 여행": {
                "top": _FakeFrame(self.top),
                "rising": _FakeFrame(self.rising),
            }
        }

    def suggestions(self, keyword):
        if self.should_fail:
            raise RuntimeError("google trends unavailable")
        return list(self.suggestions_data)


class FakeApifyClient:
    def __init__(self):
        self.run_payloads = []

    def run_actor(self, actor_id, payload, use_raw_payload=False):
        location_query = payload.get("locationQuery", "")
        run_id = f"run-{location_query}"
        self.run_payloads.append({"actor_id": actor_id, "payload": payload, "use_raw_payload": use_raw_payload, "run_id": run_id})
        return {"data": {"id": run_id}}

    def get_actor_run(self, run_id):
        if "Osaka,Japan" in run_id:
            return {"data": {"status": "FAILED", "statusMessage": "boom"}}
        return {"data": {"id": run_id, "status": "SUCCEEDED", "defaultDatasetId": f"dataset-{run_id}"}}

    def get_run_items(self, run_id):
        if "Tokyo,Japan" in run_id:
            return [{"id": "place-1", "name": "도쿄타워"}]
        return []


class FakeUpsertResult:
    def __init__(self, fetched_count=0, inserted_count=0, reused_count=0, skipped_count=0, errors=None):
        self.fetched_count = fetched_count
        self.inserted_count = inserted_count
        self.reused_count = reused_count
        self.skipped_count = skipped_count
        self.errors = list(errors or [])


class FakePlaceRepository:
    def __init__(self):
        self.calls = []

    def upsert_places(self, items, source="apify", actor_id=None, dataset_id=None, conflict_mode="update"):
        self.calls.append(
            {
                "items": list(items),
                "source": source,
                "actor_id": actor_id,
                "dataset_id": dataset_id,
                "conflict_mode": conflict_mode,
            }
        )
        return FakeUpsertResult(
            fetched_count=len(items),
            inserted_count=len(items),
            reused_count=0,
            skipped_count=0,
            errors=[],
        )


def test_build_region_trend_query_plan_merges_google_and_curated_queries():
    target = parse_core_region_targets("Tokyo,Japan")[0]
    providers = build_trend_query_providers(
        trend_source="google_then_curated",
        seed_keywords=["맛집", "카페"],
        timeframe="now 7-d",
        trend_client=FakeTrendClient(
            top=["야경", "도쿄 카페"],
            rising=["맛집"],
            suggestions=[{"title": "도쿄 전망대"}],
        ),
    )

    plan = build_region_trend_query_plan(target, providers=providers, limit=6)

    assert plan.providers_used == ["google_trends", "curated_seed"]
    assert plan.queries[:4] == ["도쿄 야경", "도쿄 카페", "도쿄 맛집", "도쿄 전망대"]


def test_build_region_trend_query_plan_falls_back_to_curated_when_google_fails():
    target = parse_core_region_targets("Tokyo,Japan")[0]
    providers = build_trend_query_providers(
        trend_source="google_then_curated",
        seed_keywords=["맛집", "카페"],
        timeframe="now 7-d",
        trend_client=FakeTrendClient(should_fail=True),
    )

    plan = build_region_trend_query_plan(target, providers=providers, limit=4)

    assert plan.providers_used == ["curated_seed"]
    assert plan.queries == ["도쿄 맛집", "도쿄 카페"]
    assert any("google_trends" in error for error in plan.errors)


def test_build_apify_input_uses_override_values():
    settings = Settings(
        apify_search_strings="restaurant,attraction",
        apify_location_query="Tokyo,Japan",
        apify_language="ko",
        apify_max_crawled_per_search=120,
    )

    payload = _build_apify_input(
        settings,
        location_query="Osaka,Japan",
        search_strings=["오사카 맛집", "오사카 카페"],
        max_crawled_per_search=12,
    )

    assert payload["locationQuery"] == "Osaka,Japan"
    assert payload["searchStringsArray"] == ["오사카 맛집", "오사카 카페"]
    assert payload["maxCrawledPlacesPerSearch"] == 12
    assert payload["maxCrawledPlaces"] == 12


def test_trend_collect_job_returns_partial_when_one_region_fails():
    settings = Settings(
        apify_actor_id="actor-1",
        trend_source="curated_only",
        trend_core_regions="Tokyo,Japan|Osaka,Japan",
        trend_seed_keywords="맛집,카페",
        trend_region_query_limit=2,
        trend_max_crawled_per_search=10,
        apify_language="ko",
    )
    context = SimpleNamespace(
        settings=settings,
        apify=FakeApifyClient(),
        place_repo=FakePlaceRepository(),
    )

    result = trend_collect_job(context=context)

    assert result["status"] == "partial"
    assert result["regions_processed"] == 2
    assert result["success_count"] == 1
    assert result["error_count"] == 1
    assert result["results"][0]["status"] == "ok"
    assert result["results"][1]["status"] == "error"


def test_scheduler_manager_registers_trend_collect_job():
    settings = Settings(
        scheduler_enable_apify_collect=False,
        scheduler_enable_trend_collect=True,
        scheduler_trend_collect_interval_hours=48,
    )
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

    assert "trend_collect_job" in job_names
