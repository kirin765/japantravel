"""Topic planning helpers for generation diversity."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import hashlib
from typing import Any, Mapping, Sequence


@dataclass(frozen=True)
class TopicPlan:
    plan_key: str
    title_family: str
    title_family_label: str
    content_angle_key: str
    content_angle_label: str
    audience_key: str
    audience_label: str
    duration_days: int
    title_hook: str
    content_brief: str

    def to_payload(self) -> dict[str, Any]:
        return {
            "plan_key": self.plan_key,
            "title_family": self.title_family,
            "title_family_label": self.title_family_label,
            "content_angle_key": self.content_angle_key,
            "content_angle_label": self.content_angle_label,
            "audience_key": self.audience_key,
            "audience_label": self.audience_label,
            "duration_days": self.duration_days,
            "title_hook": self.title_hook,
            "content_brief": self.content_brief,
        }

    def to_context(self) -> dict[str, Any]:
        return self.to_payload()


TOPIC_PLAN_CATALOG: tuple[TopicPlan, ...] = (
    TopicPlan(
        plan_key="first_visit_core_day1",
        title_family="core_guide",
        title_family_label="핵심 가이드형",
        content_angle_key="first_visit_highlights",
        content_angle_label="첫 방문자가 동선 잡기 쉬운 핵심 포인트 중심",
        audience_key="first_timer",
        audience_label="처음 가는 여행자",
        duration_days=1,
        title_hook="처음 가도 흐름이 쉬운 하루 가이드",
        content_brief="대표 포인트를 과하게 늘리지 않고 무리 없는 순서로 정리한다.",
    ),
    TopicPlan(
        plan_key="photo_walk_day1",
        title_family="walk_course",
        title_family_label="산책 코스형",
        content_angle_key="photo_walk",
        content_angle_label="걷기 좋고 사진 포인트가 살아나는 산책 중심",
        audience_key="photo_walker",
        audience_label="풍경과 사진을 즐기는 여행자",
        duration_days=1,
        title_hook="천천히 걸으며 보기 좋은 포인트",
        content_brief="걷는 흐름, 풍경 변화, 쉬어가기 좋은 포인트를 앞세운다.",
    ),
    TopicPlan(
        plan_key="food_breaks_day2",
        title_family="theme_route",
        title_family_label="테마 동선형",
        content_angle_key="food_and_breaks",
        content_angle_label="식사와 카페 쉬어가기를 섞은 동선 중심",
        audience_key="foodie",
        audience_label="먹거리와 쉬는 흐름을 함께 챙기는 여행자",
        duration_days=2,
        title_hook="맛집과 쉬어가기 포인트를 함께 묶은 일정",
        content_brief="이동 자체보다 식사와 쉬는 타이밍을 중심으로 코스를 설명한다.",
    ),
    TopicPlan(
        plan_key="budget_light_day2",
        title_family="practical_plan",
        title_family_label="실전 플랜형",
        content_angle_key="budget_light",
        content_angle_label="비용과 이동 부담을 낮춘 가벼운 일정 중심",
        audience_key="budget_saver",
        audience_label="예산과 체력을 아끼고 싶은 여행자",
        duration_days=2,
        title_hook="가볍게 움직이기 좋은 실전 코스",
        content_brief="돈과 체력을 아끼는 선택지, 대체 동선, 체크 포인트를 강조한다.",
    ),
    TopicPlan(
        plan_key="rain_backup_day1",
        title_family="problem_solving",
        title_family_label="문제 해결형",
        content_angle_key="weather_backup",
        content_angle_label="날씨 변수에도 무너지지 않는 대체 동선 중심",
        audience_key="careful_planner",
        audience_label="변수를 줄이고 싶은 여행자",
        duration_days=1,
        title_hook="날씨가 흔들려도 유지하기 쉬운 플랜",
        content_brief="실내/실외 전환과 이동 리스크를 같이 안내한다.",
    ),
    TopicPlan(
        plan_key="weekend_reset_day2",
        title_family="reset_trip",
        title_family_label="리셋 여행형",
        content_angle_key="slow_reset",
        content_angle_label="주말에 무리 없이 쉬어가기 좋은 힐링 동선 중심",
        audience_key="weekend_reset",
        audience_label="짧게 쉬고 돌아오고 싶은 여행자",
        duration_days=2,
        title_hook="주말 리듬에 맞는 느슨한 코스",
        content_brief="무리한 이동보다 여유와 회복감이 느껴지는 흐름을 우선한다.",
    ),
    TopicPlan(
        plan_key="hidden_mood_day3",
        title_family="curated_spots",
        title_family_label="큐레이션형",
        content_angle_key="local_mood",
        content_angle_label="대표 명소보다 분위기가 좋은 포인트 중심",
        audience_key="repeat_visitor",
        audience_label="조금 다른 포인트를 찾는 재방문 여행자",
        duration_days=3,
        title_hook="대표 명소 외에 분위기 좋은 선택지",
        content_brief="인기만이 아니라 현장 분위기와 체류감을 중심으로 고른다.",
    ),
    TopicPlan(
        plan_key="time_slot_day1",
        title_family="time_slot",
        title_family_label="시간대 활용형",
        content_angle_key="morning_evening",
        content_angle_label="아침과 해질녘 시간을 살리는 효율 동선 중심",
        audience_key="efficient_traveler",
        audience_label="짧은 시간 효율을 중시하는 여행자",
        duration_days=1,
        title_hook="시간대별로 만족도가 달라지는 포인트",
        content_brief="시간대 선택이 경험 차이를 만드는 장소를 앞쪽에 둔다.",
    ),
)


def select_topic_plan(
    recent_signatures: Sequence[Any],
    *,
    region_key: str = "",
    scenario: str = "solo_travel",
) -> TopicPlan:
    plans = list(TOPIC_PLAN_CATALOG)
    if not plans:
        raise ValueError("TOPIC_PLAN_CATALOG must not be empty")

    recent_plan_keys = Counter(_signature_value(item, "plan_key") for item in recent_signatures if _signature_value(item, "plan_key"))
    recent_title_families = Counter(_signature_value(item, "title_family") for item in recent_signatures if _signature_value(item, "title_family"))
    recent_angles = Counter(_signature_value(item, "content_angle_key") for item in recent_signatures if _signature_value(item, "content_angle_key"))
    recent_audiences = Counter(_signature_value(item, "audience_key") for item in recent_signatures if _signature_value(item, "audience_key"))
    recent_durations = Counter(
        int(_signature_value(item, "duration_days"))
        for item in recent_signatures
        if _signature_value(item, "duration_days")
    )
    recent_combos = Counter(
        (
            _signature_value(item, "title_family"),
            _signature_value(item, "content_angle_key"),
            _signature_value(item, "audience_key"),
            int(_signature_value(item, "duration_days")),
        )
        for item in recent_signatures
        if _signature_value(item, "title_family")
        and _signature_value(item, "content_angle_key")
        and _signature_value(item, "audience_key")
        and _signature_value(item, "duration_days")
    )

    def score(plan: TopicPlan) -> float:
        combo = (plan.title_family, plan.content_angle_key, plan.audience_key, plan.duration_days)
        points = 100.0
        points -= recent_plan_keys.get(plan.plan_key, 0) * 40.0
        points -= recent_combos.get(combo, 0) * 24.0
        points -= recent_title_families.get(plan.title_family, 0) * 9.0
        points -= recent_angles.get(plan.content_angle_key, 0) * 8.0
        points -= recent_audiences.get(plan.audience_key, 0) * 6.0
        points -= recent_durations.get(plan.duration_days, 0) * 4.0
        points += _stable_tiebreak(region_key=region_key, scenario=scenario, plan_key=plan.plan_key)
        return points

    return max(plans, key=score)


def _stable_tiebreak(*, region_key: str, scenario: str, plan_key: str) -> float:
    raw = f"{region_key}|{scenario}|{plan_key}".encode("utf-8")
    digest = hashlib.sha1(raw).hexdigest()
    return int(digest[:8], 16) / 0xFFFFFFFF


def _signature_value(item: Any, field: str) -> Any:
    if isinstance(item, Mapping):
        return item.get(field)
    return getattr(item, field, None)
