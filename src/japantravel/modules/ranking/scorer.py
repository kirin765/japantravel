"""Ranking scoring helpers for place candidates."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Sequence


Scenario = str


SCENARIO_WEIGHTS: Dict[Scenario, Dict[str, float]] = {
    "solo_travel": {
        "rating": 0.22,
        "review_count": 0.15,
        "accessibility": 0.18,
        "scenario_fitness": 0.25,
        "stability": 0.10,
        "risk_penalty": 0.10,
    },
    "rainy_day": {
        "rating": 0.18,
        "review_count": 0.12,
        "accessibility": 0.20,
        "scenario_fitness": 0.30,
        "stability": 0.15,
        "risk_penalty": 0.05,
    },
    "parents_trip": {
        "rating": 0.20,
        "review_count": 0.12,
        "accessibility": 0.22,
        "scenario_fitness": 0.24,
        "stability": 0.12,
        "risk_penalty": 0.10,
    },
}


@dataclass
class RankingComponents:
    rating: float
    review_count: float
    accessibility: float
    scenario_fitness: float
    stability: float
    risk_penalty: float
    weighted_score: float


@dataclass
class RankItem:
    place_id: Any
    score: float
    components: RankingComponents
    payload: Dict[str, Any]


def score_candidates(
    places: Iterable[Mapping[str, Any]],
    scenario: str,
    weight_map: Dict[str, float] | None = None,
) -> List[RankItem]:
    scenario_name = _normalize_scenario(scenario)
    weights = weight_map or SCENARIO_WEIGHTS[scenario_name]
    normalized_weights = _normalize_weights(weights)

    ranked_places = list(places)
    max_review_count = _max_review_count(ranked_places)

    ranked_items: List[RankItem] = []
    for place in ranked_places:
        rating_score = _rating_score(place)
        review_score = _review_count_score(place, max_review_count)
        accessibility_score = _accessibility_score(place)
        scenario_score = _scenario_fitness_score(place, scenario_name)
        stability_score = _stability_score(place)
        risk_penalty = _risk_penalty(place)

        weighted_score = (
            normalized_weights["rating"] * rating_score
            + normalized_weights["review_count"] * review_score
            + normalized_weights["accessibility"] * accessibility_score
            + normalized_weights["scenario_fitness"] * scenario_score
            + normalized_weights["stability"] * stability_score
            - normalized_weights["risk_penalty"] * risk_penalty
        )

        components = RankingComponents(
            rating=rating_score,
            review_count=review_score,
            accessibility=accessibility_score,
            scenario_fitness=scenario_score,
            stability=stability_score,
            risk_penalty=risk_penalty,
            weighted_score=weighted_score,
        )
        ranked_items.append(
            RankItem(
                place_id=place.get("id") if isinstance(place, Mapping) else None,
                score=weighted_score,
                components=components,
                payload=dict(place),
            )
        )

    ranked_items.sort(key=lambda item: item.score, reverse=True)
    return ranked_items


def _max_review_count(places: Sequence[Mapping[str, Any]]) -> int:
    max_count = 0
    for place in places:
        count = _to_int(place.get("review_count", 0))
        if count > max_count:
            max_count = count
    return max_count


def _rating_score(place: Mapping[str, Any]) -> float:
    rating = _to_float(place.get("rating"), 0.0)
    return _clamp(rating / 5.0, 0.0, 1.0)


def _review_count_score(place: Mapping[str, Any], max_review_count: int) -> float:
    review_count = _to_int(place.get("review_count", 0))
    if max_review_count <= 0:
        return 0.0
    normalized = math.log1p(review_count) / math.log1p(max_review_count)
    return _clamp(normalized, 0.0, 1.0)


def _accessibility_score(place: Mapping[str, Any]) -> float:
    accessibility = place.get("accessibility")
    if accessibility is None:
        return _infer_accessibility_from_tags(place)
    return _normalize_0_to_1(accessibility)


def _scenario_fitness_score(place: Mapping[str, Any], scenario: str) -> float:
    scenario_scores = place.get("scenario_fitness")
    if isinstance(scenario_scores, Mapping) and scenario in scenario_scores:
        return _normalize_0_to_1(scenario_scores.get(scenario, 0.0))

    override = place.get(f"{scenario}_fitness")
    if override is not None:
        return _normalize_0_to_1(override)

    tags = _extract_tags(place)
    return _tag_based_scenario_score(tags, scenario)


def _stability_score(place: Mapping[str, Any]) -> float:
    stability = place.get("stability")
    if stability is None:
        return _infer_stability_from_tags(place)
    return _normalize_0_to_1(stability)


def _risk_penalty(place: Mapping[str, Any]) -> float:
    # 0 means safe, 1 means very risky
    value = place.get("risk_penalty", place.get("risk_score", 0.0))
    return _normalize_0_to_1(value)


def _normalize_0_to_1(raw: Any, min_value: float = 0.0, max_value: float = 1.0) -> float:
    raw_float = _to_float(raw, min_value)
    if max_value == min_value:
        return 0.0
    normalized = (raw_float - min_value) / (max_value - min_value)
    return _clamp(normalized, 0.0, 1.0)


def _tag_based_scenario_score(tags: Sequence[str], scenario: str) -> float:
    scores = {
        "solo_travel": {
            "solo": 1.0,
            "cafe": 0.6,
            "coffee": 0.4,
            "walk": 0.4,
            "easy_transport": 0.5,
            "night_view": 0.3,
            "family": -0.4,
            "kids": -0.3,
            "group": -0.2,
        },
        "rainy_day": {
            "indoor": 1.0,
            "museum": 1.0,
            "cafe": 0.8,
            "mall": 0.9,
            "cover": 0.7,
            "rain_shelter": 0.7,
            "outdoor": -0.6,
            "beach": -0.4,
            "park": -0.3,
            "hike": -0.5,
        },
        "parents_trip": {
            "family": 1.0,
            "kids": 0.9,
            "children": 0.8,
            "playground": 0.7,
            "stroller": 0.8,
            "toilet": 0.5,
            "elevator": 0.5,
            "stairs": -0.4,
            "nightlife": -0.5,
            "adult_only": -0.6,
        },
    }

    rules = scores[scenario]
    if not tags:
        return 0.5

    total = 0.0
    matched = 0
    for tag in tags:
        norm = tag.strip().lower().replace(" ", "_")
        weight = rules.get(norm)
        if weight is None:
            continue
        total += weight
        matched += 1

    if matched == 0:
        return 0.5

    return _clamp((total / matched + 1.0) / 2.0, 0.0, 1.0)


def _infer_accessibility_from_tags(place: Mapping[str, Any]) -> float:
    tags = _extract_tags(place)
    score = 0.5
    for tag in tags:
        if tag in {"easy_access", "stairs_free", "wheelchair", "barrier_free"}:
            score += 0.12
        if tag in {"many_stairs", "narrow", "far"}:
            score -= 0.1
    return _clamp(score, 0.0, 1.0)


def _infer_stability_from_tags(place: Mapping[str, Any]) -> float:
    tags = _extract_tags(place)
    score = 0.5
    for tag in tags:
        if tag in {"open_247", "stationary", "sheltered", "crowded"}:
            score += 0.1
        if tag in {"closed", "cancelled", "unstable", "rarely_open"}:
            score -= 0.1
    return _clamp(score, 0.0, 1.0)


def _extract_tags(place: Mapping[str, Any]) -> List[str]:
    tags = place.get("tags", [])
    if isinstance(tags, str):
        return [tags.lower().strip()]
    if not isinstance(tags, Sequence):
        return []
    normalized = []
    for tag in tags:
        if isinstance(tag, str):
            normalized.append(tag.lower().strip().replace(" ", "_"))
    return normalized


def _to_float(value: Any, default: float) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(parsed, 0)


def _clamp(value: float, min_value: float, max_value: float) -> float:
    return max(min_value, min(value, max_value))


def _normalize_scenario(scenario: str) -> str:
    default = "solo_travel"
    if scenario not in SCENARIO_WEIGHTS:
        return default
    return scenario


def _normalize_weights(weights: Dict[str, float]) -> MutableMapping[str, float]:
    # ensure keys required for scoring
    required_keys = {"rating", "review_count", "accessibility", "scenario_fitness", "stability", "risk_penalty"}
    normalized = {key: float(weights.get(key, 0.0)) for key in required_keys}
    total = sum(max(value, 0.0) for value in normalized.values())
    if total <= 0:
        raise ValueError("weights sum must be greater than 0")
    for key in required_keys:
        normalized[key] = max(normalized[key], 0.0) / total
    return normalized
