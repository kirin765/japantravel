"""Generation-specific domain models."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class PlaceInput:
    place_id: str
    name: str
    address: Optional[str] = None
    rating: float = 0.0
    review_count: int = 0
    summary: Optional[str] = None
    scenario_fitness: Optional[float] = None
    accessibility: Optional[float] = None
    stability: Optional[float] = None
    risk_penalty: Optional[float] = None
    distance_km: Optional[float] = None
    raw: Optional[Dict[str, Any]] = None


@dataclass
class ArticleSections:
    title: str
    summary: str
    intro: str
    place_sections: List[str]
    route_suggestion: str
    checklist: List[str]
    faq: List[Dict[str, str]]
    conclusion: str


@dataclass
class ArticleDraft:
    section: ArticleSections
    markdown: str
    meta: Dict[str, Any]


@dataclass
class GenerationContext:
    region: str
    scenario: str
    places: List[PlaceInput]
    user_constraints: Optional[Dict[str, Any]] = None
    locale: str = "ko-KR"
