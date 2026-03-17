"""Candidate selection helpers."""

from __future__ import annotations

from typing import Iterable, List

from .scorer import RankItem


def top_k(items: Iterable[RankItem], k: int, threshold: float | None = None) -> List[RankItem]:
    filtered = [item for item in items if threshold is None or item.score >= threshold]
    return list(filtered)[:k]


def top_k_by_scenario(
    items: Iterable[RankItem],
    k: int,
    threshold: float | None = None,
) -> List[RankItem]:
    filtered = list(items)
    if threshold is not None:
        filtered = [item for item in filtered if item.score >= threshold]
    filtered.sort(key=lambda item: item.score, reverse=True)
    return filtered[:k]
