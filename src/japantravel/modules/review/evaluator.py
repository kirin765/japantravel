"""Review evaluator for generated outputs."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class ReviewResult:
    is_valid: bool
    reasons: list[str]


def evaluate_text(text: str, banned_terms: list[str] | None = None) -> ReviewResult:
    banned_terms = banned_terms or []
    lowered = text.lower()
    hits = [term for term in banned_terms if term.lower() in lowered]
    return ReviewResult(is_valid=not hits, reasons=hits)
