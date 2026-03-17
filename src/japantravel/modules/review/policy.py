"""Simple quality and governance policies."""

from __future__ import annotations

MIN_LENGTH = 100
MAX_LENGTH = 5000


def violates_length_policy(text: str) -> bool:
    return not (MIN_LENGTH <= len(text) <= MAX_LENGTH)
