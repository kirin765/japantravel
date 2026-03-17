"""HTTP utility wrappers."""

from __future__ import annotations

import json
from typing import Any

from requests import Response


def to_json_or_text(response: Response) -> Any:
    try:
        return response.json()
    except json.JSONDecodeError:
        return response.text
