"""Job executor abstraction."""

from __future__ import annotations

from typing import Iterable


class JobExecutor:
    def run(self, jobs: Iterable[callable]) -> None:
        for job in jobs:
            job()
