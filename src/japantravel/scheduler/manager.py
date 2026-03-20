"""Scheduler manager."""

from __future__ import annotations

from datetime import datetime
import time

from apscheduler.schedulers.background import BackgroundScheduler

from ..config.settings import Settings
from .jobs import PipelineContext, collect_job, content_cycle_job, refresh_job


class SchedulerManager:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or Settings()
        self.scheduler = BackgroundScheduler(timezone=self.settings.scheduler_timezone)
        self.context = PipelineContext(settings=self.settings)

    def bootstrap(self) -> None:
        if self.settings.scheduler_enable_apify_collect:
            self.scheduler.add_job(
                collect_job,
                "interval",
                minutes=60,
                kwargs={"context": self.context},
            )

        self.scheduler.add_job(
            content_cycle_job,
            "interval",
            hours=max(1, self.settings.scheduler_content_interval_hours),
            next_run_time=datetime.now(tz=self.scheduler.timezone),
            kwargs={"context": self.context},
        )
        # Refresh signal pass for already published posts
        self.scheduler.add_job(
            refresh_job,
            "interval",
            minutes=180,
            kwargs={"context": self.context},
        )
        self.scheduler.start()

    def run_forever(self) -> None:
        self.bootstrap()
        try:
            while True:
                time.sleep(3600)
        except (KeyboardInterrupt, SystemExit):
            self.scheduler.shutdown(wait=False)
