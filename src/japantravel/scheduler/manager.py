"""Scheduler manager."""

from __future__ import annotations

from apscheduler.schedulers.background import BackgroundScheduler

from ..config.settings import Settings
from .jobs import PipelineContext, collect_job, generate_job, publish_job, refresh_job, review_job


class SchedulerManager:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or Settings()
        self.scheduler = BackgroundScheduler(timezone=self.settings.scheduler_timezone)
        self.context = PipelineContext(settings=self.settings)

    def bootstrap(self) -> None:
        # Collection / ranking / generation
        self.scheduler.add_job(
            collect_job,
            "interval",
            minutes=60,
            kwargs={"context": self.context},
        )
        self.scheduler.add_job(
            generate_job,
            "interval",
            minutes=60,
            kwargs={"context": self.context},
        )
        # Validation and quality gates
        self.scheduler.add_job(
            review_job,
            "interval",
            minutes=60,
            kwargs={"context": self.context},
        )
        self.scheduler.add_job(
            publish_job,
            "interval",
            minutes=60,
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
