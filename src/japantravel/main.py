"""Application entry point for manual execution."""

from .config.logging import configure_logging
from .config.settings import Settings
from .scheduler.manager import SchedulerManager


def main() -> None:
    """Bootstrap scheduler and run default jobs.

    Actual job registration is delegated to SchedulerManager.
    """
    settings = Settings()
    configure_logging(settings)
    manager = SchedulerManager(settings=settings)
    manager.run_forever()


if __name__ == "__main__":
    main()
