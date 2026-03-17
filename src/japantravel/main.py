"""Application entry point for manual execution."""

from .scheduler.manager import SchedulerManager


def main() -> None:
    """Bootstrap scheduler and run default jobs.

    Actual job registration is delegated to SchedulerManager.
    """
    manager = SchedulerManager()
    manager.bootstrap()


if __name__ == "__main__":
    main()
