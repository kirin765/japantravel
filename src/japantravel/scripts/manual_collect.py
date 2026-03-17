"""Manual Apify collection runner."""

from __future__ import annotations

from ..scheduler.jobs import PipelineContext, collect_job


def main() -> None:
    result = collect_job(PipelineContext())
    print(result)


if __name__ == "__main__":
    main()
