from evidence_cartographer.orchestration.schedules import (
    DAILY_INCREMENTAL_CRON,
    WEEKLY_FULL_CRON,
)


def test_refresh_crons_are_distinct() -> None:
    assert WEEKLY_FULL_CRON == "0 2 * * 0"
    assert DAILY_INCREMENTAL_CRON == "0 3 * * *"
    assert WEEKLY_FULL_CRON != DAILY_INCREMENTAL_CRON
