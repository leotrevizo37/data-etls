import sys
import unittest
from pathlib import Path


PROJECT_SRC = Path(__file__).resolve().parents[1] / "src"
if str(PROJECT_SRC) not in sys.path:
    sys.path.insert(0, str(PROJECT_SRC))

from dagster import DefaultScheduleStatus  # noqa: E402
from carlsjr.defs.schedules.factaudiencia_scheduler import (  # noqa: E402
    fact_audiencia_sched_every_w,
)


class FactAudienciaScheduleTests(unittest.TestCase):
    def test_weekly_schedule_uses_business_and_weekend_windows(self) -> None:
        self.assertEqual(
            fact_audiencia_sched_every_w.cron_schedule,
            [
                "0 7-17/2 * * 1-5",
                "0 23 * * 1-5",
                "0 11,23 * * 0,6",
            ],
        )
        self.assertEqual(
            fact_audiencia_sched_every_w.execution_timezone,
            "America/Ciudad_Juarez",
        )
        self.assertEqual(
            fact_audiencia_sched_every_w.default_status,
            DefaultScheduleStatus.RUNNING,
        )


if __name__ == "__main__":
    unittest.main()
