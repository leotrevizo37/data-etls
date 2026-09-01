import unittest
from pathlib import Path


SCHEDULE_SOURCE = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "bafar"
    / "defs"
    / "schedules"
    / "manager_access_daily_schedule.py"
)


class ManagerAccessDailyScheduleTests(unittest.TestCase):
    def test_daily_schedule_uses_configured_local_cutoff_window(self) -> None:
        source = SCHEDULE_SOURCE.read_text()

        self.assertIn(
            'name="024_bafar_manager_access_daily_every_1d"',
            source,
        )
        self.assertIn(
            'job_name="024_bafar_manager_access_daily"',
            source,
        )
        self.assertIn('cron_schedule="40 21 * * *"', source)
        self.assertIn(
            'execution_timezone="America/Ciudad_Juarez"',
            source,
        )
        self.assertIn(
            "default_status=DefaultScheduleStatus.RUNNING",
            source,
        )


if __name__ == "__main__":
    unittest.main()
