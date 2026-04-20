import pytest

from meme_detector import scheduler


class _FakeJob:
    def __init__(self, job_id: str, name: str):
        self.id = job_id
        self.name = name
        self.next_run_time = None
        self.trigger = "cron"


class _FakeScheduler:
    def __init__(self, *, event_loop, timezone):
        self.event_loop = event_loop
        self.timezone = timezone
        self.running = False
        self.jobs: list[dict] = []
        self.shutdown_called = False

    def add_job(self, **kwargs):
        self.jobs.append(kwargs)

    def start(self):
        self.running = True

    def shutdown(self, wait=False):
        self.shutdown_called = True
        self.running = False

    def get_jobs(self):
        return [_FakeJob(job["id"], job["name"]) for job in self.jobs]


@pytest.mark.asyncio
async def test_start_scheduler_uses_current_event_loop(monkeypatch):
    scheduler.shutdown_scheduler()
    fake_scheduler_instances: list[_FakeScheduler] = []

    def fake_asyncio_scheduler(*, event_loop, timezone):
        instance = _FakeScheduler(event_loop=event_loop, timezone=timezone)
        fake_scheduler_instances.append(instance)
        return instance

    monkeypatch.setattr("meme_detector.scheduler.AsyncIOScheduler", fake_asyncio_scheduler)

    scheduler.start_scheduler()

    try:
        assert len(fake_scheduler_instances) == 1
        fake_scheduler = fake_scheduler_instances[0]
        assert fake_scheduler.event_loop is not None
        assert fake_scheduler.running is True
        assert [job["id"] for job in fake_scheduler.jobs] == [
            "daily_scout",
            "daily_miner_insights",
            "daily_miner_bundles",
            "weekly_research",
        ]
        assert all(job["func"] is scheduler._scheduled_job for job in fake_scheduler.jobs)
    finally:
        scheduler.shutdown_scheduler()


@pytest.mark.asyncio
async def test_scheduled_job_dispatches_via_pipeline_service(monkeypatch):
    recorded: dict[str, object] = {}

    async def fake_start_background_job(job_name: str, *, trigger_mode: str = "manual") -> dict:
        recorded["job_name"] = job_name
        recorded["trigger_mode"] = trigger_mode
        return {"job_name": job_name, "started": True}

    monkeypatch.setattr(
        "meme_detector.pipeline_service.start_background_job",
        fake_start_background_job,
    )

    await scheduler._scheduled_job("miner_bundles")

    assert recorded == {
        "job_name": "miner_bundles",
        "trigger_mode": "scheduled",
    }
