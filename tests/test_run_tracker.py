from meme_detector.miner.models import MinerInsightsRunResult
from meme_detector.researcher.models import ResearchRunResult
from meme_detector.run_tracker import _build_job_summary
from meme_detector.scout.models import ScoutRunResult


def test_build_job_summary_handles_model_and_dict_consistently_for_scout():
    model_result = ScoutRunResult(target_date="2026-04-17", video_count=3, comment_count=6)
    dict_result = {"target_date": "2026-04-17", "video_count": 3, "comment_count": 6}

    model_summary = _build_job_summary("scout", model_result)
    dict_summary = _build_job_summary("scout", dict_result)

    assert model_summary == dict_summary


def test_build_job_summary_handles_model_and_dict_consistently_for_miner_insights():
    model_result = MinerInsightsRunResult(
        target_date="2026-04-17",
        video_count=4,
        insight_count=9,
        high_value_count=2,
        failed_video_count=1,
    )
    dict_result = {
        "target_date": "2026-04-17",
        "video_count": 4,
        "insight_count": 9,
        "high_value_count": 2,
        "failed_video_count": 1,
    }

    model_summary = _build_job_summary("miner_insights", model_result)
    dict_summary = _build_job_summary("miner_insights", dict_result)

    assert model_summary == dict_summary


def test_build_job_summary_handles_model_and_dict_consistently_for_research():
    model_result = ResearchRunResult(
        pending_count=3,
        adjudicated_count=3,
        accepted_count=1,
        rejected_count=1,
        failed_bundle_ids=["bundle-1"],
    )
    dict_result = model_result.model_dump(mode="json")

    model_summary = _build_job_summary("research", model_result)
    dict_summary = _build_job_summary("research", dict_result)

    assert model_summary == dict_summary
