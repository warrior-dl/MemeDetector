import asyncio

import pytest

from meme_detector.scout.collector import (
    CommentRiskState,
    _compute_comment_retry_delay,
    _extract_status_code,
    _is_retryable_comment_error,
    _is_risk_control_error,
)


class DummyError(Exception):
    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        if status_code is not None:
            self.status_code = status_code


class TestStatusDetection:
    def test_extract_status_code_from_attribute(self):
        err = DummyError("network error", status_code=412)
        assert _extract_status_code(err) == 412

    def test_extract_status_code_from_error_text(self):
        err = DummyError("网络错误，状态码：412 - <!DOCTYPE html>")
        assert _extract_status_code(err) == 412

    def test_detects_risk_control_html(self):
        err = DummyError(
            "The request was rejected because of the bilibili security control policy."
        )
        assert _is_risk_control_error(err) is True

    def test_retryable_status_codes_include_429(self):
        err = DummyError("too many requests", status_code=429)
        assert _is_retryable_comment_error(err) is True


class TestRiskState:
    @pytest.mark.asyncio
    async def test_cooldown_window_skips_comments(self, monkeypatch):
        monkeypatch.setattr(
            "meme_detector.scout.collector.settings.scout_risk_skip_threshold",
            1,
        )
        state = CommentRiskState()
        state.note_risk_hit(0.05)

        assert state.should_skip_comments() is True
        await asyncio.sleep(0.06)
        assert state.should_skip_comments() is False

    def test_success_resets_cooldown(self, monkeypatch):
        monkeypatch.setattr(
            "meme_detector.scout.collector.settings.scout_risk_skip_threshold",
            1,
        )
        state = CommentRiskState()
        state.note_risk_hit(10.0)

        state.note_success()

        assert state.consecutive_hits == 0
        assert state.remaining_cooldown() == 0.0
        assert state.should_skip_comments() is False


class TestRetryDelay:
    def test_risk_control_uses_cooldown_floor(self, monkeypatch):
        monkeypatch.setattr(
            "meme_detector.scout.collector.settings.scout_comment_backoff_base",
            3.0,
        )
        monkeypatch.setattr(
            "meme_detector.scout.collector.settings.scout_risk_cooldown_seconds",
            20.0,
        )
        monkeypatch.setattr(
            "meme_detector.scout.collector.random.uniform",
            lambda _a, _b: 0.0,
        )

        err = DummyError("network error", status_code=412)

        assert _compute_comment_retry_delay(0, err) == 20.0
