import json
import logging

from meme_detector.logging_utils import (
    ConsoleLogFormatter,
    JsonLogFormatter,
    LogContextFilter,
    bind_log_context,
    reset_log_context,
    setup_logging,
)


def test_json_log_formatter_includes_context_and_extra_fields():
    logger = logging.getLogger("tests.logging")
    token = bind_log_context(run_id="run-1", job_name="research")
    try:
        record = logger.makeRecord(
            name="tests.logging",
            level=logging.INFO,
            fn=__file__,
            lno=10,
            msg="candidate accepted",
            args=(),
            exc_info=None,
            extra={"event": "research_candidate_accepted", "word": "放蚊山"},
        )
        LogContextFilter().filter(record)
        payload = json.loads(JsonLogFormatter().format(record))
    finally:
        reset_log_context(token)

    assert payload["message"] == "candidate accepted"
    assert payload["event"] == "research_candidate_accepted"
    assert payload["word"] == "放蚊山"
    assert payload["run_id"] == "run-1"
    assert payload["job_name"] == "research"


def test_console_log_formatter_renders_key_fields():
    logger = logging.getLogger("tests.logging.console")
    token = bind_log_context(run_id="run-2", job_name="miner")
    try:
        record = logger.makeRecord(
            name="tests.logging.console",
            level=logging.INFO,
            fn=__file__,
            lno=30,
            msg="chunk completed",
            args=(),
            exc_info=None,
            extra={"bvid": "BV1TEST001", "result_count": 8, "high_value_count": 3},
        )
        LogContextFilter().filter(record)
        rendered = ConsoleLogFormatter().format(record)
    finally:
        reset_log_context(token)

    assert "chunk completed" in rendered
    assert "run_id=run-2" in rendered
    assert "job_name=miner" in rendered
    assert "bvid=BV1TEST001" in rendered
    assert "result_count=8" in rendered


def test_setup_logging_disables_uvicorn_access_log():
    setup_logging()

    access_logger = logging.getLogger("uvicorn.access")

    assert access_logger.disabled is True
    assert access_logger.propagate is False
