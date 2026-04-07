import json
import logging

from meme_detector.logging_utils import (
    JsonLogFormatter,
    LogContextFilter,
    bind_log_context,
    reset_log_context,
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
