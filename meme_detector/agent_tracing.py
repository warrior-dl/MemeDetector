from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime
import os
from typing import Any
from uuid import uuid4

from meme_detector.config import settings
from meme_detector.logging_utils import get_logger

logger = get_logger(__name__)

_LANGFUSE_IMPORT_ERROR: Exception | None = None
_LANGFUSE_CLIENT: Any | None = None

if settings.langfuse_public_key.strip():
    os.environ.setdefault("LANGFUSE_PUBLIC_KEY", settings.langfuse_public_key.strip())
if settings.langfuse_secret_key.strip():
    os.environ.setdefault("LANGFUSE_SECRET_KEY", settings.langfuse_secret_key.strip())
if settings.langfuse_host.strip():
    os.environ.setdefault("LANGFUSE_HOST", settings.langfuse_host.strip())

try:
    from langfuse import Langfuse, get_client as _get_langfuse_client  # type: ignore
except Exception as exc:  # pragma: no cover - optional dependency
    Langfuse = None  # type: ignore
    _get_langfuse_client = None
    _LANGFUSE_IMPORT_ERROR = exc

try:
    from langfuse.openai import AsyncOpenAI as _LangfuseAsyncOpenAI  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    _LangfuseAsyncOpenAI = None


def is_langfuse_enabled() -> bool:
    return bool(
        settings.langfuse_enabled
        and settings.langfuse_host.strip()
        and settings.langfuse_public_key.strip()
        and settings.langfuse_secret_key.strip()
    )


def resolve_async_openai_client_cls(default_cls: type) -> type:
    if not is_langfuse_enabled() or _LangfuseAsyncOpenAI is None:
        return default_cls
    return _LangfuseAsyncOpenAI


def get_langfuse_client() -> Any | None:
    global _LANGFUSE_CLIENT
    if not is_langfuse_enabled() or Langfuse is None:
        return None
    if _LANGFUSE_CLIENT is not None:
        return _LANGFUSE_CLIENT
    try:
        _LANGFUSE_CLIENT = Langfuse(
            public_key=settings.langfuse_public_key.strip(),
            secret_key=settings.langfuse_secret_key.strip(),
            host=settings.langfuse_host.strip(),
        )
        return _LANGFUSE_CLIENT
    except Exception:  # pragma: no cover - network/config dependent
        logger.warning(
            "langfuse client bootstrap failed",
            extra={"event": "langfuse_client_bootstrap_failed"},
            exc_info=True,
        )
        return None


@dataclass
class LangfuseTraceContext:
    trace_id: str = ""
    trace_url: str = ""
    span: Any | None = None


@contextmanager
def start_langfuse_trace(
    *,
    name: str,
    session_id: str | None,
    metadata: dict[str, Any] | None = None,
) -> Any:
    if not is_langfuse_enabled():
        yield LangfuseTraceContext()
        return

    client = get_langfuse_client()
    if client is None:
        yield LangfuseTraceContext()
        return
    span = None
    try:
        span = client.start_as_current_observation(name=name, as_type="span")
    except Exception:  # pragma: no cover - SDK shape may differ across versions
        logger.warning(
            "langfuse span bootstrap failed",
            extra={"event": "langfuse_span_bootstrap_failed"},
            exc_info=True,
        )
        yield LangfuseTraceContext()
        return

    with span as current_span:
        try:
            enriched_metadata = dict(metadata or {})
            if session_id:
                enriched_metadata.setdefault("session_id", session_id)
            update_payload = {"metadata": enriched_metadata}
            if hasattr(current_span, "update"):
                current_span.update(**update_payload)
        except Exception:  # pragma: no cover - best effort only
            logger.debug(
                "langfuse span update skipped",
                extra={"event": "langfuse_span_update_skipped"},
                exc_info=True,
            )

        context = LangfuseTraceContext(span=current_span)
        try:
            yield context
        finally:
            try:
                if hasattr(client, "get_current_trace_id"):
                    context.trace_id = str(client.get_current_trace_id() or "")
                if context.trace_id and hasattr(client, "get_trace_url"):
                    context.trace_url = str(client.get_trace_url(trace_id=context.trace_id) or "")
                elif context.trace_id:
                    context.trace_url = build_langfuse_public_url(context.trace_id)
                if hasattr(client, "flush"):
                    client.flush()
            except Exception:  # pragma: no cover - best effort only
                logger.debug(
                    "langfuse trace metadata unavailable",
                    extra={"event": "langfuse_trace_metadata_unavailable"},
                    exc_info=True,
                )


@dataclass
class TraceStep:
    id: str
    step_index: int
    event_type: str
    stage: str
    title: str
    status: str
    started_at: datetime
    finished_at: datetime
    duration_ms: int
    summary: str = ""
    input: Any = None
    output: Any = None
    metadata: dict[str, Any] = field(default_factory=dict)
    parent_event_id: str | None = None
    is_user_visible: bool = True
    langfuse_observation_id: str = ""


@dataclass
class TraceTimelineBuilder:
    conversation_id: str
    run_id: str
    agent_name: str
    entity_type: str
    entity_id: str
    steps: list[TraceStep] = field(default_factory=list)
    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0

    def add_step(
        self,
        *,
        event_type: str,
        stage: str,
        title: str,
        status: str,
        summary: str = "",
        input_data: Any = None,
        output_data: Any = None,
        metadata: dict[str, Any] | None = None,
        started_at: datetime | None = None,
        finished_at: datetime | None = None,
        parent_event_id: str | None = None,
        is_user_visible: bool = True,
        langfuse_observation_id: str = "",
    ) -> None:
        started = started_at or datetime.now()
        finished = finished_at or datetime.now()
        self.steps.append(
            TraceStep(
                id=uuid4().hex,
                step_index=len(self.steps) + 1,
                event_type=event_type,
                stage=stage,
                title=title,
                status=status,
                started_at=started,
                finished_at=finished,
                duration_ms=max(0, int((finished - started).total_seconds() * 1000)),
                summary=summary,
                input=_json_safe(input_data),
                output=_json_safe(output_data),
                metadata=_json_safe(metadata or {}),
                parent_event_id=parent_event_id,
                is_user_visible=is_user_visible,
                langfuse_observation_id=langfuse_observation_id,
            )
        )

    def add_llm_usage(self, usage: dict[str, Any] | None) -> None:
        if not isinstance(usage, dict):
            return
        self.input_tokens += int(usage.get("input_tokens") or usage.get("prompt_tokens") or 0)
        self.output_tokens += int(usage.get("output_tokens") or usage.get("completion_tokens") or 0)
        self.total_tokens += int(usage.get("total_tokens") or 0)

    def token_usage(self) -> dict[str, int]:
        return {
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "total_tokens": self.total_tokens or (self.input_tokens + self.output_tokens),
        }

    def all_steps(self) -> list[dict[str, Any]]:
        return [serialize_trace_step(step) for step in self.steps]

    def public_steps(self) -> list[dict[str, Any]]:
        public_items: list[dict[str, Any]] = []
        for step in self.steps:
            if not step.is_user_visible:
                continue
            item = serialize_trace_step(step)
            public_items.append(item)
        return public_items


def serialize_trace_step(step: TraceStep) -> dict[str, Any]:
    return {
        "id": step.id,
        "parent_event_id": step.parent_event_id,
        "step_index": step.step_index,
        "event_type": step.event_type,
        "stage": step.stage,
        "title": step.title,
        "status": step.status,
        "started_at": step.started_at,
        "finished_at": step.finished_at,
        "duration_ms": step.duration_ms,
        "summary": step.summary,
        "input": deepcopy(step.input),
        "output": deepcopy(step.output),
        "metadata": deepcopy(step.metadata),
        "is_user_visible": step.is_user_visible,
        "langfuse_observation_id": step.langfuse_observation_id,
    }


def build_langfuse_public_url(trace_id: str) -> str:
    if not trace_id:
        return ""
    base_url = settings.langfuse_public_base_url.strip() or settings.langfuse_host.strip()
    return f"{base_url.rstrip('/')}/trace/{trace_id}" if base_url else ""


def _json_safe(value: Any) -> Any:
    if value is None:
        return None
    try:
        return deepcopy(value)
    except Exception:
        return str(value)
