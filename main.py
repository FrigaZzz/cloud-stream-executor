from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import string
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, AsyncGenerator, Coroutine
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, model_validator


TRUTHY_ENV_VALUES = {"1", "true", "yes", "on"}


def _env_flag(name: str, *, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in TRUTHY_ENV_VALUES


DEBUG_MODE = _env_flag("DEBUG")
LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG" if DEBUG_MODE else "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))


def _format_log_value(value: Any) -> str:
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, int | float):
        return str(value)

    text = str(value)
    if not text or any(char.isspace() for char in text):
        return json.dumps(text, separators=(",", ":"))
    return text


def _log_event(level: int, event: str, **fields: Any) -> None:
    field_text = " ".join(
        f"{key}={_format_log_value(value)}"
        for key, value in fields.items()
        if value is not None
    )
    message = f"event={event} {field_text}" if field_text else f"event={event}"
    logger.log(level, message)


def _payload_for_log(payload: str) -> str:
    return payload.replace("\n", "\\n")


def _extract_sse_event_type(payload: str) -> str | None:
    for line in payload.splitlines():
        if not line.startswith("data:"):
            continue

        try:
            decoded = json.loads(line.removeprefix("data:").strip())
        except json.JSONDecodeError:
            return None

        if isinstance(decoded, dict):
            event_type = decoded.get("type")
            if isinstance(event_type, str):
                return event_type

    return None


def _extract_cloud_trace_context(header_value: str | None) -> str | None:
    if not header_value:
        return None

    return header_value.split("/", 1)[0].split(";", 1)[0].strip() or None


def _normalize_sse_payload(payload: str, event_id: int) -> str:
    stripped = payload.rstrip("\n")
    lines = stripped.splitlines() if stripped else []

    if lines and lines[0].startswith("id:"):
        return "\n".join(lines) + "\n\n"

    lines.insert(0, f"id: {event_id}")
    return "\n".join(lines) + "\n\n"


def make_sse_data(payload: dict[str, Any]) -> str:
    return f"data: {json.dumps(payload, separators=(',', ':'))}"


@dataclass(slots=True)
class BufferedSSEEvent:
    seq: int
    payload: str
    terminal: bool = False
    created_at: float = field(default_factory=time.time)


@dataclass(slots=True)
class DetachedSSEJob:
    job_id: str
    request_id: str
    cloud_trace_id: str | None = None
    created_at: float = field(default_factory=time.time)
    condition: asyncio.Condition = field(default_factory=asyncio.Condition)
    events: list[BufferedSSEEvent] = field(default_factory=list)
    next_seq: int = 1
    task: asyncio.Task[None] | None = None
    done: bool = False
    cancelled: bool = False
    stream_closed: bool = False
    error: str | None = None
    finished_at: float | None = None


class DetachedSSEManager:
    def __init__(
        self,
        *,
        keepalive_seconds: float = 10.0,
        retention_seconds: int = 300,
        max_buffered_events: int = 100,
    ) -> None:
        self.keepalive_seconds = keepalive_seconds
        self.retention_seconds = retention_seconds
        self.max_buffered_events = max_buffered_events
        self._jobs: dict[str, DetachedSSEJob] = {}
        self._jobs_lock = asyncio.Lock()

    async def create_job(
        self,
        job_id: str | None = None,
        *,
        request_id: str | None = None,
        cloud_trace_id: str | None = None,
    ) -> DetachedSSEJob:
        resolved_job_id = job_id or str(uuid4())
        job = DetachedSSEJob(
            job_id=resolved_job_id,
            request_id=request_id or resolved_job_id,
            cloud_trace_id=cloud_trace_id,
        )

        async with self._jobs_lock:
            self._prune_finished_jobs_locked()
            self._jobs[job.job_id] = job

        _log_event(
            logging.INFO,
            "stream.job.created",
            request_id=job.request_id,
            job_id=job.job_id,
            cloud_trace_id=job.cloud_trace_id,
        )

        return job

    async def get_job(self, job_id: str) -> DetachedSSEJob | None:
        async with self._jobs_lock:
            self._prune_finished_jobs_locked()
            return self._jobs.get(job_id)

    async def start_job(
        self,
        job_id: str,
        producer_coro: Coroutine[Any, Any, None],
    ) -> None:
        job = await self.get_job(job_id)
        if job is None:
            raise KeyError(job_id)

        if job.task is not None:
            raise RuntimeError(f"Job {job_id} already started")

        job.task = asyncio.create_task(
            self._run_job(job_id, producer_coro),
            name=f"detached-sse-{job_id}",
        )
        _log_event(
            logging.INFO,
            "stream.job.scheduled",
            request_id=job.request_id,
            job_id=job.job_id,
            cloud_trace_id=job.cloud_trace_id,
        )

    async def append(
        self,
        job_id: str,
        payload: str,
        *,
        terminal: bool = False,
    ) -> int:
        job = await self.get_job(job_id)
        if job is None:
            raise KeyError(job_id)

        async with job.condition:
            seq = job.next_seq
            job.next_seq += 1

            normalized_payload = _normalize_sse_payload(payload, seq)
            event_type = _extract_sse_event_type(normalized_payload)

            if not job.stream_closed:
                job.events.append(
                    BufferedSSEEvent(
                        seq=seq,
                        payload=normalized_payload,
                        terminal=terminal,
                    )
                )
                self._trim_live_buffer_locked(job)
                _log_event(
                    logging.INFO,
                    "sse.packet.buffered",
                    request_id=job.request_id,
                    job_id=job.job_id,
                    cloud_trace_id=job.cloud_trace_id,
                    seq=seq,
                    event_type=event_type,
                    terminal=terminal,
                    bytes=len(normalized_payload.encode("utf-8")),
                    buffered_events=len(job.events),
                    payload=_payload_for_log(normalized_payload),
                )
            else:
                _log_event(
                    logging.WARNING,
                    "sse.packet.dropped_stream_closed",
                    request_id=job.request_id,
                    job_id=job.job_id,
                    cloud_trace_id=job.cloud_trace_id,
                    seq=seq,
                    event_type=event_type,
                    terminal=terminal,
                    bytes=len(normalized_payload.encode("utf-8")),
                    payload=_payload_for_log(normalized_payload),
                )

            if terminal:
                job.done = True
                job.finished_at = time.time()

            job.condition.notify_all()
            return seq

    async def mark_done(self, job_id: str) -> None:
        job = await self.get_job(job_id)
        if job is None:
            return

        async with job.condition:
            job.done = True
            job.finished_at = job.finished_at or time.time()
            job.condition.notify_all()

    async def mark_error(self, job_id: str, message: str) -> None:
        job = await self.get_job(job_id)
        if job is None:
            return

        async with job.condition:
            job.error = message
            job.done = True
            job.finished_at = time.time()

            seq = job.next_seq
            job.next_seq += 1
            error_payload = make_sse_data(
                {
                    "type": "error",
                    "job_id": job_id,
                    "error": message,
                }
            )
            normalized_payload = _normalize_sse_payload(error_payload, seq)

            if not job.stream_closed:
                job.events.append(
                    BufferedSSEEvent(
                        seq=seq,
                        payload=normalized_payload,
                        terminal=True,
                    )
                )
                _log_event(
                    logging.ERROR,
                    "sse.packet.buffered",
                    request_id=job.request_id,
                    job_id=job.job_id,
                    cloud_trace_id=job.cloud_trace_id,
                    seq=seq,
                    event_type="error",
                    terminal=True,
                    bytes=len(normalized_payload.encode("utf-8")),
                    buffered_events=len(job.events),
                    payload=_payload_for_log(normalized_payload),
                )
            else:
                _log_event(
                    logging.ERROR,
                    "sse.packet.dropped_stream_closed",
                    request_id=job.request_id,
                    job_id=job.job_id,
                    cloud_trace_id=job.cloud_trace_id,
                    seq=seq,
                    event_type="error",
                    terminal=True,
                    bytes=len(normalized_payload.encode("utf-8")),
                    payload=_payload_for_log(normalized_payload),
                )

            job.condition.notify_all()

    async def cancel_job(self, job_id: str, *, reason: str = "cancelled") -> bool:
        job = await self.get_job(job_id)
        if job is None:
            return False

        job.cancelled = True
        task = job.task
        if task is not None and not task.done():
            _log_event(
                logging.WARNING,
                "stream.job.cancelling",
                request_id=job.request_id,
                job_id=job.job_id,
                cloud_trace_id=job.cloud_trace_id,
                reason=reason,
            )
            task.cancel(reason)

        async with job.condition:
            job.done = True
            job.finished_at = time.time()
            job.condition.notify_all()

        return True

    async def stream(self, *, job_id: str) -> AsyncGenerator[str, None]:
        job = await self.get_job(job_id)
        if job is None:
            raise KeyError(job_id)

        _log_event(
            logging.INFO,
            "stream.response.opened",
            request_id=job.request_id,
            job_id=job.job_id,
            cloud_trace_id=job.cloud_trace_id,
        )

        try:
            while True:
                keepalive = False

                async with job.condition:
                    while not job.events and not job.done:
                        try:
                            await asyncio.wait_for(
                                job.condition.wait(),
                                timeout=self.keepalive_seconds,
                            )
                        except TimeoutError:
                            keepalive = True
                            break

                    pending = list(job.events)
                    job.events.clear()
                    is_done = job.done and not pending

                if pending:
                    for event in pending:
                        _log_event(
                            logging.INFO,
                            "sse.packet.sent",
                            request_id=job.request_id,
                            job_id=job.job_id,
                            cloud_trace_id=job.cloud_trace_id,
                            seq=event.seq,
                            event_type=_extract_sse_event_type(event.payload),
                            terminal=event.terminal,
                            bytes=len(event.payload.encode("utf-8")),
                            payload=_payload_for_log(event.payload),
                        )
                        yield event.payload
                    continue

                if is_done:
                    return

                if keepalive:
                    keepalive_payload = ": keep-alive\n\n"
                    _log_event(
                        logging.INFO,
                        "sse.packet.sent",
                        request_id=job.request_id,
                        job_id=job.job_id,
                        cloud_trace_id=job.cloud_trace_id,
                        event_type="keepalive",
                        terminal=False,
                        bytes=len(keepalive_payload.encode("utf-8")),
                        payload=_payload_for_log(keepalive_payload),
                    )
                    yield keepalive_payload
        finally:
            await self.close_stream(job_id, reason="response_generator_closed")

    async def close_stream(self, job_id: str, *, reason: str = "stream_closed") -> None:
        job = await self.get_job(job_id)
        if job is None:
            return

        async with job.condition:
            already_closed = job.stream_closed
            job.events.clear()
            job.stream_closed = True
            job.condition.notify_all()

        closed_before_done = not job.done
        _log_event(
            logging.WARNING if closed_before_done else logging.INFO,
            (
                "stream.response.closed_before_job_done"
                if closed_before_done
                else "stream.response.closed_after_job_done"
            ),
            request_id=job.request_id,
            job_id=job.job_id,
            cloud_trace_id=job.cloud_trace_id,
            reason=reason,
            already_closed=already_closed,
            job_done=job.done,
            job_cancelled=job.cancelled,
            job_error=job.error,
            cloud_run_background_may_stop=closed_before_done,
            duration_seconds=round(time.time() - job.created_at, 3),
        )

    async def shutdown(self) -> None:
        async with self._jobs_lock:
            jobs = list(self._jobs.values())

        _log_event(
            logging.INFO,
            "stream.manager.shutdown_started",
            active_jobs=sum(
                1 for job in jobs if job.task is not None and not job.task.done()
            ),
        )

        tasks = [
            job.task
            for job in jobs
            if job.task is not None and not job.task.done()
        ]

        for job in jobs:
            if job.task is not None and not job.task.done():
                _log_event(
                    logging.WARNING,
                    "stream.job.cancelling",
                    request_id=job.request_id,
                    job_id=job.job_id,
                    cloud_trace_id=job.cloud_trace_id,
                    reason="application_shutdown",
                )
                job.task.cancel("application shutdown")

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        _log_event(logging.INFO, "stream.manager.shutdown_finished")

    async def _run_job(
        self,
        job_id: str,
        producer_coro: Coroutine[Any, Any, None],
    ) -> None:
        job = await self.get_job(job_id)
        if job is None:
            raise KeyError(job_id)

        status = "completed"
        _log_event(
            logging.INFO,
            "stream.job.background_started",
            request_id=job.request_id,
            job_id=job.job_id,
            cloud_trace_id=job.cloud_trace_id,
        )

        try:
            await producer_coro
        except asyncio.CancelledError:
            status = "cancelled"
            _log_event(
                logging.WARNING,
                "stream.job.background_cancelled",
                request_id=job.request_id,
                job_id=job.job_id,
                cloud_trace_id=job.cloud_trace_id,
                duration_seconds=round(time.time() - job.created_at, 3),
            )
            raise
        except Exception as exc:
            status = "failed"
            _log_event(
                logging.ERROR,
                "stream.job.background_failed",
                request_id=job.request_id,
                job_id=job.job_id,
                cloud_trace_id=job.cloud_trace_id,
                error=str(exc),
                duration_seconds=round(time.time() - job.created_at, 3),
            )
            logger.exception(
                "event=stream.job.background_exception request_id=%s job_id=%s cloud_trace_id=%s",
                job.request_id,
                job.job_id,
                job.cloud_trace_id,
            )
            await self.mark_error(job_id, str(exc))
        else:
            _log_event(
                logging.INFO,
                "stream.job.background_completed",
                request_id=job.request_id,
                job_id=job.job_id,
                cloud_trace_id=job.cloud_trace_id,
                duration_seconds=round(time.time() - job.created_at, 3),
            )
        finally:
            await self.mark_done(job_id)
            _log_event(
                logging.INFO,
                "stream.job.background_stopped",
                request_id=job.request_id,
                job_id=job.job_id,
                cloud_trace_id=job.cloud_trace_id,
                status=status,
                duration_seconds=round(time.time() - job.created_at, 3),
            )

    def _prune_finished_jobs_locked(self) -> None:
        if self.retention_seconds <= 0:
            return

        now = time.time()
        expired_job_ids = [
            job_id
            for job_id, job in self._jobs.items()
            if job.finished_at is not None
            and now - job.finished_at >= self.retention_seconds
        ]

        for job_id in expired_job_ids:
            self._jobs.pop(job_id, None)

    def _trim_live_buffer_locked(self, job: DetachedSSEJob) -> None:
        if self.max_buffered_events <= 0:
            job.events.clear()
            return

        overflow = len(job.events) - self.max_buffered_events
        if overflow > 0:
            del job.events[:overflow]


manager = DetachedSSEManager()


class StreamRequest(BaseModel):
    character: str | None = Field(
        default=None,
        min_length=1,
        max_length=1,
        description="Character to repeat. If omitted, a random character is chosen.",
    )
    repeat: int = Field(
        default=20,
        gt=0,
        le=10_000,
        description="Maximum number of chunk events to produce.",
    )
    total_duration_seconds: float = Field(
        default=10.0,
        gt=0,
        le=3600,
        description="Maximum total duration for the stream.",
    )
    min_delay_seconds: float = Field(
        default=0.1,
        ge=0,
        le=60,
        description="Minimum delay between streamed events.",
    )
    max_delay_seconds: float = Field(
        default=1.0,
        ge=0,
        le=60,
        description="Maximum delay between streamed events.",
    )

    @model_validator(mode="after")
    def validate_delays(self) -> StreamRequest:
        if self.min_delay_seconds > self.max_delay_seconds:
            raise ValueError("min_delay_seconds must be <= max_delay_seconds")
        return self


async def random_character_event_producer(
    *,
    job_id: str,
    request: StreamRequest,
) -> None:
    selected_character = request.character or random.choice(string.ascii_letters)
    started_at = time.monotonic()
    produced = 0

    await manager.append(
        job_id,
        make_sse_data(
            {
                "type": "start",
                "job_id": job_id,
                "character": selected_character,
                "repeat": request.repeat,
                "total_duration_seconds": request.total_duration_seconds,
            }
        ),
    )

    while produced < request.repeat:
        elapsed = time.monotonic() - started_at
        remaining = request.total_duration_seconds - elapsed

        if remaining <= 0:
            break

        produced += 1
        await manager.append(
            job_id,
            make_sse_data(
                {
                    "type": "chunk",
                    "job_id": job_id,
                    "seq": produced,
                    "char": selected_character,
                }
            ),
        )

        if produced >= request.repeat:
            break

        delay = random.uniform(
            request.min_delay_seconds,
            request.max_delay_seconds,
        )
        sleep_for = min(delay, max(0.0, remaining))

        if sleep_for > 0:
            await asyncio.sleep(sleep_for)

    await manager.append(
        job_id,
        make_sse_data(
            {
                "type": "end",
                "job_id": job_id,
                "produced": produced,
                "character": selected_character,
            }
        ),
        terminal=True,
    )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    _log_event(
        logging.INFO,
        "app.started",
        debug=DEBUG_MODE,
        log_level=LOG_LEVEL,
    )
    yield
    _log_event(logging.INFO, "app.shutting_down")
    await manager.shutdown()


app = FastAPI(
    title="Detached HTTP Streaming Demo",
    version="1.0.0",
    lifespan=lifespan,
    debug=DEBUG_MODE,
)


@app.get("/")
async def root() -> dict[str, str]:
    return {
        "status": "ok",
        "message": "service is running",
    }


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "healthy"}


@app.post("/stream")
async def stream_random_character_events(
    http_request: Request,
    stream_request: StreamRequest,
) -> StreamingResponse:
    cloud_trace_id = _extract_cloud_trace_context(
        http_request.headers.get("x-cloud-trace-context")
    )
    request_id = http_request.headers.get("x-request-id")
    job = await manager.create_job(
        request_id=request_id,
        cloud_trace_id=cloud_trace_id,
    )
    _log_event(
        logging.INFO,
        "stream.request.accepted",
        request_id=job.request_id,
        job_id=job.job_id,
        cloud_trace_id=job.cloud_trace_id,
        client_host=http_request.client.host if http_request.client else None,
        repeat=stream_request.repeat,
        total_duration_seconds=stream_request.total_duration_seconds,
        min_delay_seconds=stream_request.min_delay_seconds,
        max_delay_seconds=stream_request.max_delay_seconds,
    )
    await manager.start_job(
        job.job_id,
        random_character_event_producer(
            job_id=job.job_id,
            request=stream_request,
        ),
    )

    async def response_generator() -> AsyncGenerator[str, None]:
        async for payload in manager.stream(job_id=job.job_id):
            yield payload

    return StreamingResponse(
        response_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
            "X-Request-ID": job.request_id,
            "X-Job-ID": job.job_id,
        },
    )
