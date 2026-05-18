from __future__ import annotations

import asyncio
import json
import logging
import random
import string
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, AsyncGenerator, Coroutine
from uuid import uuid4

from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, model_validator


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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

    async def create_job(self, job_id: str | None = None) -> DetachedSSEJob:
        job = DetachedSSEJob(job_id=job_id or str(uuid4()))

        async with self._jobs_lock:
            self._prune_finished_jobs_locked()
            self._jobs[job.job_id] = job

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

            if not job.stream_closed:
                normalized_payload = _normalize_sse_payload(payload, seq)
                job.events.append(
                    BufferedSSEEvent(
                        seq=seq,
                        payload=normalized_payload,
                        terminal=terminal,
                    )
                )
                self._trim_live_buffer_locked(job)

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

            if not job.stream_closed:
                job.events.append(
                    BufferedSSEEvent(
                        seq=seq,
                        payload=_normalize_sse_payload(error_payload, seq),
                        terminal=True,
                    )
                )

            job.condition.notify_all()

    async def cancel_job(self, job_id: str, *, reason: str = "cancelled") -> bool:
        job = await self.get_job(job_id)
        if job is None:
            return False

        job.cancelled = True
        task = job.task
        if task is not None and not task.done():
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
                        yield event.payload
                    continue

                if is_done:
                    return

                if keepalive:
                    yield ": keep-alive\n\n"
        finally:
            await self.close_stream(job_id)

    async def close_stream(self, job_id: str) -> None:
        job = await self.get_job(job_id)
        if job is None:
            return

        async with job.condition:
            job.events.clear()
            job.stream_closed = True
            job.condition.notify_all()

    async def shutdown(self) -> None:
        async with self._jobs_lock:
            jobs = list(self._jobs.values())

        tasks = [
            job.task
            for job in jobs
            if job.task is not None and not job.task.done()
        ]

        for task in tasks:
            task.cancel("application shutdown")

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _run_job(
        self,
        job_id: str,
        producer_coro: Coroutine[Any, Any, None],
    ) -> None:
        try:
            await producer_coro
        except asyncio.CancelledError:
            logger.info("Detached SSE job cancelled: %s", job_id)
            raise
        except Exception as exc:
            logger.exception("Detached SSE job failed: %s", job_id)
            await self.mark_error(job_id, str(exc))
        finally:
            await self.mark_done(job_id)

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
    logger.info("FastAPI app started")
    yield
    logger.info("FastAPI app shutting down")
    await manager.shutdown()


app = FastAPI(
    title="Detached HTTP Streaming Demo",
    version="1.0.0",
    lifespan=lifespan,
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
async def stream_random_character_events(request: StreamRequest) -> StreamingResponse:
    job = await manager.create_job()
    await manager.start_job(
        job.job_id,
        random_character_event_producer(
            job_id=job.job_id,
            request=request,
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
        },
    )
