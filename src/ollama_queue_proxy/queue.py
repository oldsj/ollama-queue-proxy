"""Priority queue: three-tier asyncio queues with event-based worker."""

from __future__ import annotations

import asyncio
import logging
import math
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from .config import QueueConfig

logger = logging.getLogger(__name__)

TIERS = ("high", "normal", "low")


@dataclass
class QueueItem:
    tier: str
    enqueue_time: float
    request_id: str
    future: asyncio.Future
    dispatch_fn: Callable[[], Awaitable[Any]]
    position: int = 0
    client_id: str | None = None
    body_size: int = 0
    try_acquire: Callable[[], bool] | None = None
    release: Callable[[], None] | None = None


@dataclass
class QueueReservation:
    tier: str
    body_bytes: int
    active: bool = True


@dataclass
class TierStats:
    processed: int = 0
    rejected: int = 0
    expired: int = 0
    # Rolling window of recent wait times (last 20)
    recent_waits: deque = field(default_factory=lambda: deque(maxlen=20))

    def mean_wait(self) -> float:
        if len(self.recent_waits) < 3:
            return 5.0
        return sum(self.recent_waits) / len(self.recent_waits)


class PriorityQueueManager:
    def __init__(
        self,
        config: QueueConfig,
        max_concurrent: int,
        max_buffered_body_bytes: int = 256 * 1024 * 1024,
    ) -> None:
        self._config = config
        tier_cfgs = {
            "high": config.high,
            "normal": config.normal,
            "low": config.low,
        }
        self._queues: dict[str, asyncio.Queue] = {
            t: asyncio.Queue(maxsize=tier_cfgs[t].max_depth) for t in TIERS
        }
        self._max_waits = {t: tier_cfgs[t].max_wait for t in TIERS}
        self._watermark_pcts = {t: tier_cfgs[t].high_watermark_pct for t in TIERS}
        self._paused: set[str] = set()
        self._stats: dict[str, TierStats] = {t: TierStats() for t in TIERS}
        self._max_concurrent = max_concurrent
        self._max_buffered_body_bytes = max_buffered_body_bytes
        self._buffered_body_bytes = 0
        self._reserved_slots: dict[str, int] = {t: 0 for t in TIERS}
        self._active = 0
        self._active_lock = asyncio.Lock()
        self._has_items = asyncio.Event()
        self._worker_tasks: list[asyncio.Task] = []
        self._reaper_task: asyncio.Task | None = None
        self._overflow_code = config.overflow_status_code
        self._watermark_fired: set[str] = set()
        self._event_callbacks: list[Callable] = []

    def add_event_callback(self, cb: Callable) -> None:
        self._event_callbacks.append(cb)

    async def _fire_event(self, event: str, tier: str | None = None, **kwargs) -> None:
        for cb in self._event_callbacks:
            await cb(event, tier=tier, **kwargs)

    def start_workers(self) -> None:
        for _ in range(self._max_concurrent):
            t = asyncio.create_task(self._worker())
            self._worker_tasks.append(t)
        self._reaper_task = asyncio.create_task(self._reaper())

    async def stop_workers(self) -> None:
        for t in self._worker_tasks:
            t.cancel()
        await asyncio.gather(*self._worker_tasks, return_exceptions=True)
        if self._reaper_task is not None:
            self._reaper_task.cancel()
            await asyncio.gather(self._reaper_task, return_exceptions=True)

    async def reserve(self, tier: str, anticipated_body_bytes: int) -> QueueReservation:
        """Reserve a queue slot and aggregate body-memory capacity before reading."""
        q = self._queues[tier]
        if tier in self._paused:
            raise QueuePaused(tier)
        if q.qsize() + self._reserved_slots[tier] >= q.maxsize:
            self._stats[tier].rejected += 1
            await self._fire_event("queue.full", tier=tier)
            raise QueueFull(tier, self._overflow_code)
        if self._buffered_body_bytes + anticipated_body_bytes > self._max_buffered_body_bytes:
            self._stats[tier].rejected += 1
            raise QueueFull(tier, self._overflow_code, reason="buffered body budget exhausted")
        self._reserved_slots[tier] += 1
        self._buffered_body_bytes += anticipated_body_bytes
        return QueueReservation(tier=tier, body_bytes=anticipated_body_bytes)

    def resize_reservation(self, reservation: QueueReservation, actual_bytes: int) -> None:
        if not reservation.active:
            raise RuntimeError("queue reservation is no longer active")
        delta = actual_bytes - reservation.body_bytes
        if self._buffered_body_bytes + delta > self._max_buffered_body_bytes:
            raise QueueFull(
                reservation.tier,
                self._overflow_code,
                reason="buffered body budget exhausted",
            )
        self._buffered_body_bytes += delta
        reservation.body_bytes = actual_bytes

    def release_reservation(self, reservation: QueueReservation) -> None:
        if not reservation.active:
            return
        reservation.active = False
        self._reserved_slots[reservation.tier] -= 1
        self._buffered_body_bytes = max(0, self._buffered_body_bytes - reservation.body_bytes)

    async def enqueue(
        self, item: QueueItem, reservation: QueueReservation | None = None
    ) -> int:
        """
        Enqueue item. Returns queue position (1-based) or raises QueueFull.
        Raises QueuePaused if tier is paused.
        """
        tier = item.tier
        q = self._queues[tier]

        if tier in self._paused:
            if reservation is not None:
                self.release_reservation(reservation)
            raise QueuePaused(tier)

        if reservation is None and q.qsize() + self._reserved_slots[tier] >= q.maxsize:
            self._stats[tier].rejected += 1
            await self._fire_event("queue.full", tier=tier, client_id=item.request_id)
            raise QueueFull(tier, self._overflow_code)

        position = (
            q.qsize() + self._reserved_slots[tier]
            if reservation is not None
            else q.qsize() + 1
        )
        item.position = position
        if reservation is not None:
            if not reservation.active or reservation.tier != tier:
                raise RuntimeError("invalid queue reservation")
            reservation.active = False
            self._reserved_slots[tier] -= 1
            item.body_size = reservation.body_bytes
        else:
            if self._buffered_body_bytes + item.body_size > self._max_buffered_body_bytes:
                raise QueueFull(tier, self._overflow_code, reason="buffered body budget exhausted")
            self._buffered_body_bytes += item.body_size
        await q.put(item)
        self._has_items.set()

        # Check high watermark
        tier_cfg = getattr(self._config, tier)
        pct = (q.qsize() / tier_cfg.max_depth) * 100
        if pct >= self._watermark_pcts[tier] and tier not in self._watermark_fired:
            self._watermark_fired.add(tier)
            await self._fire_event("queue.high_watermark", tier=tier, queue_depth=q.qsize())
        elif pct < self._watermark_pcts[tier]:
            self._watermark_fired.discard(tier)

        return position

    async def _worker(self) -> None:
        while True:
            await self._has_items.wait()
            self._has_items.clear()
            item = self._pop_dispatchable()
            if item is None:
                continue

            try:
                async with self._active_lock:
                    self._active += 1
                tier = item.tier
                try:
                    result = await item.dispatch_fn()
                    completion = getattr(result, "_oqp_completion", None)
                    handed_off = not item.future.done()
                    if not item.future.done():
                        item.future.set_result(result)
                    if completion is not None:
                        if handed_off:
                            await completion
                        else:
                            abort = getattr(result, "_oqp_abort", None)
                            if abort is not None:
                                await abort()
                    wait_ms = (time.monotonic() - item.enqueue_time) * 1000
                    self._stats[tier].recent_waits.append(wait_ms / 1000)
                    self._stats[tier].processed += 1
                except Exception as e:
                    if not item.future.done():
                        item.future.set_exception(e)
            finally:
                if item.release is not None:
                    item.release()
                self._buffered_body_bytes = max(
                    0, self._buffered_body_bytes - item.body_size
                )
                async with self._active_lock:
                    self._active -= 1
                if any(not q.empty() for q in self._queues.values()):
                    self._has_items.set()
                else:
                    await self._fire_event("queue.drained", tier=None)

    def _expire_item(self, item: QueueItem) -> None:
        self._stats[item.tier].expired += 1
        if not item.future.done():
            item.future.set_exception(RequestExpired(item.tier, item.request_id))
        self._buffered_body_bytes = max(0, self._buffered_body_bytes - item.body_size)

    def _pop_dispatchable(self) -> QueueItem | None:
        now = time.monotonic()
        for tier in TIERS:
            q = self._queues[tier]
            for _ in range(q.qsize()):
                item = q.get_nowait()
                if item.future.cancelled():
                    self._buffered_body_bytes = max(
                        0, self._buffered_body_bytes - item.body_size
                    )
                    continue
                if now - item.enqueue_time > self._max_waits[tier]:
                    self._expire_item(item)
                    continue
                if item.try_acquire is None or item.try_acquire():
                    return item
                q.put_nowait(item)
        return None

    async def _reaper(self) -> None:
        while True:
            await asyncio.sleep(0.5)
            now = time.monotonic()
            for tier in TIERS:
                q = self._queues[tier]
                for _ in range(q.qsize()):
                    item = q.get_nowait()
                    if item.future.cancelled():
                        self._buffered_body_bytes = max(
                            0, self._buffered_body_bytes - item.body_size
                        )
                    elif now - item.enqueue_time > self._max_waits[tier]:
                        self._expire_item(item)
                    else:
                        q.put_nowait(item)

    def queue_depths(self) -> dict[str, int]:
        return {t: self._queues[t].qsize() for t in TIERS}

    def active_count(self) -> int:
        return self._active

    def buffered_body_bytes(self) -> int:
        return self._buffered_body_bytes

    def client_waiting_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for queue in self._queues.values():
            for item in queue._queue:
                if item.client_id is not None and not item.future.done():
                    counts[item.client_id] = counts.get(item.client_id, 0) + 1
        return counts

    def wake_workers(self) -> None:
        self._has_items.set()

    def stats(self) -> dict[str, TierStats]:
        return self._stats

    def retry_after(self, tier: str) -> int:
        q = self._queues[tier]
        depth = q.qsize()
        mean_wait = self._stats[tier].mean_wait()
        return math.ceil(depth / max(self._max_concurrent, 1) * mean_wait)

    def pause(self, tier: str | None) -> None:
        tiers = TIERS if tier is None else (tier,)
        for t in tiers:
            self._paused.add(t)

    def resume(self, tier: str | None) -> None:
        tiers = TIERS if tier is None else (tier,)
        for t in tiers:
            self._paused.discard(t)

    async def flush(self, tier: str | None) -> int:
        """Drop all pending items in tier(s). Returns count of dropped items."""
        tiers = TIERS if tier is None else (tier,)
        dropped = 0
        for t in tiers:
            while not self._queues[t].empty():
                try:
                    item = self._queues[t].get_nowait()
                    if not item.future.done():
                        item.future.set_exception(QueueFlushed(t))
                    self._buffered_body_bytes = max(
                        0, self._buffered_body_bytes - item.body_size
                    )
                    dropped += 1
                except asyncio.QueueEmpty:
                    break
        return dropped

    async def drain(self) -> None:
        """Wait until queued and active requests are complete."""
        while self._active or any(not q.empty() for q in self._queues.values()):
            await asyncio.sleep(0.1)


class QueueFull(Exception):
    def __init__(self, tier: str, status_code: int, reason: str = "queue full") -> None:
        self.tier = tier
        self.status_code = status_code
        self.reason = reason


class QueuePaused(Exception):
    def __init__(self, tier: str) -> None:
        self.tier = tier


class QueueFlushed(Exception):
    def __init__(self, tier: str) -> None:
        self.tier = tier


class RequestExpired(Exception):
    def __init__(self, tier: str, request_id: str) -> None:
        self.tier = tier
        self.request_id = request_id
