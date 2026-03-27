#!/usr/bin/env python3
"""In-memory async task queue with retries and timeouts. Stdlib only."""

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Awaitable, Callable, Dict, Optional


class Status(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class TaskHandle:
    """Handle for a submitted task."""
    id: str
    status: Status = Status.PENDING
    _future: asyncio.Future = field(default=None, repr=False)
    _retries_left: int = 0
    _attempts: int = 0

    async def result(self) -> Any:
        if self._future is None:
            raise RuntimeError("Task not started")
        return await self._future

    def cancel(self) -> bool:
        if self.status in (Status.DONE, Status.FAILED, Status.CANCELLED):
            return False
        self.status = Status.CANCELLED
        if self._future and not self._future.done():
            self._future.cancel()
        return True


class TaskQueue:
    """Async task queue with concurrency limits, retries, and timeouts."""

    def __init__(self, max_concurrency: int = 4):
        self._semaphore = asyncio.Semaphore(max_concurrency)
        self._tasks: Dict[str, TaskHandle] = {}
        self._running: list[asyncio.Task] = []
        self._counter = 0

    @property
    def stats(self) -> dict:
        counts = {"pending": 0, "running": 0, "done": 0, "failed": 0, "cancelled": 0}
        for h in self._tasks.values():
            counts[h.status.value] += 1
        return counts

    def submit(
        self,
        fn: Callable[..., Awaitable],
        *args: Any,
        retries: int = 3,
        timeout_sec: float = 30,
    ) -> TaskHandle:
        self._counter += 1
        task_id = f"task-{self._counter}"
        handle = TaskHandle(id=task_id, _retries_left=retries)
        handle._future = asyncio.get_event_loop().create_future()
        self._tasks[task_id] = handle

        async_task = asyncio.ensure_future(
            self._run(handle, fn, args, retries, timeout_sec)
        )
        self._running.append(async_task)
        return handle

    async def _run(
        self,
        handle: TaskHandle,
        fn: Callable,
        args: tuple,
        retries: int,
        timeout_sec: float,
    ) -> None:
        last_exc = None
        for attempt in range(retries + 1):
            if handle.status == Status.CANCELLED:
                if not handle._future.done():
                    handle._future.cancel()
                return

            handle.status = Status.RUNNING
            handle._attempts = attempt + 1
            try:
                async with asyncio.timeout(timeout_sec):
                    await self._semaphore.acquire()
                    try:
                        result = await fn(*args)
                    finally:
                        self._semaphore.release()
                handle.status = Status.DONE
                if not handle._future.done():
                    handle._future.set_result(result)
                return
            except asyncio.CancelledError:
                handle.status = Status.CANCELLED
                if not handle._future.done():
                    handle._future.cancel()
                return
            except Exception as exc:
                last_exc = exc
                handle._retries_left -= 1
                if attempt < retries:
                    delay = 2 ** attempt
                    await asyncio.sleep(delay)

        handle.status = Status.FAILED
        if not handle._future.done():
            handle._future.set_exception(last_exc or RuntimeError("Task failed"))

    async def drain(self) -> None:
        """Wait for all submitted tasks to complete."""
        if self._running:
            await asyncio.gather(*self._running, return_exceptions=True)
            self._running.clear()


async def _demo():
    q = TaskQueue(max_concurrency=2)

    async def work(name, delay):
        print(f"  [{name}] starting...")
        await asyncio.sleep(delay)
        print(f"  [{name}] done!")
        return f"{name}-result"

    async def flaky():
        import random
        if random.random() < 0.7:
            raise ValueError("random failure")
        return "success"

    print("=== Task Queue Demo ===")
    h1 = q.submit(work, "fast", 0.1)
    h2 = q.submit(work, "slow", 0.3)
    h3 = q.submit(flaky, retries=5, timeout_sec=5)

    await q.drain()
    print(f"\nStats: {q.stats}")
    print(f"h1 result: {await h1.result()}")
    print(f"h2 result: {await h2.result()}")
    try:
        print(f"h3 result: {await h3.result()}")
    except Exception as e:
        print(f"h3 failed: {e}")


if __name__ == "__main__":
    asyncio.run(_demo())
