"""Tests for task_queue.py"""

import asyncio
import unittest

from task_queue import TaskQueue, Status


class TestTaskQueue(unittest.IsolatedAsyncioTestCase):

    async def test_basic_success(self):
        q = TaskQueue()
        async def add(a, b): return a + b
        h = q.submit(add, 2, 3)
        await q.drain()
        self.assertEqual(h.status, Status.DONE)
        self.assertEqual(await h.result(), 5)

    async def test_failure_with_retries(self):
        attempts = 0
        async def fail_twice():
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise ValueError("not yet")
            return "ok"
        q = TaskQueue()
        h = q.submit(fail_twice, retries=3, timeout_sec=10)
        await q.drain()
        self.assertEqual(h.status, Status.DONE)
        self.assertEqual(await h.result(), "ok")
        self.assertEqual(attempts, 3)

    async def test_all_retries_exhausted(self):
        async def always_fail():
            raise ValueError("nope")
        q = TaskQueue()
        h = q.submit(always_fail, retries=2, timeout_sec=10)
        await q.drain()
        self.assertEqual(h.status, Status.FAILED)
        with self.assertRaises(ValueError):
            await h.result()

    async def test_timeout(self):
        async def slow():
            await asyncio.sleep(100)
        q = TaskQueue()
        h = q.submit(slow, retries=0, timeout_sec=0.1)
        await q.drain()
        self.assertEqual(h.status, Status.FAILED)

    async def test_cancellation(self):
        async def slow():
            await asyncio.sleep(100)
        q = TaskQueue()
        h = q.submit(slow, retries=0, timeout_sec=60)
        await asyncio.sleep(0.05)
        self.assertTrue(h.cancel())
        await q.drain()
        self.assertEqual(h.status, Status.CANCELLED)

    async def test_concurrency_limit(self):
        running = 0
        max_running = 0
        async def track():
            nonlocal running, max_running
            running += 1
            max_running = max(max_running, running)
            await asyncio.sleep(0.05)
            running -= 1
        q = TaskQueue(max_concurrency=2)
        for _ in range(6):
            q.submit(track, retries=0, timeout_sec=10)
        await q.drain()
        self.assertLessEqual(max_running, 2)

    async def test_stats(self):
        async def ok(): return 1
        async def fail(): raise ValueError("x")
        q = TaskQueue()
        q.submit(ok, retries=0)
        q.submit(ok, retries=0)
        q.submit(fail, retries=0)
        await q.drain()
        s = q.stats
        self.assertEqual(s["done"], 2)
        self.assertEqual(s["failed"], 1)

    async def test_drain_empty(self):
        q = TaskQueue()
        await q.drain()  # should not raise

    async def test_cancel_completed_task(self):
        async def ok(): return 1
        q = TaskQueue()
        h = q.submit(ok, retries=0)
        await q.drain()
        self.assertFalse(h.cancel())


if __name__ == "__main__":
    unittest.main()
