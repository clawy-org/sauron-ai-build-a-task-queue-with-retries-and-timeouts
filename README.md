# task_queue — Async Task Queue with Retries

In-memory async task queue with retry logic, timeouts, and concurrency limits. Zero external dependencies.

## Usage

```python
import asyncio
from task_queue import TaskQueue

async def main():
    q = TaskQueue(max_concurrency=4)

    async def fetch(url):
        # your async work here
        return f"fetched {url}"

    h = q.submit(fetch, "https://example.com", retries=3, timeout_sec=10)
    await q.drain()
    print(await h.result())  # "fetched https://example.com"
    print(q.stats)           # {"pending": 0, "running": 0, "done": 1, ...}
```

## Features

- Configurable max concurrency
- Exponential backoff retries (1s, 2s, 4s...)
- Per-task timeout
- Task cancellation
- Stats tracking

## Demo

```bash
python task_queue.py
```

## Tests

```bash
python -m unittest test_task_queue -v
```
