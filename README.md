# tiny-sentinel 🛡️

> Zero-dependency circuit breaker + rate limiter + timeout wrapper.
> The "three musketeers" of production resilience in one tiny Python file.

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Zero deps](https://img.shields.io/badge/dependencies-zero-brightgreen.svg)](#)
[![Version 0.1.0](https://img.shields.io/badge/version-0.1.0-orange.svg)](#)

Part of the [**tiny-\*** ecosystem](https://github.com/hussain-alsaibai) — single-file Python primitives that do one thing well, with zero dependencies.

---

## Why?

Every external API you call needs three guardrails:

1. A **circuit breaker** so a failing upstream doesn't take you down.
2. A **rate limiter** so you don't trip the upstream's rate limits.
3. A **timeout** so one slow call doesn't stall your agent.

Three concerns, three libraries — usually. **tiny-sentinel** rolls them into
one file (~300 lines, stdlib only) so you can drop it into any project without
touching your dependency tree. Perfect for AI agents that hit many external APIs.

## Features

| Class             | What it does                                            | Algorithm               |
| ----------------- | ------------------------------------------------------- | ----------------------- |
| `CircuitBreaker`  | Stops calling a failing dependency until it recovers   | closed/open/half-open   |
| `RateLimiter`     | Caps request rate, with burst capacity                  | token bucket            |
| `timeout`         | Enforces per-call deadlines (sync + async)              | thread / asyncio.wait_for |
| `Sentinel`        | Combines all three into one unified wrapper             | rate → circuit → timeout |

All return `Result` objects so the **Sentinel** can never crash your caller.

## Install

```bash
pip install tiny-sentinel
```

Or just copy `tiny_sentinel.py` into your project — it has no dependencies.

## Quick start

### 1. Circuit breaker

```python
from tiny_sentinel import CircuitBreaker

breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=30)

@breaker
def call_flaky_api():
    return requests.get("https://flaky.example.com")

# After 5 failures, calls are rejected immediately until the cooldown expires.
```

### 2. Rate limiter

```python
from tiny_sentinel import RateLimiter

limiter = RateLimiter(rate=10, bucket=30)  # 10 req/s, burst of 30

@limiter
def call_api():
    return requests.get("https://example.com")

# Non-blocking probe:
if limiter.try_acquire():
    call_api()
else:
    print(f"wait ~{limiter.wait_time():.2f}s")
```

### 3. Timeout

```python
from tiny_sentinel import timeout

@timeout(5)
def slow_sync():
    time.sleep(10)

@timeout(5)
async def slow_async():
    await asyncio.sleep(10)
```

### 4. The unified Sentinel

```python
from tiny_sentinel import Sentinel

sentinel = Sentinel(
    rate=10,              # tokens per second
    bucket=30,            # burst capacity
    failure_threshold=5,  # open after N failures
    recovery_timeout=30,   # seconds before half-open probe
    timeout_seconds=30,   # per-call deadline
)

@sentinel
def call_api():
    return requests.get("https://example.com")

result = call_api()
if result.ok:
    print(result.value)
else:
    print(f"failed: {result.error}")
    # result.timed_out, result.circuit_open, result.rate_limited
```

`Sentinel.execute(func, *args, **kwargs)` returns a `Result` that always
describes *why* a call failed (timeout vs. circuit open vs. rate limited vs.
exception), so you can decide whether to retry, fall back, or give up.

### Functional decorator form

```python
from tiny_sentinel import sentinel

@sentinel(rate=10, bucket=30, failure_threshold=5, timeout_seconds=10)
def call_api():
    return requests.get("https://example.com")
```

## API summary

### `CircuitBreaker(failure_threshold=5, recovery_timeout=30, half_open_max_calls=1, expected_exception=Exception)`

- `@breaker` / `breaker.call(func, *args, **kwargs)`
- `breaker.state` → `CircuitState.CLOSED | OPEN | HALF_OPEN`
- `breaker.get_stats()` → `{state, failure_count, success_count, last_failure_time, …}`
- `breaker.reset()` → manual recovery

### `RateLimiter(rate, bucket)`

- `try_acquire(n=1)` — non-blocking
- `acquire(n=1, timeout=None)` — blocking
- `wait_time(n=1)` — estimated seconds until `n` tokens
- `get_stats()` → `{available_tokens, bucket_size, rate, wait_time}`

### `timeout(seconds)`

- `@timeout(5)` for sync or async callables
- Raises `TimeoutError` on overrun

### `Sentinel(rate=10, bucket=30, failure_threshold=5, recovery_timeout=30, timeout_seconds=30)`

- `sentinel.execute(func, *args, **kwargs) → Result`
- `@sentinel` decorator form
- `sentinel.get_stats()` → combined snapshot

## License

MIT © 2026 H. A. Alsaibai — see [LICENSE](LICENSE).
