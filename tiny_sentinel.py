"""
tiny-sentinel — zero-dependency circuit breaker + rate limiter + timeout wrapper.

Unified resilience sentinel. Combines three production-grade patterns in one file:
- CircuitBreaker: cascading failure protection
- RateLimiter: token bucket with burst
- timeout(): deadline enforcement decorator

Perfect for AI agents hitting external APIs where you need all three patterns
without pulling in three separate libraries.

Usage:

    from tiny_sentinel import CircuitBreaker, RateLimiter, timeout, Sentinel, Result

    # Just a circuit breaker
    breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=30)

    @breaker
    def call_api():
        return requests.get("https://example.com")

    # Just a rate limiter
    limiter = RateLimiter(rate=10, bucket=30)

    @limiter
    def call_api():
        return requests.get("https://example.com")

    # Just a timeout
    @timeout(5)
    def slow():
        time.sleep(10)

    # All three unified
    sent = Sentinel(rate=10, bucket=30, failure_threshold=5, timeout=30)

    @sent
    def call_api():
        return requests.get("https://example.com")

    result = sent.execute(call_api)
    if result.ok:
        print(result.value)
    else:
        print(f"failed: {result.error}")
"""
from __future__ import annotations

import asyncio
import functools
import json
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Awaitable, Callable, Optional, TypeVar, Union

__version__ = "0.1.0"
__all__ = [
    "CircuitBreaker",
    "RateLimiter",
    "Sentinel",
    "timeout",
    "Result",
    "CircuitState",
    "sentinel",
]


# --------------------------------------------------------------------------- #
# Result type
# --------------------------------------------------------------------------- #
@dataclass
class Result:
    """Result of a Sentinel.execute call. Either ok with value, or err with error."""

    value: Any = None
    error: Optional[BaseException] = None
    timed_out: bool = False
    circuit_open: bool = False
    rate_limited: bool = False

    @property
    def ok(self) -> bool:
        return self.error is None

    @classmethod
    def from_ok(cls, value: Any) -> "Result":
        return cls(value=value)

    @classmethod
    def from_err(cls, error: BaseException, **flags) -> "Result":
        return cls(error=error, **flags)

    def __repr__(self) -> str:
        if self.ok:
            return f"Result.ok({self.value!r})"
        flags = []
        if self.timed_out:
            flags.append("timed_out")
        if self.circuit_open:
            flags.append("circuit_open")
        if self.rate_limited:
            flags.append("rate_limited")
        flag_str = f" [{', '.join(flags)}]" if flags else ""
        return f"Result.err({self.error!r}){flag_str}"


# --------------------------------------------------------------------------- #
# Circuit Breaker
# --------------------------------------------------------------------------- #
class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreakerError(Exception):
    """Raised when the circuit is open and a call is rejected."""


class CircuitBreaker:
    """
    Circuit breaker with closed/open/half-open states.

    closed     → normal operation, failures counted
    open       → calls rejected immediately until recovery_timeout elapses
    half_open  → probe mode; up to half_open_max_calls allowed; success closes,
                 any failure re-opens
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        half_open_max_calls: int = 1,
        expected_exception: type = Exception,
    ):
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        if recovery_timeout < 0:
            raise ValueError("recovery_timeout must be >= 0")
        if half_open_max_calls < 1:
            raise ValueError("half_open_max_calls must be >= 1")

        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls
        self.expected_exception = expected_exception

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._half_open_calls = 0
        self._opened_at: Optional[float] = None

    # ---- core API ----
    def call(self, func: Callable[..., Any], *args, **kwargs) -> Any:
        if not self._allow_request():
            raise CircuitBreakerError(
                f"circuit is {self._state.value}; "
                f"retry after {self.recovery_timeout}s"
            )
        try:
            result = func(*args, **kwargs)
        except self.expected_exception:
            self._record_failure()
            raise
        else:
            self._record_success()
            return result

    def __call__(self, func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return self.call(func, *args, **kwargs)

        return wrapper

    # ---- state helpers ----
    def _allow_request(self) -> bool:
        if self._state == CircuitState.CLOSED:
            return True
        if self._state == CircuitState.OPEN:
            if self._opened_at is not None and (
                time.monotonic() - self._opened_at >= self.recovery_timeout
            ):
                self._transition(CircuitState.HALF_OPEN)
                return self._allow_request()
            return False
        # HALF_OPEN — each successful _allow_request() consumes one of the
        # probe slots. When the slot cap is reached, no more calls are admitted
        # until the probe resolves (success closes, failure re-opens).
        if self._half_open_calls < self.half_open_max_calls:
            self._half_open_calls += 1
            return True
        return False

    def _record_success(self) -> None:
        self._success_count += 1
        if self._state == CircuitState.HALF_OPEN:
            self._transition(CircuitState.CLOSED)
        else:
            self._failure_count = 0

    def _record_failure(self) -> None:
        self._failure_count += 1
        self._last_failure_time = time.monotonic()
        if self._state == CircuitState.HALF_OPEN:
            self._transition(CircuitState.OPEN)
        elif self._failure_count >= self.failure_threshold:
            self._transition(CircuitState.OPEN)

    def _transition(self, new_state: CircuitState) -> None:
        if self._state == new_state:
            return
        self._state = new_state
        if new_state == CircuitState.OPEN:
            self._opened_at = time.monotonic()
            self._half_open_calls = 0
        elif new_state == CircuitState.HALF_OPEN:
            # reset the probe counter when entering half-open so the first
            # _allow_request() admission counts as the single in-flight probe.
            self._half_open_calls = 0
        elif new_state == CircuitState.CLOSED:
            self._failure_count = 0
            self._half_open_calls = 0

    def reset(self) -> None:
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._half_open_calls = 0
        self._opened_at = None

    # ---- introspection ----
    def get_stats(self) -> dict:
        return {
            "state": self._state.value,
            "failure_count": self._failure_count,
            "success_count": self._success_count,
            "last_failure_time": self._last_failure_time,
            "opened_at": self._opened_at,
            "failure_threshold": self.failure_threshold,
            "recovery_timeout": self.recovery_timeout,
        }

    @property
    def state(self) -> CircuitState:
        return self._state

    def __repr__(self) -> str:
        return (
            f"CircuitBreaker(state={self._state.value}, "
            f"failures={self._failure_count}/{self.failure_threshold})"
        )


# --------------------------------------------------------------------------- #
# Rate Limiter (token bucket)
# --------------------------------------------------------------------------- #
class RateLimiter:
    """Token bucket rate limiter with burst capacity.

    refills at `rate` tokens/second up to a maximum of `bucket` tokens.
    """

    def __init__(self, rate: float, bucket: int):
        if rate <= 0:
            raise ValueError("rate must be > 0")
        if bucket < 1:
            raise ValueError("bucket must be >= 1")

        self.rate = float(rate)
        self.bucket = int(bucket)
        self._tokens = float(bucket)
        self._last_refill = time.monotonic()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        if elapsed > 0:
            self._tokens = min(self.bucket, self._tokens + elapsed * self.rate)
            self._last_refill = now

    def try_acquire(self, n: int = 1) -> bool:
        """Non-blocking attempt to take n tokens. Returns True/False."""
        if n < 1:
            raise ValueError("n must be >= 1")
        self._refill()
        if self._tokens >= n:
            self._tokens -= n
            return True
        return False

    def acquire(self, n: int = 1, timeout: Optional[float] = None) -> bool:
        """Block until n tokens are available (or timeout)."""
        if n < 1:
            raise ValueError("n must be >= 1")
        deadline = None if timeout is None else time.monotonic() + timeout
        # small bounded loop with sleep — keep tiny and dependency-free
        while True:
            if self.try_acquire(n):
                return True
            if deadline is not None and time.monotonic() >= deadline:
                return False
            # sleep proportional to the deficit; cap so we stay responsive
            deficit = n - self._tokens
            wait = min(0.05, max(0.001, deficit / self.rate))
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                wait = min(wait, remaining)
            time.sleep(wait)

    def __call__(self, func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            self.acquire()
            return func(*args, **kwargs)

        return wrapper

    def wait_time(self, n: int = 1) -> float:
        """Estimated seconds until n tokens are available. 0 if available now."""
        self._refill()
        if self._tokens >= n:
            return 0.0
        deficit = n - self._tokens
        return deficit / self.rate

    def get_stats(self) -> dict:
        self._refill()
        return {
            "available_tokens": self._tokens,
            "bucket_size": self.bucket,
            "rate": self.rate,
            "wait_time": self.wait_time(),
        }

    def __repr__(self) -> str:
        return f"RateLimiter(rate={self.rate}/s, bucket={self.bucket})"


# --------------------------------------------------------------------------- #
# Timeout decorator (sync + async)
# --------------------------------------------------------------------------- #
class TimeoutError_(TimeoutError):  # noqa: A001  (intentional alias namespace)
    pass


def timeout(seconds: float):
    """Raise TimeoutError if the wrapped callable exceeds `seconds`.

    Works for both sync and async callables. On async, uses asyncio.wait_for;
    on sync, runs in a worker thread and joins with a deadline.
    """
    if seconds <= 0:
        raise ValueError("seconds must be > 0")

    def decorator(func: Callable) -> Callable:
        if asyncio.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                try:
                    return await asyncio.wait_for(func(*args, **kwargs), timeout=seconds)
                except asyncio.TimeoutError as exc:
                    raise TimeoutError_(
                        f"{func.__name__} timed out after {seconds}s"
                    ) from exc

            return async_wrapper

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            import threading

            result_box: dict = {}

            def runner():
                try:
                    result_box["value"] = func(*args, **kwargs)
                except BaseException as exc:  # noqa: BLE001
                    result_box["error"] = exc

            thread = threading.Thread(target=runner, daemon=True)
            thread.start()
            thread.join(timeout=seconds)
            if thread.is_alive():
                # thread keeps running but we abandon it; daemon=True cleans up
                raise TimeoutError_(f"{func.__name__} timed out after {seconds}s")
            if "error" in result_box:
                raise result_box["error"]
            return result_box.get("value")

        return sync_wrapper

    return decorator


# --------------------------------------------------------------------------- #
# Sentinel — unified wrapper
# --------------------------------------------------------------------------- #
class Sentinel:
    """
    Combined resilience wrapper: rate limit → circuit check → timeout → execute.

    sentinel = Sentinel(rate=10, bucket=30, failure_threshold=5, timeout=30)

    @sentinel
    def call_api(): ...

    result = sentinel.execute(call_api)
    """

    def __init__(
        self,
        rate: float = 10.0,
        bucket: int = 30,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        half_open_max_calls: int = 1,
        timeout_seconds: float = 30.0,
        expected_exception: type = Exception,
    ):
        self.limiter = RateLimiter(rate=rate, bucket=bucket)
        self.breaker = CircuitBreaker(
            failure_threshold=failure_threshold,
            recovery_timeout=recovery_timeout,
            half_open_max_calls=half_open_max_calls,
            expected_exception=expected_exception,
        )
        self.timeout_seconds = timeout_seconds

    def execute(self, func: Callable[..., Any], *args, **kwargs) -> Result:
        # 1) rate limit (non-blocking so the caller isn't stalled on every attempt)
        if not self.limiter.try_acquire():
            return Result.from_err(
                RateLimitExceededError(
                    f"rate limited; next token in ~{self.limiter.wait_time():.3f}s"
                ),
                rate_limited=True,
            )

        # 2) circuit check
        if not self.breaker._allow_request():
            return Result.from_err(
                CircuitBreakerError(
                    f"circuit is {self.breaker.state.value}"
                ),
                circuit_open=True,
            )

        # 3) execute with timeout
        try:
            value = _run_with_timeout(
                func, self.timeout_seconds, args, kwargs
            )
        except TimeoutError_ as exc:
            self.breaker._record_failure()
            return Result.from_err(exc, timed_out=True)
        except CircuitBreakerError as exc:
            return Result.from_err(exc, circuit_open=True)
        except BaseException as exc:  # noqa: BLE001
            self.breaker._record_failure()
            return Result.from_err(exc)

        # 4) record success
        self.breaker._record_success()
        return Result.from_ok(value)

    def __call__(self, func: Callable) -> Callable:
        sent = self

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return sent.execute(func, *args, **kwargs)

        wrapper.execute = lambda *a, **kw: sent.execute(func, *a, **kw)
        return wrapper

    def get_stats(self) -> dict:
        return {
            "rate_limiter": self.limiter.get_stats(),
            "circuit_breaker": self.breaker.get_stats(),
            "timeout_seconds": self.timeout_seconds,
        }

    def __repr__(self) -> str:
        return (
            f"Sentinel(rate={self.limiter.rate}, bucket={self.limiter.bucket}, "
            f"failures={self.breaker.failure_threshold}, "
            f"timeout={self.timeout_seconds}s)"
        )


class RateLimitExceededError(Exception):
    """Raised by Sentinel.execute when the rate limiter rejects the call."""


def _run_with_timeout(
    func: Callable, seconds: float, args: tuple, kwargs: dict
) -> Any:
    """Run sync or async func with a deadline; raise TimeoutError_ on expiry."""
    if asyncio.iscoroutinefunction(func):
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # can't wait_for inside a running loop — fall back to thread
                return _run_in_thread(func, seconds, args, kwargs)
            return loop.run_until_complete(
                asyncio.wait_for(func(*args, **kwargs), timeout=seconds)
            )
        except asyncio.TimeoutError as exc:
            raise TimeoutError_(
                f"{func.__name__} timed out after {seconds}s"
            ) from exc
    return _run_in_thread(func, seconds, args, kwargs)


def _run_in_thread(func: Callable, seconds: float, args: tuple, kwargs: dict) -> Any:
    import threading

    box: dict = {}

    def runner():
        try:
            box["value"] = func(*args, **kwargs)
        except BaseException as exc:  # noqa: BLE001
            box["error"] = exc

    thread = threading.Thread(target=runner, daemon=True)
    thread.start()
    thread.join(timeout=seconds)
    if thread.is_alive():
        raise TimeoutError_(f"{func.__name__} timed out after {seconds}s")
    if "error" in box:
        raise box["error"]
    return box.get("value")


# --------------------------------------------------------------------------- #
# Functional alias
# --------------------------------------------------------------------------- #
def sentinel(
    rate: float = 10.0,
    bucket: int = 30,
    failure_threshold: int = 5,
    recovery_timeout: float = 30.0,
    half_open_max_calls: int = 1,
    timeout_seconds: float = 30.0,
):
    """Functional decorator form of Sentinel.

    @sentinel(rate=10, bucket=30, failure_threshold=5, timeout_seconds=10)
    def call(): ...
    """
    return Sentinel(
        rate=rate,
        bucket=bucket,
        failure_threshold=failure_threshold,
        recovery_timeout=recovery_timeout,
        half_open_max_calls=half_open_max_calls,
        timeout_seconds=timeout_seconds,
    )
