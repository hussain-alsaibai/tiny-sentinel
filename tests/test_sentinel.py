"""Tests for tiny_sentinel — circuit breaker, rate limiter, timeout, Sentinel."""
from __future__ import annotations

import asyncio
import time

import pytest

from tiny_sentinel import (
    CircuitBreaker,
    CircuitBreakerError,
    CircuitState,
    RateLimiter,
    Result,
    Sentinel,
    timeout,
)


# --------------------------------------------------------------------------- #
# CircuitBreaker tests
# --------------------------------------------------------------------------- #
class TestCircuitBreaker:
    def test_starts_closed(self):
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=1)
        assert cb.state == CircuitState.CLOSED

    def test_successful_calls_keep_closed(self):
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=1)

        @cb
        def ok():
            return "ok"

        for _ in range(10):
            assert ok() == "ok"
        assert cb.state == CircuitState.CLOSED

    def test_opens_after_threshold_failures(self):
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=1)

        @cb
        def boom():
            raise ValueError("nope")

        for _ in range(3):
            with pytest.raises(ValueError):
                boom()
        assert cb.state == CircuitState.OPEN

    def test_open_circuit_rejects_calls(self):
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=10)

        @cb
        def boom():
            raise ValueError("nope")

        for _ in range(2):
            with pytest.raises(ValueError):
                boom()
        with pytest.raises(CircuitBreakerError):
            boom()

    def test_transitions_to_half_open_after_timeout(self):
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=0.1)

        @cb
        def boom():
            raise ValueError("nope")

        with pytest.raises(ValueError):
            boom()
        assert cb.state == CircuitState.OPEN
        time.sleep(0.15)
        # first call after timeout should be allowed → enters HALF_OPEN
        assert cb._allow_request() is True
        assert cb.state == CircuitState.HALF_OPEN

    def test_half_open_success_closes(self):
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=0.1)

        @cb
        def boom():
            raise ValueError("nope")

        with pytest.raises(ValueError):
            boom()
        time.sleep(0.15)

        @cb
        def ok():
            return "ok"

        assert ok() == "ok"
        assert cb.state == CircuitState.CLOSED

    def test_half_open_failure_reopens(self):
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=0.1)

        @cb
        def boom():
            raise ValueError("nope")

        with pytest.raises(ValueError):
            boom()
        assert cb.state == CircuitState.OPEN
        time.sleep(0.15)
        # after cooldown, the next call is admitted as a half-open probe and
        # its failure re-opens the circuit
        with pytest.raises(ValueError):
            boom()
        assert cb.state == CircuitState.OPEN
        # and it's open again, so subsequent calls are rejected
        with pytest.raises(CircuitBreakerError):
            boom()

    def test_reset(self):
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=10)

        @cb
        def boom():
            raise ValueError("nope")

        with pytest.raises(ValueError):
            boom()
        assert cb.state == CircuitState.OPEN
        cb.reset()
        assert cb.state == CircuitState.CLOSED

    def test_get_stats_keys(self):
        cb = CircuitBreaker(failure_threshold=5, recovery_timeout=30)
        stats = cb.get_stats()
        for key in ("state", "failure_count", "last_failure_time", "success_count"):
            assert key in stats
        assert stats["state"] == "closed"
        assert stats["failure_count"] == 0

    def test_call_method(self):
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=10)

        def boom():
            raise RuntimeError("bad")

        with pytest.raises(RuntimeError):
            cb.call(boom)
        assert cb.state == CircuitState.OPEN

    def test_expected_exception_filter(self):
        cb = CircuitBreaker(
            failure_threshold=1, recovery_timeout=10, expected_exception=ValueError
        )

        @cb
        def boom():
            raise KeyError("ignored")

        # KeyError isn't expected, so circuit shouldn't open
        with pytest.raises(KeyError):
            boom()
        assert cb.state == CircuitState.CLOSED


# --------------------------------------------------------------------------- #
# RateLimiter tests
# --------------------------------------------------------------------------- #
class TestRateLimiter:
    def test_initial_burst(self):
        rl = RateLimiter(rate=1, bucket=5)
        for _ in range(5):
            assert rl.try_acquire() is True
        assert rl.try_acquire() is False

    def test_refills_over_time(self):
        rl = RateLimiter(rate=100, bucket=2)
        assert rl.try_acquire() is True
        assert rl.try_acquire() is True
        assert rl.try_acquire() is False
        time.sleep(0.05)  # 100/s * 0.05s = 5 tokens, capped at bucket=2
        assert rl.try_acquire() is True

    def test_acquire_blocks_until_available(self):
        rl = RateLimiter(rate=50, bucket=1)
        assert rl.try_acquire() is True
        assert rl.try_acquire() is False
        start = time.monotonic()
        assert rl.acquire(timeout=1) is True
        elapsed = time.monotonic() - start
        assert elapsed >= 0.01  # had to wait some time

    def test_acquire_timeout(self):
        rl = RateLimiter(rate=0.1, bucket=1)
        assert rl.try_acquire() is True
        start = time.monotonic()
        assert rl.acquire(timeout=0.1) is False
        elapsed = time.monotonic() - start
        assert elapsed < 0.5

    def test_decorator(self):
        rl = RateLimiter(rate=100, bucket=2)
        calls = []

        @rl
        def f():
            calls.append(1)
            return "x"

        assert f() == "x"
        assert f() == "x"
        # 3rd call would block; use try_acquire to confirm bucket empty
        assert rl.try_acquire() is False

    def test_get_stats(self):
        rl = RateLimiter(rate=10, bucket=5)
        stats = rl.get_stats()
        assert "available_tokens" in stats
        assert "bucket_size" in stats
        assert "rate" in stats
        assert "wait_time" in stats
        assert stats["bucket_size"] == 5

    def test_invalid_args(self):
        with pytest.raises(ValueError):
            RateLimiter(rate=0, bucket=10)
        with pytest.raises(ValueError):
            RateLimiter(rate=1, bucket=0)

    def test_wait_time_estimate(self):
        rl = RateLimiter(rate=10, bucket=1)
        assert rl.wait_time() == 0.0
        rl.try_acquire()
        assert rl.wait_time() >= 0.05  # roughly 1/10s


# --------------------------------------------------------------------------- #
# Timeout tests
# --------------------------------------------------------------------------- #
class TestTimeout:
    def test_sync_raises_on_overrun(self):
        @timeout(0.05)
        def slow():
            time.sleep(1)
            return "done"

        with pytest.raises(TimeoutError):
            slow()

    def test_sync_returns_normally(self):
        @timeout(1)
        def fast():
            return "ok"

        assert fast() == "ok"

    def test_async_raises_on_overrun(self):
        @timeout(0.05)
        async def slow():
            await asyncio.sleep(1)
            return "done"

        with pytest.raises(TimeoutError):
            asyncio.run(slow())

    def test_async_returns_normally(self):
        @timeout(1)
        async def fast():
            await asyncio.sleep(0.01)
            return "ok"

        assert asyncio.run(fast()) == "ok"

    def test_propagates_exceptions(self):
        @timeout(1)
        def boom():
            raise RuntimeError("nope")

        with pytest.raises(RuntimeError):
            boom()

    def test_invalid_seconds(self):
        with pytest.raises(ValueError):
            timeout(0)


# --------------------------------------------------------------------------- #
# Sentinel tests
# --------------------------------------------------------------------------- #
class TestSentinel:
    def test_successful_execute(self):
        s = Sentinel(rate=100, bucket=10, failure_threshold=5, timeout_seconds=1)

        def ok():
            return "ok"

        r = s.execute(ok)
        assert r.ok is True
        assert r.value == "ok"

    def test_rate_limit_rejection(self):
        s = Sentinel(rate=1, bucket=1, failure_threshold=100, timeout_seconds=1)

        def ok():
            return "ok"

        r1 = s.execute(ok)
        assert r1.ok is True
        r2 = s.execute(ok)
        assert r2.ok is False
        assert r2.rate_limited is True

    def test_circuit_open_result(self):
        s = Sentinel(rate=1000, bucket=100, failure_threshold=1, timeout_seconds=1)

        def boom():
            raise ValueError("nope")

        r1 = s.execute(boom)
        assert r1.ok is False
        # next call should be circuit_open
        r2 = s.execute(boom)
        assert r2.circuit_open is True

    def test_timeout_result(self):
        s = Sentinel(rate=1000, bucket=100, failure_threshold=100, timeout_seconds=0.05)

        def slow():
            time.sleep(1)
            return "done"

        r = s.execute(slow)
        assert r.ok is False
        assert r.timed_out is True

    def test_decorator_form(self):
        s = Sentinel(rate=1000, bucket=10, failure_threshold=100, timeout_seconds=1)

        @s
        def add(a, b):
            return a + b

        r = add(2, 3)
        assert r.ok is True
        assert r.value == 5

    def test_result_ok_helper(self):
        r = Result.from_ok("hello")
        assert r.ok is True
        assert r.value == "hello"
        assert r.error is None

    def test_result_err_helper(self):
        e = RuntimeError("bad")
        r = Result.from_err(e, timed_out=True)
        assert r.ok is False
        assert r.error is e
        assert r.timed_out is True

    def test_get_stats_shape(self):
        s = Sentinel()
        stats = s.get_stats()
        assert "rate_limiter" in stats
        assert "circuit_breaker" in stats
        assert "timeout_seconds" in stats
        assert stats["rate_limiter"]["bucket_size"] == 30
        assert stats["circuit_breaker"]["state"] == "closed"


# --------------------------------------------------------------------------- #
# Integration smoke test
# --------------------------------------------------------------------------- #
def test_end_to_end():
    sent = Sentinel(
        rate=100, bucket=20, failure_threshold=3, timeout_seconds=0.5
    )

    success = 0
    for i in range(5):
        r = sent.execute(lambda: i)
        if r.ok:
            success += 1
    assert success == 5
