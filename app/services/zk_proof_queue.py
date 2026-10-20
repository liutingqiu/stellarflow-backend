"""app/services/zk_proof_queue.py

Issue #1262 — ZK Proof Generation Queue Rate Limiter & Priority Manager.

Provides:
1. Prioritized queue constants (``zk_high_priority``, ``zk_standard``).
2. Routing helper that sends time-sensitive remittance redemption proofs
   to the high-priority queue.
3. Per-user sliding-window rate limiter (Redis-backed with in-memory
   fallback) to prevent system overload during high traffic periods.

Design notes
------------
- No third-party imports at module load time (only stdlib). Redis/Celery
  are imported lazily so unit tests run without a broker.
- Fairness: high-priority jobs run on a dedicated worker pool
  (see ``docker-compose.yml`` ``zk-high-worker`` consuming only
  ``zk_high_priority``), so they execute with zero queue delay under
  normal load. Standard jobs run on their own pool (``zk-standard-worker``
  consuming only ``zk_standard``), so they can never starve.
"""

from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

# ---------------------------------------------------------------------------
# Queue constants (must match app/celery_app.py task_queues / task_routes)
# ---------------------------------------------------------------------------

ZK_HIGH_PRIORITY_QUEUE: str = "zk_high_priority"
ZK_STANDARD_QUEUE: str = "zk_standard"
ZK_EXCHANGE: str = "zk"
ZK_HIGH_ROUTING_KEY: str = "zk.high"
ZK_STANDARD_ROUTING_KEY: str = "zk.standard"

HIGH_PRIORITY_TASK_NAME: str = "app.tasks.generate_zk_proof_high_priority"
STANDARD_PRIORITY_TASK_NAME: str = "app.tasks.generate_zk_proof_standard"

# Job types that are time-sensitive and must always use the high-priority
# queue, even when the caller requests standard priority.
HIGH_PRIORITY_JOB_TYPES: frozenset[str] = frozenset(
    {
        "remittance_redemption",
        "redemption",
        "remittance_redeem",
        "urgent_redemption",
        "redemption_urgent",
    }
)

# ---------------------------------------------------------------------------
# Rate-limit configuration (env-overridable, no new dependencies)
# ---------------------------------------------------------------------------


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def get_rate_limit_per_minute() -> int:
    """Max proof-generation requests per user per window."""
    return _int_env("ZK_PROOF_RATE_LIMIT_PER_MINUTE", 20)


def get_rate_limit_window_seconds() -> int:
    """Sliding-window length in seconds."""
    return _int_env("ZK_PROOF_RATE_WINDOW_SECONDS", 60)


def _redis_key_prefix() -> str:
    return os.getenv("ZK_PROOF_RATE_KEY_PREFIX", "zk_proof_rl")


# ---------------------------------------------------------------------------
# Routing
# ---------------------------------------------------------------------------


def _normalize_job_type(job_type: Any) -> str:
    if not isinstance(job_type, str):
        return ""
    return job_type.strip().lower()


def is_high_priority_job(job_type: str) -> bool:
    """Return True when *job_type* is time-sensitive (remittance redemption)."""
    return _normalize_job_type(job_type) in HIGH_PRIORITY_JOB_TYPES


def resolve_zk_queue(job_type: str = "", priority: str = "standard") -> str:
    """Resolve the Celery queue for a proof-generation job.

    Rules:
    - Time-sensitive remittance redemption proofs ALWAYS go to
      ``zk_high_priority`` (overrides an explicit ``standard`` request).
    - An explicit ``high``/``urgent`` priority request goes to
      ``zk_high_priority``.
    - Everything else goes to ``zk_standard``.
    """
    normalized_job = _normalize_job_type(job_type)
    normalized_priority = (
        priority.strip().lower() if isinstance(priority, str) else "standard"
    )
    if normalized_job in HIGH_PRIORITY_JOB_TYPES:
        return ZK_HIGH_PRIORITY_QUEUE
    if normalized_priority in {"high", "urgent", "critical"}:
        return ZK_HIGH_PRIORITY_QUEUE
    return ZK_STANDARD_QUEUE


def resolve_task_name(queue: str) -> str:
    """Map a queue name to its canonical Celery task name."""
    if queue == ZK_HIGH_PRIORITY_QUEUE:
        return HIGH_PRIORITY_TASK_NAME
    return STANDARD_PRIORITY_TASK_NAME


# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------


class RateLimitExceeded(Exception):
    """Raised when a user exceeds the per-user proof-generation rate limit."""

    def __init__(
        self,
        user_id: str,
        limit: int,
        retry_after: float,
        queue: Optional[str] = None,
    ) -> None:
        self.user_id = user_id
        self.limit = limit
        self.retry_after = retry_after
        self.queue = queue
        super().__init__(
            f"rate limit exceeded for user {user_id!r}: "
            f"{limit} requests per {get_rate_limit_window_seconds()}s "
            f"(retry after {retry_after:.1f}s)"
        )


@dataclass(frozen=True)
class RateLimitStatus:
    allowed: bool
    remaining: int
    limit: int
    retry_after: float


class ZkProofRateLimiter:
    """Per-user sliding-window rate limiter with Redis + in-memory fallback.

    - When ``REDIS_URL`` is configured and the ``redis`` package is
      importable, counts live in a Redis sorted set
      (``<prefix>:<user_id>``) so limits are enforced across workers.
    - Otherwise (tests, local dev, broker down) an in-process sliding
      window is used. Fail-open is never applied: the in-memory path
      still enforces the limit for the local process.
    """

    def __init__(
        self,
        max_requests: Optional[int] = None,
        window_seconds: Optional[int] = None,
        redis_client: Any = None,
    ) -> None:
        self._max_requests = (
            max_requests if max_requests is not None else get_rate_limit_per_minute()
        )
        self._window = float(
            window_seconds
            if window_seconds is not None
            else get_rate_limit_window_seconds()
        )
        self._explicit_redis = redis_client
        self._lock = threading.Lock()
        self._memory: Dict[str, list[float]] = {}

    # -- redis helpers (lazy, guarded) -----------------------------------

    def _get_redis(self) -> Any:
        if self._explicit_redis is not None:
            return self._explicit_redis
        redis_url = os.getenv("REDIS_URL", os.getenv("ZK_PROOF_REDIS_URL", ""))
        if not redis_url:
            return None
        try:
            import redis as redis_lib  # type: ignore

            return redis_lib.from_url(redis_url, decode_responses=True)
        except Exception:
            return None

    def _redis_key(self, user_id: str) -> str:
        return f"{_redis_key_prefix()}:{user_id}"

    def _redis_check_and_consume(self, user_id: str) -> Optional[RateLimitStatus]:
        client = self._get_redis()
        if client is None:
            return None
        try:
            key = self._redis_key(user_id)
            now_ms = int(time.time() * 1000)
            window_ms = int(self._window * 1000)
            cutoff = now_ms - window_ms
            pipe = client.pipeline()
            pipe.zremrangebyscore(key, 0, cutoff)
            pipe.zcard(key)
            pipe.zrange(key, 0, 0, withscores=True)
            _, count, earliest = pipe.execute()[:3]
            if count >= self._max_requests:
                retry_after = self._window
                if earliest:
                    try:
                        earliest_ts = earliest[0][1]
                        retry_after = max(
                            0.0, (window_ms - (now_ms - float(earliest_ts))) / 1000.0
                        )
                    except Exception:
                        pass
                return RateLimitStatus(
                    allowed=False,
                    remaining=0,
                    limit=self._max_requests,
                    retry_after=round(retry_after, 2),
                )
            # consume one token
            pipe = client.pipeline()
            pipe.zadd(key, {str(now_ms): now_ms})
            pipe.expire(key, int(self._window) + 5)
            pipe.execute()
            remaining = max(0, self._max_requests - count - 1)
            return RateLimitStatus(
                allowed=True,
                remaining=remaining,
                limit=self._max_requests,
                retry_after=0.0,
            )
        except Exception:
            return None

    def _redis_remaining(self, user_id: str) -> Optional[int]:
        client = self._get_redis()
        if client is None:
            return None
        try:
            key = self._redis_key(user_id)
            now_ms = int(time.time() * 1000)
            cutoff = now_ms - int(self._window * 1000)
            pipe = client.pipeline()
            pipe.zremrangebyscore(key, 0, cutoff)
            pipe.zcard(key)
            pipe.expire(key, int(self._window) + 5)
            _, count, _ = pipe.execute()
            return max(0, self._max_requests - int(count))
        except Exception:
            return None

    # -- in-memory path ---------------------------------------------------

    def _memory_prune(self, user_id: str, now: float) -> list[float]:
        cutoff = now - self._window
        hits = [t for t in self._memory.get(user_id, []) if t > cutoff]
        self._memory[user_id] = hits
        return hits

    # -- public API --------------------------------------------------------

    def _require_user(self, user_id: Any) -> str:
        if not isinstance(user_id, str) or not user_id.strip():
            raise ValueError("user_id must be a non-empty string")
        return user_id.strip()

    def check(self, user_id: str) -> RateLimitStatus:
        """Return current status WITHOUT consuming a token."""
        uid = self._require_user(user_id)
        redis_remaining = self._redis_remaining(uid)
        if redis_remaining is not None:
            return RateLimitStatus(
                allowed=redis_remaining > 0,
                remaining=redis_remaining,
                limit=self._max_requests,
                retry_after=0.0 if redis_remaining > 0 else self._window,
            )
        now = time.time()
        with self._lock:
            hits = self._memory_prune(uid, now)
            remaining = max(0, self._max_requests - len(hits))
            retry_after = 0.0
            if remaining == 0 and hits:
                retry_after = round(max(0.0, self._window - (now - hits[0])), 2)
            return RateLimitStatus(
                allowed=remaining > 0,
                remaining=remaining,
                limit=self._max_requests,
                retry_after=retry_after,
            )

    def is_allowed(self, user_id: str) -> bool:
        """Consume one token; return True when the request may proceed."""
        uid = self._require_user(user_id)
        redis_status = self._redis_check_and_consume(uid)
        if redis_status is not None:
            return redis_status.allowed
        now = time.time()
        with self._lock:
            hits = self._memory_prune(uid, now)
            if len(hits) >= self._max_requests:
                return False
            hits.append(now)
            self._memory[uid] = hits
            return True

    def check_and_consume(self, user_id: str) -> RateLimitStatus:
        """Consume one token and return the post-consume status."""
        uid = self._require_user(user_id)
        redis_status = self._redis_check_and_consume(uid)
        if redis_status is not None:
            return redis_status
        now = time.time()
        with self._lock:
            hits = self._memory_prune(uid, now)
            if len(hits) >= self._max_requests:
                retry_after = round(max(0.0, self._window - (now - hits[0])), 2)
                return RateLimitStatus(
                    allowed=False,
                    remaining=0,
                    limit=self._max_requests,
                    retry_after=retry_after,
                )
            hits.append(now)
            self._memory[uid] = hits
            return RateLimitStatus(
                allowed=True,
                remaining=max(0, self._max_requests - len(hits)),
                limit=self._max_requests,
                retry_after=0.0,
            )

    def reset(self, user_id: str) -> None:
        uid = self._require_user(user_id)
        with self._lock:
            self._memory.pop(uid, None)
        client = self._get_redis()
        if client is not None:
            try:
                client.delete(self._redis_key(uid))
            except Exception:
                pass

    def reset_all(self) -> None:
        with self._lock:
            self._memory.clear()


_limiter_singleton: Optional[ZkProofRateLimiter] = None
_limiter_lock = threading.Lock()


def get_rate_limiter() -> ZkProofRateLimiter:
    """Process-wide singleton rate limiter (env-configured)."""
    global _limiter_singleton
    if _limiter_singleton is None:
        with _limiter_lock:
            if _limiter_singleton is None:
                _limiter_singleton = ZkProofRateLimiter()
    return _limiter_singleton


def _reset_rate_limiter_singleton() -> None:
    """Test hook: drop the singleton so env changes take effect."""
    global _limiter_singleton
    with _limiter_lock:
        _limiter_singleton = None


# ---------------------------------------------------------------------------
# Priority manager — enqueue helper used by tasks + API router
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EnqueueResult:
    task_id: str
    queue: str
    task_name: str
    priority: str
    remaining: int
    retry_after: float


def describe_queues() -> Dict[str, Any]:
    """Return queue topology + rate-limit config for monitoring/health."""
    return {
        "queues": [
            {
                "name": ZK_HIGH_PRIORITY_QUEUE,
                "exchange": ZK_EXCHANGE,
                "routing_key": ZK_HIGH_ROUTING_KEY,
                "task": HIGH_PRIORITY_TASK_NAME,
                "dedicated_worker": "zk-high-worker",
                "sla": "zero queue delay under normal load",
            },
            {
                "name": ZK_STANDARD_QUEUE,
                "exchange": ZK_EXCHANGE,
                "routing_key": ZK_STANDARD_ROUTING_KEY,
                "task": STANDARD_PRIORITY_TASK_NAME,
                "dedicated_worker": "zk-standard-worker",
                "sla": "smooth processing without starving workers",
            },
        ],
        "rate_limit": {
            "max_requests": get_rate_limit_per_minute(),
            "window_seconds": get_rate_limit_window_seconds(),
            "scope": "per-user",
        },
        "time_sensitive_job_types": sorted(HIGH_PRIORITY_JOB_TYPES),
    }


def request_proof_generation(
    user_id: str,
    job_type: str = "standard",
    priority: str = "standard",
    proof_hex: str = "",
    public_inputs: Optional[list] = None,
    proof_scheme: str = "groth16",
    transaction_id: Optional[str] = None,
    limiter: Optional[ZkProofRateLimiter] = None,
) -> EnqueueResult:
    """Rate-limit, route, and enqueue a ZK proof-generation job.

    - Enforces the per-user sliding-window limit (raises
      :class:`RateLimitExceeded` with ``retry_after`` on overflow).
    - Routes remittance redemption proofs to ``zk_high_priority``.
    - Dispatches via Celery ``apply_async`` with an explicit ``queue``
      argument so routing holds even if broker-side routes drift.
    """
    if not isinstance(user_id, str) or not user_id.strip():
        raise ValueError("user_id must be a non-empty string")
    uid = user_id.strip()
    queue = resolve_zk_queue(job_type, priority)
    task_name = resolve_task_name(queue)

    active_limiter = limiter or get_rate_limiter()
    status = active_limiter.check_and_consume(uid)
    if not status.allowed:
        raise RateLimitExceeded(
            user_id=uid,
            limit=status.limit,
            retry_after=status.retry_after,
            queue=queue,
        )

    # Lazy Celery import: keeps this module importable without a broker.
    from app.celery_app import celery_app  # noqa: E402

    kwargs: Dict[str, Any] = {
        "user_id": uid,
        "job_type": _normalize_job_type(job_type) or "standard",
        "proof_hex": proof_hex,
        "public_inputs": list(public_inputs) if public_inputs else [],
        "proof_scheme": proof_scheme,
    }
    if transaction_id:
        kwargs["transaction_id"] = transaction_id

    routing_key = (
        ZK_HIGH_ROUTING_KEY if queue == ZK_HIGH_PRIORITY_QUEUE else ZK_STANDARD_ROUTING_KEY
    )
    # Priority 9 for high-priority jobs gives RabbitMQ x-max-priority
    # consumers an extra ordering hint inside the dedicated queue.
    message_priority = 9 if queue == ZK_HIGH_PRIORITY_QUEUE else 5
    async_result = celery_app.send_task(
        task_name,
        kwargs=kwargs,
        queue=queue,
        routing_key=routing_key,
        priority=message_priority,
    )
    return EnqueueResult(
        task_id=str(getattr(async_result, "id", "")),
        queue=queue,
        task_name=task_name,
        priority="high" if queue == ZK_HIGH_PRIORITY_QUEUE else "standard",
        remaining=status.remaining,
        retry_after=status.retry_after,
    )


__all__ = [
    "ZK_HIGH_PRIORITY_QUEUE",
    "ZK_STANDARD_QUEUE",
    "ZK_EXCHANGE",
    "ZK_HIGH_ROUTING_KEY",
    "ZK_STANDARD_ROUTING_KEY",
    "HIGH_PRIORITY_TASK_NAME",
    "STANDARD_PRIORITY_TASK_NAME",
    "HIGH_PRIORITY_JOB_TYPES",
    "RateLimitExceeded",
    "RateLimitStatus",
    "ZkProofRateLimiter",
    "EnqueueResult",
    "describe_queues",
    "get_rate_limit_per_minute",
    "get_rate_limit_window_seconds",
    "get_rate_limiter",
    "is_high_priority_job",
    "resolve_task_name",
    "resolve_zk_queue",
    "request_proof_generation",
]
