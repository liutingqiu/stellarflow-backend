"""tests/test_zk_proof_queue.py

Issue #1262 — ZK Proof Generation Queue Rate Limiter & Priority Manager.

Covers:
- Queue constants + routing (remittance redemption -> high priority).
- Per-user sliding-window rate limiting (isolation, reset, retry_after).
- Celery topology: zk_high_priority / zk_standard queues + routes.
- Dispatcher: rate-limit enforcement + correct queue selection.
- Task execution core (_execute_zk_proof_generation) without a broker.
"""

from __future__ import annotations

import os
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.services.zk_proof_queue import (
    HIGH_PRIORITY_JOB_TYPES,
    HIGH_PRIORITY_TASK_NAME,
    STANDARD_PRIORITY_TASK_NAME,
    ZK_HIGH_PRIORITY_QUEUE,
    ZK_STANDARD_QUEUE,
    RateLimitExceeded,
    ZkProofRateLimiter,
    describe_queues,
    is_high_priority_job,
    resolve_task_name,
    resolve_zk_queue,
)


def _valid_hex(n: int = 64) -> str:
    return "a" * n


# ---------------------------------------------------------------------------
# Routing
# ---------------------------------------------------------------------------


class TestQueueConstants:
    def test_queue_names(self):
        assert ZK_HIGH_PRIORITY_QUEUE == "zk_high_priority"
        assert ZK_STANDARD_QUEUE == "zk_standard"

    def test_task_names(self):
        assert HIGH_PRIORITY_TASK_NAME == "app.tasks.generate_zk_proof_high_priority"
        assert STANDARD_PRIORITY_TASK_NAME == "app.tasks.generate_zk_proof_standard"


class TestRouting:
    def test_remittance_redemption_goes_high(self):
        assert resolve_zk_queue("remittance_redemption") == ZK_HIGH_PRIORITY_QUEUE

    def test_redemption_aliases_go_high(self):
        for job in ["redemption", "remittance_redeem", "URGENT_REDEMPTION"]:
            assert resolve_zk_queue(job) == ZK_HIGH_PRIORITY_QUEUE, job

    def test_time_sensitive_overrides_explicit_standard(self):
        # Even when caller asks for standard, redemption stays high.
        assert (
            resolve_zk_queue("remittance_redemption", priority="standard")
            == ZK_HIGH_PRIORITY_QUEUE
        )

    def test_standard_job_goes_standard(self):
        assert resolve_zk_queue("standard") == ZK_STANDARD_QUEUE
        assert resolve_zk_queue("batch_verify") == ZK_STANDARD_QUEUE
        assert resolve_zk_queue("") == ZK_STANDARD_QUEUE

    def test_explicit_high_priority(self):
        assert resolve_zk_queue("anything", priority="high") == ZK_HIGH_PRIORITY_QUEUE
        assert resolve_zk_queue("batch_job", priority="high") == ZK_HIGH_PRIORITY_QUEUE
        assert resolve_zk_queue("batch_job", priority="urgent") == ZK_HIGH_PRIORITY_QUEUE

    def test_is_high_priority_job(self):
        assert is_high_priority_job("remittance_redemption") is True
        assert is_high_priority_job("REMITTANCE_REDEMPTION") is True
        assert is_high_priority_job("standard") is False

    def test_resolve_task_name(self):
        assert resolve_task_name(ZK_HIGH_PRIORITY_QUEUE) == HIGH_PRIORITY_TASK_NAME
        assert resolve_task_name(ZK_STANDARD_QUEUE) == STANDARD_PRIORITY_TASK_NAME

    def test_high_priority_job_types_contains_redemption(self):
        assert "remittance_redemption" in HIGH_PRIORITY_JOB_TYPES


# ---------------------------------------------------------------------------
# Rate limiter
# ---------------------------------------------------------------------------


class TestRateLimiter:
    def test_allows_up_to_limit(self):
        limiter = ZkProofRateLimiter(max_requests=3, window_seconds=60)
        assert limiter.is_allowed("alice") is True
        assert limiter.is_allowed("alice") is True
        assert limiter.is_allowed("alice") is True
        assert limiter.is_allowed("alice") is False

    def test_per_user_isolation(self):
        limiter = ZkProofRateLimiter(max_requests=1, window_seconds=60)
        assert limiter.is_allowed("alice") is True
        assert limiter.is_allowed("alice") is False
        # Different user unaffected
        assert limiter.is_allowed("bob") is True

    def test_check_does_not_consume(self):
        limiter = ZkProofRateLimiter(max_requests=2, window_seconds=60)
        s1 = limiter.check("carol")
        assert s1.allowed is True and s1.remaining == 2
        limiter.is_allowed("carol")
        s2 = limiter.check("carol")
        assert s2.remaining == 1

    def test_retry_after_on_exhaustion(self):
        limiter = ZkProofRateLimiter(max_requests=1, window_seconds=60)
        limiter.is_allowed("dave")
        status = limiter.check_and_consume("dave")
        assert status.allowed is False
        assert status.retry_after > 0

    def test_reset(self):
        limiter = ZkProofRateLimiter(max_requests=1, window_seconds=60)
        limiter.is_allowed("erin")
        assert limiter.is_allowed("erin") is False
        limiter.reset("erin")
        assert limiter.is_allowed("erin") is True

    def test_rejects_empty_user(self):
        limiter = ZkProofRateLimiter(max_requests=5, window_seconds=60)
        with pytest.raises(ValueError):
            limiter.is_allowed("")
        with pytest.raises(ValueError):
            limiter.check("   ")

    def test_describe_queues(self):
        info = describe_queues()
        names = [q["name"] for q in info["queues"]]
        assert ZK_HIGH_PRIORITY_QUEUE in names
        assert ZK_STANDARD_QUEUE in names
        assert info["rate_limit"]["scope"] == "per-user"
        assert "remittance_redemption" in info["time_sensitive_job_types"]


# ---------------------------------------------------------------------------
# Celery topology (requires celery installed; skipped otherwise)
# ---------------------------------------------------------------------------


def _require_celery():
    return pytest.importorskip("app.celery_app", reason="celery not installed")


class TestCeleryTopology:
    def test_both_queues_declared(self):
        _require_celery()
        from app.celery_app import celery_app as app

        queue_names = {q.name for q in app.conf.task_queues}
        assert ZK_HIGH_PRIORITY_QUEUE in queue_names
        assert ZK_STANDARD_QUEUE in queue_names

    def test_routes_point_to_queues(self):
        _require_celery()
        from app.celery_app import celery_app as app

        routes = app.conf.task_routes
        assert routes[HIGH_PRIORITY_TASK_NAME]["queue"] == ZK_HIGH_PRIORITY_QUEUE
        assert routes[STANDARD_PRIORITY_TASK_NAME]["queue"] == ZK_STANDARD_QUEUE
        assert routes[HIGH_PRIORITY_TASK_NAME]["routing_key"] == "zk.high"
        assert routes[STANDARD_PRIORITY_TASK_NAME]["routing_key"] == "zk.standard"

    def test_queues_are_durable_with_priority(self):
        _require_celery()
        from app.celery_app import celery_app as app

        by_name = {q.name: q for q in app.conf.task_queues}
        for name in (ZK_HIGH_PRIORITY_QUEUE, ZK_STANDARD_QUEUE):
            q = by_name[name]
            assert q.durable is True
            assert q.queue_arguments.get("x-max-priority") == 10

    def test_tasks_registered(self):
        _require_celery()
        from app.celery_app import celery_app as app

        assert HIGH_PRIORITY_TASK_NAME in app.tasks
        assert STANDARD_PRIORITY_TASK_NAME in app.tasks

    def test_fairness_prefetch(self):
        # prefetch=1 prevents a single worker hoarding; dedicated -Q pools
        # in docker-compose guarantee standard jobs never starve.
        _require_celery()
        from app.celery_app import celery_app as app

        assert app.conf.worker_prefetch_multiplier == 1


# ---------------------------------------------------------------------------
# Dispatcher + task core
# ---------------------------------------------------------------------------


class TestDispatcher:
    def test_high_priority_dispatch_uses_high_queue(self):
        _require_celery()
        limiter = ZkProofRateLimiter(max_requests=10, window_seconds=60)
        fake_result = MagicMock()
        fake_result.id = "task-123"
        with patch("app.celery_app.celery_app.send_task", return_value=fake_result) as st:
            from app.services.zk_proof_queue import request_proof_generation

            out = request_proof_generation(
                user_id="alice",
                job_type="remittance_redemption",
                proof_hex=_valid_hex(),
                public_inputs=["0x1"],
                limiter=limiter,
            )
            assert out.queue == ZK_HIGH_PRIORITY_QUEUE
            assert out.task_name == HIGH_PRIORITY_TASK_NAME
            assert out.priority == "high"
            kwargs = st.call_args
            assert kwargs.kwargs["queue"] == ZK_HIGH_PRIORITY_QUEUE

    def test_standard_dispatch_uses_standard_queue(self):
        _require_celery()
        limiter = ZkProofRateLimiter(max_requests=10, window_seconds=60)
        fake_result = MagicMock()
        fake_result.id = "task-456"
        with patch("app.celery_app.celery_app.send_task", return_value=fake_result):
            from app.services.zk_proof_queue import request_proof_generation

            out = request_proof_generation(
                user_id="bob",
                job_type="batch_verify",
                proof_hex=_valid_hex(),
                public_inputs=["0x1"],
                limiter=limiter,
            )
            assert out.queue == ZK_STANDARD_QUEUE
            assert out.priority == "standard"

    def test_rate_limit_blocks_dispatch(self):
        _require_celery()
        limiter = ZkProofRateLimiter(max_requests=1, window_seconds=60)
        fake_result = MagicMock()
        fake_result.id = "t1"
        with patch("app.celery_app.celery_app.send_task", return_value=fake_result):
            from app.services.zk_proof_queue import request_proof_generation

            request_proof_generation(
                user_id="carol",
                job_type="standard",
                proof_hex=_valid_hex(),
                public_inputs=["0x1"],
                limiter=limiter,
            )
            with pytest.raises(RateLimitExceeded) as exc_info:
                request_proof_generation(
                    user_id="carol",
                    job_type="standard",
                    proof_hex=_valid_hex(),
                    public_inputs=["0x1"],
                    limiter=limiter,
                )
            assert exc_info.value.retry_after >= 0

    def test_rejects_empty_user(self):
        from app.services.zk_proof_queue import request_proof_generation

        with pytest.raises(ValueError):
            request_proof_generation(user_id="  ", job_type="standard")


class TestTaskCore:
    def test_execute_success(self):
        pytest.importorskip("app.tasks", reason="celery not installed")
        from app.tasks import _execute_zk_proof_generation

        out = _execute_zk_proof_generation(
            user_id="alice",
            job_type="remittance_redemption",
            proof_hex=_valid_hex(),
            public_inputs=["0x1", "0x2"],
            proof_scheme="groth16",
            queue=ZK_HIGH_PRIORITY_QUEUE,
        )
        assert out["success"] is True
        assert out["queue"] == ZK_HIGH_PRIORITY_QUEUE
        assert out["job_type"] == "remittance_redemption"
        assert len(out["proof_hash"]) == 64

    def test_execute_rejects_bad_structure(self):
        pytest.importorskip("app.tasks", reason="celery not installed")
        from app.tasks import _execute_zk_proof_generation

        out = _execute_zk_proof_generation(
            user_id="alice",
            job_type="standard",
            proof_hex="",
            public_inputs=["0x1"],
            queue=ZK_STANDARD_QUEUE,
        )
        assert out["success"] is False
        assert "Structure" in out["error"]

    def test_execute_rejects_empty_user(self):
        pytest.importorskip("app.tasks", reason="celery not installed")
        from app.tasks import _execute_zk_proof_generation

        with pytest.raises(ValueError):
            _execute_zk_proof_generation(
                user_id="",
                job_type="standard",
                proof_hex=_valid_hex(),
                public_inputs=["0x1"],
            )
