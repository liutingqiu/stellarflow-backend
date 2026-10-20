"""Tests for Issue #1288 — Payout Anchor Availability Health Monitor.

Verifies:
1. /info health pings every 30s (interval constant).
2. OFFLINE after 3 consecutive failures (90s detection window).
3. Automatic re-routing of new remittance transfers away from OFFLINE anchors.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.analytics.fiat_settlement import AnchorHealthStatus
from src.analytics.remittance_anchor_health import (
    HEALTH_CHECK_INTERVAL_SECONDS,
    MAX_CONSECUTIVE_FAILURES,
    OFFLINE_DETECTION_WINDOW_SECONDS,
    PayoutAnchorStatus,
    RemittancePayoutAnchorHealthSupervisor,
    build_info_url,
    is_healthy_response,
)


class TestHealthCheckTuning:
    def test_interval_is_30_seconds(self):
        assert HEALTH_CHECK_INTERVAL_SECONDS == pytest.approx(30.0)

    def test_offline_threshold_is_3_failures(self):
        assert MAX_CONSECUTIVE_FAILURES == 3

    def test_detection_within_90_seconds(self):
        assert OFFLINE_DETECTION_WINDOW_SECONDS == pytest.approx(90.0)
        sup = RemittancePayoutAnchorHealthSupervisor()
        assert sup.detection_window_seconds == pytest.approx(90.0)
        assert sup.detection_window_seconds <= 90.0

    def test_info_url_builder(self):
        assert build_info_url("https://anchor.example.com") == "https://anchor.example.com/info"
        assert build_info_url("https://anchor.example.com/") == "https://anchor.example.com/info"
        assert build_info_url("https://anchor.example.com/info") == "https://anchor.example.com/info"
        with pytest.raises(ValueError):
            build_info_url("")

    def test_healthy_response_classification(self):
        assert is_healthy_response(200) is True
        assert is_healthy_response(201) is True
        assert is_healthy_response(500) is False
        assert is_healthy_response(404) is False
        assert is_healthy_response(None) is False


class TestOfflineDetection:
    def test_single_failure_keeps_active(self):
        sup = RemittancePayoutAnchorHealthSupervisor()
        sup.register_anchor("anchor_fw", "https://fw.example.com")
        sup.record_failure("anchor_fw", reason="timeout")
        assert sup.get_status("anchor_fw") == PayoutAnchorStatus.ACTIVE
        assert sup.is_offline("anchor_fw") is False

    def test_two_failures_still_active(self):
        sup = RemittancePayoutAnchorHealthSupervisor()
        sup.register_anchor("anchor_fw", "https://fw.example.com")
        sup.record_failure("anchor_fw", reason="timeout 1")
        sup.record_failure("anchor_fw", reason="timeout 2")
        assert sup.get_status("anchor_fw") == PayoutAnchorStatus.ACTIVE

    def test_three_consecutive_failures_marks_offline(self):
        sup = RemittancePayoutAnchorHealthSupervisor()
        sup.register_anchor("anchor_fw", "https://fw.example.com")
        sup.record_failure("anchor_fw", reason="timeout 1")
        sup.record_failure("anchor_fw", reason="timeout 2")
        state = sup.record_failure("anchor_fw", reason="timeout 3")
        assert state.status == PayoutAnchorStatus.OFFLINE
        assert sup.is_offline("anchor_fw") is True
        assert sup.get_status("anchor_fw") == PayoutAnchorStatus.OFFLINE

    def test_success_resets_consecutive_failures(self):
        sup = RemittancePayoutAnchorHealthSupervisor()
        sup.register_anchor("anchor_fw", "https://fw.example.com")
        sup.record_failure("anchor_fw", reason="boom 1")
        sup.record_failure("anchor_fw", reason="boom 2")
        sup.record_success("anchor_fw")
        assert sup.get_availability("anchor_fw").consecutive_failures == 0
        sup.record_failure("anchor_fw", reason="boom 3")
        sup.record_failure("anchor_fw", reason="boom 4")
        # Only 2 in a row after reset -> still ACTIVE
        assert sup.get_status("anchor_fw") == PayoutAnchorStatus.ACTIVE

    def test_offline_anchor_recovers_on_success(self):
        sup = RemittancePayoutAnchorHealthSupervisor()
        sup.register_anchor("anchor_fw", "https://fw.example.com")
        for i in range(3):
            sup.record_failure("anchor_fw", reason=f"fail {i}")
        assert sup.is_offline("anchor_fw") is True
        sup.record_success("anchor_fw")
        assert sup.get_status("anchor_fw") == PayoutAnchorStatus.ACTIVE
        assert sup.is_healthy("anchor_fw") is True

    def test_transitions_are_audited(self):
        sup = RemittancePayoutAnchorHealthSupervisor()
        sup.register_anchor("anchor_fw", "https://fw.example.com")
        for i in range(3):
            sup.record_failure("anchor_fw", reason=f"fail {i}")
        transitions = sup.get_transitions()
        assert len(transitions) == 1
        assert transitions[0].anchor_id == "anchor_fw"
        assert transitions[0].previous_status == "ACTIVE"
        assert transitions[0].new_status == "OFFLINE"


class TestReroutingAwayFromOffline:
    def test_healthy_anchors_excludes_offline(self):
        sup = RemittancePayoutAnchorHealthSupervisor()
        sup.register_anchor("anchor_fw", "https://fw.example.com")
        sup.register_anchor("anchor_beta", "https://beta.example.com")
        for _ in range(3):
            sup.record_failure("anchor_fw", reason="down")
        assert sup.healthy_anchors(["anchor_fw", "anchor_beta"]) == ["anchor_beta"]
        assert sup.offline_anchors() == ["anchor_fw"]

    def test_select_healthy_anchor_skips_offline_primary(self):
        sup = RemittancePayoutAnchorHealthSupervisor()
        sup.register_anchor("primary", "https://primary.example.com", priority=10)
        sup.register_anchor("backup", "https://backup.example.com", priority=1)
        for _ in range(3):
            sup.record_failure("primary", reason="down")
        assert sup.select_healthy_anchor(["primary", "backup"]) == "backup"

    def test_route_new_remittance_reports_reroute(self):
        sup = RemittancePayoutAnchorHealthSupervisor()
        sup.register_anchor("primary", "https://primary.example.com")
        sup.register_anchor("backup", "https://backup.example.com")
        for _ in range(3):
            sup.record_failure("primary", reason="down")
        decision = sup.route_new_remittance("USD -> NGN", ["primary", "backup"])
        assert decision["anchor_id"] == "backup"
        assert decision["rerouted"] is True
        assert "OFFLINE" in decision["reason"]
        assert "primary" in decision["excluded_offline"]

    def test_route_without_reroute_when_primary_healthy(self):
        sup = RemittancePayoutAnchorHealthSupervisor()
        sup.register_anchor("primary", "https://primary.example.com")
        sup.register_anchor("backup", "https://backup.example.com")
        decision = sup.route_new_remittance("USD -> NGN", ["primary", "backup"])
        assert decision["anchor_id"] == "primary"
        assert decision["rerouted"] is False

    def test_no_healthy_anchor_returns_none(self):
        sup = RemittancePayoutAnchorHealthSupervisor()
        sup.register_anchor("only", "https://only.example.com")
        for _ in range(3):
            sup.record_failure("only", reason="down")
        assert sup.select_healthy_anchor(["only"]) is None
        decision = sup.route_new_remittance("USD -> NGN", ["only"])
        assert decision["anchor_id"] is None
        assert decision["rerouted"] is True


class TestSharedStatusEnum:
    def test_offline_status_exists_for_router(self):
        assert AnchorHealthStatus.OFFLINE.value == "OFFLINE"
        assert AnchorHealthStatus.ACTIVE.value == "ACTIVE"


class TestWorkerIntegration:
    @pytest.mark.asyncio
    async def test_ping_success_and_failure_paths(self):
        from app.services.payout_anchor_health import PayoutAnchorAvailabilityWorker

        worker = PayoutAnchorAvailabilityWorker(
            database_url="postgres://user:pass@localhost:5432/testdb",
            pool_factory=AsyncMock(),
            redis_factory=MagicMock(),
        )

        class _Resp:
            status = 200

            async def read(self):
                return b"{}"

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return False

        class _Session:
            def get(self, url, timeout=None):
                assert url.endswith("/info")
                return _Resp()

        ok, detail = await worker.ping_anchor_info(_Session(), "https://a.example.com/info")
        assert ok is True
        assert "200" in detail

        class _BadResp(_Resp):
            status = 503

        class _BadSession:
            def get(self, url, timeout=None):
                return _BadResp()

        ok, detail = await worker.ping_anchor_info(_BadSession(), "https://a.example.com/info")
        assert ok is False

    @pytest.mark.asyncio
    async def test_worker_marks_offline_after_3_failed_cycles(self):
        from app.services.payout_anchor_health import PayoutAnchorAvailabilityWorker
        from src.analytics.remittance_anchor_health import RemittancePayoutAnchorHealthSupervisor

        supervisor = RemittancePayoutAnchorHealthSupervisor()
        worker = PayoutAnchorAvailabilityWorker(
            database_url="postgres://user:pass@localhost:5432/testdb",
            supervisor=supervisor,
            anchor_urls={"anchor_down": "https://down.example.com"},
            pool_factory=AsyncMock(),
            redis_factory=MagicMock(),
        )

        mock_connection = AsyncMock()
        mock_connection.fetch.return_value = [
            {
                "provider": "anchor_down",
                "senderCurrency": "USD",
                "receiverCurrency": "NGN",
                "priority": 1,
            },
            {
                "provider": "anchor_up",
                "senderCurrency": "USD",
                "receiverCurrency": "NGN",
                "priority": 5,
            },
        ]
        mock_connection.execute.return_value = "UPDATE 1"
        # anchor_up has no configured URL -> skipped (not penalised);
        # give it an explicit URL map so both are pinged? Only anchor_down is mapped.
        worker.anchor_urls = {"anchor_down": "https://down.example.com"}

        async def _fake_ping(session, info_url):
            if "down.example.com" in info_url:
                return False, "Connection refused"
            return True, "HTTP 200"

        worker.ping_anchor_info = _fake_ping  # type: ignore[method-assign]

        mock_pool = MagicMock()
        mock_pool.close = AsyncMock()
        mock_pool.acquire.return_value.__aenter__ = AsyncMock(return_value=mock_connection)
        mock_pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
        worker.pool_factory = AsyncMock(return_value=mock_pool)

        mock_redis = AsyncMock()
        worker.redis_factory = MagicMock(return_value=mock_redis)

        for _ in range(3):
            result = await worker.run_health_check_cycle()
            assert result["checked_anchors"] == 2

        assert supervisor.is_offline("anchor_down") is True
        assert supervisor.is_healthy("anchor_down") is False
        # OFFLINE anchor must never be selected for new remittances.
        assert worker.select_healthy_provider(["anchor_down", "anchor_up"]) == "anchor_up"

        # PaymentRoute should have been marked OFFLINE at least once.
        update_calls = [c for c in mock_connection.execute.call_args_list if "OFFLINE" in str(c.args[0])]
        assert len(update_calls) >= 1

    @pytest.mark.asyncio
    async def test_router_endpoints_exclude_offline(self):
        from app.routers.payout_anchor_health import (
            get_anchor_health_supervisor,
            list_healthy_anchors,
            preview_remittance_route,
            register_anchor,
        )

        supervisor = get_anchor_health_supervisor()
        supervisor.reset()

        await register_anchor(
            type("Req", (), {
                "anchor_id": "anchor_primary",
                "base_url": "https://primary.example.com",
                "corridor": "USD -> NGN",
                "priority": 10,
            })()
        )
        await register_anchor(
            type("Req", (), {
                "anchor_id": "anchor_backup",
                "base_url": "https://backup.example.com",
                "corridor": "USD -> NGN",
                "priority": 1,
            })()
        )
        for _ in range(3):
            supervisor.record_failure("anchor_primary", reason="simulated outage")

        healthy = await list_healthy_anchors(candidates="anchor_primary,anchor_backup")
        assert healthy["healthy_anchors"] == ["anchor_backup"]
        assert healthy["offline_anchors"] == ["anchor_primary"]

        preview = await preview_remittance_route(
            corridor="USD -> NGN",
            candidates="anchor_primary,anchor_backup",
        )
        assert preview.anchor_id == "anchor_backup"
        assert preview.rerouted is True

        supervisor.reset()
