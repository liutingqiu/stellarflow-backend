"""Cross-Border Remittance Payout Anchor Availability Health Monitor.

Issue #1288 — Build health supervisor service that periodically checks payout
anchor API availability and marks un-responsive anchors as OFFLINE in the router.

Technical requirements:
- Issue HTTP health check pings to registered payout anchor ``/info`` endpoints
  every 30 seconds.
- Mark anchor status as ``OFFLINE`` if pings fail 3 consecutive times.
- Automatically re-route incoming remittance transfers away from offline anchors.

Acceptance:
- Detects offline payout anchors within 90 seconds of failure (30s * 3).
- Re-routes new remittance requests smoothly without user disruption
  (offline anchors are excluded from route selection).

This module is deliberately dependency-free (stdlib only) so it can be unit
tested without network access. HTTP I/O is injected by callers
(``app/services/payout_anchor_health.py`` performs real ``/info`` pings).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Tuning constants — directly encode the acceptance criteria.
# ---------------------------------------------------------------------------

#: How often the supervisor pings each registered anchor ``/info`` endpoint.
HEALTH_CHECK_INTERVAL_SECONDS: float = 30.0

#: Consecutive failures before an anchor is marked OFFLINE.
MAX_CONSECUTIVE_FAILURES: int = 3

#: Worst-case detection latency: 30s interval x 3 failures = 90s.
OFFLINE_DETECTION_WINDOW_SECONDS: float = (
    HEALTH_CHECK_INTERVAL_SECONDS * MAX_CONSECUTIVE_FAILURES
)

#: Consecutive successes required to recover an OFFLINE anchor to ACTIVE.
RECOVERY_SUCCESS_THRESHOLD: int = 1

#: Status string persisted to ``PaymentRoute.status`` for unreachable anchors.
OFFLINE_STATUS: str = "OFFLINE"

#: Status string for reachable anchors eligible for new remittance traffic.
ACTIVE_STATUS: str = "ACTIVE"


class PayoutAnchorStatus(str, Enum):
    """Availability status for a payout anchor in the remittance router."""

    ACTIVE = "ACTIVE"
    OFFLINE = "OFFLINE"


def build_info_url(base_url: str) -> str:
    """Return the Stellar ``/info`` endpoint for a payout anchor base URL."""
    base = (base_url or "").strip().rstrip("/")
    if not base:
        raise ValueError("anchor base_url must be a non-empty string")
    if base.endswith("/info"):
        return base
    return f"{base}/info"


def is_healthy_response(status_code: Optional[int], payload: Any = None) -> bool:
    """Return True when an ``/info`` ping counts as a successful health check.

    Any 2xx HTTP status is healthy. Payload content is not required — an
    anchor that answers ``/info`` is considered available.
    """
    if status_code is None:
        return False
    try:
        code = int(status_code)
    except (TypeError, ValueError):
        return False
    return 200 <= code < 300


@dataclass
class PayoutAnchor:
    """Registered payout anchor endpoint."""

    anchor_id: str
    base_url: str
    corridor: str = "ANY"
    priority: int = 0

    @property
    def info_url(self) -> str:
        return build_info_url(self.base_url)


@dataclass
class AnchorAvailability:
    """Mutable per-anchor health bookkeeping."""

    anchor_id: str
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    status: PayoutAnchorStatus = PayoutAnchorStatus.ACTIVE
    last_check_at: Optional[datetime] = None
    last_success_at: Optional[datetime] = None
    last_failure_at: Optional[datetime] = None
    last_failure_reason: Optional[str] = None
    total_checks: int = 0
    total_failures: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "anchor_id": self.anchor_id,
            "status": self.status.value,
            "consecutive_failures": self.consecutive_failures,
            "consecutive_successes": self.consecutive_successes,
            "last_check_at": self.last_check_at.isoformat() if self.last_check_at else None,
            "last_success_at": self.last_success_at.isoformat() if self.last_success_at else None,
            "last_failure_at": self.last_failure_at.isoformat() if self.last_failure_at else None,
            "last_failure_reason": self.last_failure_reason,
            "total_checks": self.total_checks,
            "total_failures": self.total_failures,
        }


@dataclass
class AnchorHealthTransition:
    """Record of a status change (ACTIVE <-> OFFLINE)."""

    anchor_id: str
    previous_status: str
    new_status: str
    consecutive_failures: int
    reason: str
    transitioned_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        return {
            "anchor_id": self.anchor_id,
            "previous_status": self.previous_status,
            "new_status": self.new_status,
            "consecutive_failures": self.consecutive_failures,
            "reason": self.reason,
            "transitioned_at": self.transitioned_at.isoformat(),
        }


class RemittancePayoutAnchorHealthSupervisor:
    """In-memory supervisor tracking payout anchor availability.

    Usage:
        supervisor = RemittancePayoutAnchorHealthSupervisor()
        supervisor.register_anchor("anchor_fw", "https://fw.example.com")
        supervisor.record_failure("anchor_fw", reason="timeout")
        # ... after 3 consecutive failures status becomes OFFLINE ...
        supervisor.get_status("anchor_fw")  # PayoutAnchorStatus.OFFLINE
        supervisor.select_healthy_anchor(["anchor_fw", "anchor_beta"])
    """

    def __init__(
        self,
        max_consecutive_failures: int = MAX_CONSECUTIVE_FAILURES,
        check_interval_seconds: float = HEALTH_CHECK_INTERVAL_SECONDS,
    ) -> None:
        if max_consecutive_failures < 1:
            raise ValueError("max_consecutive_failures must be >= 1")
        if check_interval_seconds <= 0:
            raise ValueError("check_interval_seconds must be positive")
        self.max_consecutive_failures = int(max_consecutive_failures)
        self.check_interval_seconds = float(check_interval_seconds)
        self._anchors: Dict[str, PayoutAnchor] = {}
        self._availability: Dict[str, AnchorAvailability] = {}
        self._transitions: List[AnchorHealthTransition] = []

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register_anchor(
        self,
        anchor_id: str,
        base_url: str,
        corridor: str = "ANY",
        priority: int = 0,
    ) -> PayoutAnchor:
        """Register (or update) a payout anchor ``/info`` endpoint."""
        anchor_id = (anchor_id or "").strip()
        if not anchor_id:
            raise ValueError("anchor_id must be a non-empty string")
        # Validates URL shape eagerly so misconfiguration fails fast.
        build_info_url(base_url)
        anchor = PayoutAnchor(
            anchor_id=anchor_id,
            base_url=base_url.strip().rstrip("/"),
            corridor=(corridor or "ANY").strip() or "ANY",
            priority=int(priority),
        )
        self._anchors[anchor_id] = anchor
        if anchor_id not in self._availability:
            self._availability[anchor_id] = AnchorAvailability(anchor_id=anchor_id)
        return anchor

    def unregister_anchor(self, anchor_id: str) -> bool:
        """Remove an anchor from supervision. Returns True if it existed."""
        removed = self._anchors.pop(anchor_id, None) is not None
        self._availability.pop(anchor_id, None)
        return removed

    def registered_anchor_ids(self) -> List[str]:
        return sorted(self._anchors.keys())

    def get_anchor(self, anchor_id: str) -> Optional[PayoutAnchor]:
        return self._anchors.get(anchor_id)

    # ------------------------------------------------------------------
    # Health bookkeeping — the heart of issue #1288
    # ------------------------------------------------------------------

    def record_success(self, anchor_id: str) -> AnchorAvailability:
        """Record a successful ``/info`` ping; recovers OFFLINE anchors."""
        now = datetime.now(timezone.utc)
        state = self._ensure_state(anchor_id)
        previous = state.status
        state.total_checks += 1
        state.consecutive_successes += 1
        state.consecutive_failures = 0
        state.last_check_at = now
        state.last_success_at = now
        state.last_failure_reason = None
        if previous == PayoutAnchorStatus.OFFLINE and state.consecutive_successes >= RECOVERY_SUCCESS_THRESHOLD:
            state.status = PayoutAnchorStatus.ACTIVE
            self._record_transition(
                anchor_id=anchor_id,
                previous_status=previous.value,
                new_status=state.status.value,
                reason="Anchor /info health check recovered — marking ACTIVE",
            )
            logger.info(
                "Payout anchor '%s' recovered — marking ACTIVE in router",
                anchor_id,
            )
        return state

    def record_failure(self, anchor_id: str, reason: str = "health check failed") -> AnchorAvailability:
        """Record a failed ``/info`` ping; marks OFFLINE after 3 in a row."""
        now = datetime.now(timezone.utc)
        state = self._ensure_state(anchor_id)
        previous = state.status
        state.total_checks += 1
        state.total_failures += 1
        state.consecutive_failures += 1
        state.consecutive_successes = 0
        state.last_check_at = now
        state.last_failure_at = now
        state.last_failure_reason = reason
        if (
            previous != PayoutAnchorStatus.OFFLINE
            and state.consecutive_failures >= self.max_consecutive_failures
        ):
            state.status = PayoutAnchorStatus.OFFLINE
            self._record_transition(
                anchor_id=anchor_id,
                previous_status=previous.value,
                new_status=state.status.value,
                reason=(
                    f"Anchor /info failed {state.consecutive_failures} consecutive times "
                    f"(>= {self.max_consecutive_failures}) — marking OFFLINE. Last error: {reason}"
                ),
            )
            logger.warning(
                "Payout anchor '%s' OFFLINE after %d consecutive /info failures — "
                "re-routing new remittances away",
                anchor_id,
                state.consecutive_failures,
            )
        return state

    def record_probe(
        self,
        anchor_id: str,
        success: bool,
        reason: str = "health check failed",
    ) -> AnchorAvailability:
        """Convenience wrapper recording a boolean probe outcome."""
        if success:
            return self.record_success(anchor_id)
        return self.record_failure(anchor_id, reason=reason)

    def _ensure_state(self, anchor_id: str) -> AnchorAvailability:
        state = self._availability.get(anchor_id)
        if state is None:
            state = AnchorAvailability(anchor_id=anchor_id)
            self._availability[anchor_id] = state
        return state

    def _record_transition(
        self,
        anchor_id: str,
        previous_status: str,
        new_status: str,
        reason: str,
    ) -> AnchorHealthTransition:
        transition = AnchorHealthTransition(
            anchor_id=anchor_id,
            previous_status=previous_status,
            new_status=new_status,
            consecutive_failures=self._availability[anchor_id].consecutive_failures,
            reason=reason,
        )
        self._transitions.append(transition)
        return transition

    # ------------------------------------------------------------------
    # Router integration — re-route new remittances away from OFFLINE
    # ------------------------------------------------------------------

    def get_status(self, anchor_id: str) -> PayoutAnchorStatus:
        """Current availability status (defaults to ACTIVE if never probed)."""
        state = self._availability.get(anchor_id)
        if state is None:
            return PayoutAnchorStatus.ACTIVE
        return state.status

    def is_offline(self, anchor_id: str) -> bool:
        return self.get_status(anchor_id) == PayoutAnchorStatus.OFFLINE

    def is_healthy(self, anchor_id: str) -> bool:
        return self.get_status(anchor_id) == PayoutAnchorStatus.ACTIVE

    def healthy_anchors(self, candidates: Optional[List[str]] = None) -> List[str]:
        """Filter a candidate list down to ACTIVE anchors only."""
        pool = list(candidates) if candidates is not None else self.registered_anchor_ids()
        return [a for a in pool if self.is_healthy(a)]

    def offline_anchors(self) -> List[str]:
        """All anchors currently marked OFFLINE."""
        return [a for a in self._availability if self._availability[a].status == PayoutAnchorStatus.OFFLINE]

    def select_healthy_anchor(
        self,
        candidates: List[str],
        exclude: Optional[List[str]] = None,
    ) -> Optional[str]:
        """Pick the first healthy anchor for a new remittance transfer.

        Respects explicit ``exclude`` list plus any OFFLINE anchors, and
        prefers higher registered priority (then lexicographic order for
        determinism). Returns None when no healthy anchor exists.
        """
        excluded = set(exclude or [])
        excluded.update(self.offline_anchors())
        healthy = [c for c in candidates if c not in excluded and self.is_healthy(c)]
        if not healthy:
            return None

        def _rank(anchor_id: str) -> tuple:
            anchor = self._anchors.get(anchor_id)
            priority = anchor.priority if anchor else 0
            return (-priority, anchor_id)

        healthy.sort(key=_rank)
        return healthy[0]

    def route_new_remittance(
        self,
        corridor: str,
        candidate_anchor_ids: List[str],
    ) -> Dict[str, Any]:
        """Route helper for incoming remittance transfers.

        Returns ``{"anchor_id": ..., "rerouted": bool, ...}`` so callers can
        log whether the primary choice was skipped due to OFFLINE status.
        """
        if not candidate_anchor_ids:
            return {
                "corridor": corridor,
                "anchor_id": None,
                "rerouted": False,
                "reason": "No candidate anchors registered for corridor",
            }
        primary = candidate_anchor_ids[0]
        selected = self.select_healthy_anchor(candidate_anchor_ids)
        if selected is None:
            return {
                "corridor": corridor,
                "anchor_id": None,
                "rerouted": True,
                "reason": "All candidate payout anchors are OFFLINE",
                "excluded_offline": self.offline_anchors(),
            }
        return {
            "corridor": corridor,
            "anchor_id": selected,
            "rerouted": selected != primary,
            "reason": (
                f"Primary anchor '{primary}' OFFLINE — rerouted to '{selected}'"
                if selected != primary
                else f"Routed to healthy anchor '{selected}'"
            ),
            "excluded_offline": [a for a in candidate_anchor_ids if self.is_offline(a)],
        }

    # ------------------------------------------------------------------
    # Inspection
    # ------------------------------------------------------------------

    def get_availability(self, anchor_id: str) -> AnchorAvailability:
        return self._ensure_state(anchor_id)

    def snapshot(self) -> Dict[str, Any]:
        """Full supervisor snapshot for API responses and logging."""
        return {
            "check_interval_seconds": self.check_interval_seconds,
            "max_consecutive_failures": self.max_consecutive_failures,
            "detection_window_seconds": self.check_interval_seconds * self.max_consecutive_failures,
            "registered_anchors": {
                anchor_id: {
                    "anchor_id": a.anchor_id,
                    "base_url": a.base_url,
                    "info_url": a.info_url,
                    "corridor": a.corridor,
                    "priority": a.priority,
                }
                for anchor_id, a in sorted(self._anchors.items())
            },
            "availability": {
                anchor_id: state.to_dict() for anchor_id, state in sorted(self._availability.items())
            },
            "healthy_anchors": self.healthy_anchors(),
            "offline_anchors": self.offline_anchors(),
            "transitions": [t.to_dict() for t in self._transitions],
        }

    def get_transitions(self) -> List[AnchorHealthTransition]:
        return list(self._transitions)

    def reset(self) -> None:
        """Clear all registration, health state, and transition history."""
        self._anchors.clear()
        self._availability.clear()
        self._transitions.clear()

    @property
    def detection_window_seconds(self) -> float:
        """Worst-case time to mark an anchor OFFLINE (interval x threshold)."""
        return self.check_interval_seconds * self.max_consecutive_failures
