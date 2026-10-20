"""FastAPI router for payout anchor availability health monitoring.

Issue #1288 — exposes supervisor state and lets operators trigger checks
and preview re-routing of new remittance transfers away from OFFLINE anchors.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

try:
    from fastapi import APIRouter, HTTPException, Query
    from pydantic import BaseModel, Field
except ImportError:  # pragma: no cover - fastapi/pydantic are declared deps
    class APIRouter:  # type: ignore
        def __init__(self, *args, **kwargs): pass
        def get(self, *args, **kwargs): return lambda f: f
        def post(self, *args, **kwargs): return lambda f: f

    class HTTPException(Exception):  # type: ignore
        def __init__(self, status_code: int, detail: str):
            super().__init__(detail)
            self.status_code = status_code

    class BaseModel:  # type: ignore
        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)

    def Query(default=None, **kwargs): return default  # type: ignore
    def Field(default=None, **kwargs): return default  # type: ignore

from src.analytics.remittance_anchor_health import (
    HEALTH_CHECK_INTERVAL_SECONDS,
    MAX_CONSECUTIVE_FAILURES,
    RemittancePayoutAnchorHealthSupervisor,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/remittance/anchors", tags=["Remittance Anchor Health"])

# Shared singleton supervisor for application-level state.
_global_supervisor = RemittancePayoutAnchorHealthSupervisor(
    max_consecutive_failures=MAX_CONSECUTIVE_FAILURES,
    check_interval_seconds=HEALTH_CHECK_INTERVAL_SECONDS,
)


def get_anchor_health_supervisor() -> RemittancePayoutAnchorHealthSupervisor:
    return _global_supervisor


class AnchorRegisterRequest(BaseModel):
    anchor_id: str
    base_url: str
    corridor: str = "ANY"
    priority: int = 0


class RoutePreviewResponse(BaseModel):
    corridor: str
    anchor_id: Optional[str] = None
    rerouted: bool = False
    reason: str = ""
    excluded_offline: List[str] = Field(default_factory=list)


@router.get("/health", response_model=Dict[str, Any])
async def get_anchors_health() -> Dict[str, Any]:
    """Snapshot of all payout anchor availability (ACTIVE/OFFLINE)."""
    return get_anchor_health_supervisor().snapshot()


@router.get("/healthy", response_model=Dict[str, Any])
async def list_healthy_anchors(
    candidates: Optional[str] = Query(None, description="Comma-separated anchor ids to filter"),
) -> Dict[str, Any]:
    """List anchors eligible for new remittance transfers (excludes OFFLINE)."""
    supervisor = get_anchor_health_supervisor()
    pool = [c.strip() for c in candidates.split(",") if c.strip()] if candidates else None
    healthy = supervisor.healthy_anchors(pool)
    return {
        "healthy_anchors": healthy,
        "offline_anchors": supervisor.offline_anchors(),
        "check_interval_seconds": supervisor.check_interval_seconds,
        "max_consecutive_failures": supervisor.max_consecutive_failures,
        "detection_window_seconds": supervisor.detection_window_seconds,
    }


@router.post("/register", response_model=Dict[str, Any])
async def register_anchor(request: AnchorRegisterRequest) -> Dict[str, Any]:
    """Register (or update) a payout anchor ``/info`` endpoint for supervision."""
    supervisor = get_anchor_health_supervisor()
    try:
        anchor = supervisor.register_anchor(
            anchor_id=request.anchor_id,
            base_url=request.base_url,
            corridor=request.corridor,
            priority=request.priority,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {
        "anchor_id": anchor.anchor_id,
        "base_url": anchor.base_url,
        "info_url": anchor.info_url,
        "corridor": anchor.corridor,
        "priority": anchor.priority,
        "status": supervisor.get_status(anchor.anchor_id).value,
    }


@router.get("/route", response_model=RoutePreviewResponse)
async def preview_remittance_route(
    corridor: str = Query("ANY", description="Corridor e.g. 'USD -> NGN'"),
    candidates: str = Query(..., description="Comma-separated candidate anchor ids in priority order"),
) -> RoutePreviewResponse:
    """Preview which healthy anchor a new remittance would be routed to.

    OFFLINE anchors are automatically skipped — demonstrating smooth
    re-routing without user disruption.
    """
    supervisor = get_anchor_health_supervisor()
    pool = [c.strip() for c in candidates.split(",") if c.strip()]
    if not pool:
        raise HTTPException(status_code=400, detail="candidates must list at least one anchor id")
    decision = supervisor.route_new_remittance(corridor, pool)
    return RoutePreviewResponse(
        corridor=decision["corridor"],
        anchor_id=decision["anchor_id"],
        rerouted=decision["rerouted"],
        reason=decision["reason"],
        excluded_offline=decision.get("excluded_offline", []),
    )


@router.get("/{anchor_id}", response_model=Dict[str, Any])
async def get_anchor_status(anchor_id: str) -> Dict[str, Any]:
    """Availability detail for a single payout anchor."""
    supervisor = get_anchor_health_supervisor()
    if supervisor.get_anchor(anchor_id) is None and anchor_id not in supervisor.snapshot()["availability"]:
        raise HTTPException(status_code=404, detail=f"Unknown anchor '{anchor_id}'")
    state = supervisor.get_availability(anchor_id)
    anchor = supervisor.get_anchor(anchor_id)
    return {
        **state.to_dict(),
        "info_url": anchor.info_url if anchor else None,
        "corridor": anchor.corridor if anchor else None,
    }
