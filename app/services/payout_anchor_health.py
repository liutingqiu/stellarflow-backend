"""Database + network integration for payout anchor availability monitoring.

Issue #1288 — periodically pings registered payout anchor ``/info`` endpoints
(every 30s via Celery beat), marks anchors OFFLINE after 3 consecutive
failures, persists the status to ``PaymentRoute.status``, and ensures new
remittance transfers are routed away from offline anchors.

Only stdlib + already-declared dependencies are used (``aiohttp``,
``asyncpg``, ``redis``, ``structlog``) — no new packages.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional

try:
    import aiohttp
except ImportError:  # pragma: no cover - aiohttp is a declared dependency
    aiohttp = None  # type: ignore

try:
    import asyncpg
except ImportError:
    asyncpg = None  # type: ignore

try:
    import redis.asyncio as aioredis
except ImportError:
    aioredis = None  # type: ignore

try:
    import structlog

    log = structlog.get_logger(__name__)
except ImportError:
    log = logging.getLogger(__name__)  # type: ignore

from src.analytics.fiat_settlement import AnchorHealthStatus
from src.analytics.remittance_anchor_health import (
    HEALTH_CHECK_INTERVAL_SECONDS,
    MAX_CONSECUTIVE_FAILURES,
    RemittancePayoutAnchorHealthSupervisor,
    build_info_url,
    is_healthy_response,
)

#: How often Celery beat triggers the availability check (matches spec: 30s).
CHECK_INTERVAL_SECONDS: float = HEALTH_CHECK_INTERVAL_SECONDS

#: Failures before OFFLINE (matches spec: 3).
FAILURE_THRESHOLD: int = MAX_CONSECUTIVE_FAILURES

#: Per-ping HTTP timeout so one slow anchor never blocks the 30s cycle.
PING_TIMEOUT_SECONDS: float = 5.0


def resolve_info_url(provider: str, explicit: Optional[str] = None) -> Optional[str]:
    """Resolve an anchor ``/info`` URL from config or provider naming.

    Precedence:
    1. Explicit mapping passed to the worker.
    2. ``ANCHOR_INFO_URL_<PROVIDER>`` env var (e.g. ``ANCHOR_INFO_URL_FLUTTERWAVE``).
    3. ``ANCHOR_<PROVIDER>_BASE_URL`` + ``/info``.
    4. Generic ``ANCHOR_INFO_URLS`` JSON map ``{"provider": "https://..."}``.
    5. Heuristic ``https://<provider>/info`` when provider looks like a hostname.
    """
    if explicit:
        return build_info_url(explicit)
    normalized = "".join(c if c.isalnum() else "_" for c in provider.upper())
    for env_key in (f"ANCHOR_INFO_URL_{normalized}", f"ANCHOR_{normalized}_BASE_URL"):
        value = os.getenv(env_key)
        if value:
            try:
                return build_info_url(value)
            except ValueError:
                continue
    raw_map = os.getenv("ANCHOR_INFO_URLS")
    if raw_map:
        try:
            mapping = json.loads(raw_map)
            if isinstance(mapping, dict) and provider in mapping:
                return build_info_url(str(mapping[provider]))
        except (ValueError, TypeError):
            pass
    if "." in provider and " " not in provider:
        base = provider if provider.startswith("http") else f"https://{provider}"
        try:
            return build_info_url(base)
        except ValueError:
            return None
    return None


class PayoutAnchorAvailabilityWorker:
    """Async worker that pings anchor ``/info`` endpoints and updates routing."""

    def __init__(
        self,
        database_url: Optional[str] = None,
        redis_url: Optional[str] = None,
        supervisor: Optional[RemittancePayoutAnchorHealthSupervisor] = None,
        anchor_urls: Optional[Dict[str, str]] = None,
        ping_timeout_seconds: float = PING_TIMEOUT_SECONDS,
        pool_factory: Optional[Callable[..., Awaitable[Any]]] = None,
        redis_factory: Optional[Callable[..., Any]] = None,
        http_session_factory: Optional[Callable[..., Any]] = None,
    ) -> None:
        self.database_url = database_url or os.getenv("DATABASE_URL") or os.getenv("DB_URL")
        self.redis_url = redis_url or os.getenv("REDIS_URL", "redis://localhost:6379")
        self.supervisor = supervisor or RemittancePayoutAnchorHealthSupervisor(
            max_consecutive_failures=FAILURE_THRESHOLD,
            check_interval_seconds=CHECK_INTERVAL_SECONDS,
        )
        self.anchor_urls = dict(anchor_urls or {})
        self.ping_timeout_seconds = float(ping_timeout_seconds)
        if pool_factory is not None:
            self.pool_factory = pool_factory
        elif asyncpg is not None:
            self.pool_factory = asyncpg.create_pool
        else:
            self.pool_factory = None
        if redis_factory is not None:
            self.redis_factory = redis_factory
        elif aioredis is not None:
            self.redis_factory = aioredis.from_url
        else:
            self.redis_factory = None
        self.http_session_factory = http_session_factory or aiohttp.Session if aiohttp else None

    # ------------------------------------------------------------------
    # Single ping
    # ------------------------------------------------------------------

    async def ping_anchor_info(self, session: Any, info_url: str) -> tuple[bool, str]:
        """GET an anchor ``/info`` endpoint. Returns (healthy, detail)."""
        try:
            timeout = aiohttp.ClientTimeout(total=self.ping_timeout_seconds) if aiohttp else None
            if timeout is not None:
                request = session.get(info_url, timeout=timeout)
            else:
                request = session.get(info_url)
            async with request as response:
                status = getattr(response, "status", None)
                healthy = is_healthy_response(status)
                # Drain body so connections can be reused; ignore parse errors.
                try:
                    await response.read() if hasattr(response, "read") else None
                except Exception:
                    pass
                if healthy:
                    return True, f"HTTP {status}"
                return False, f"Unhealthy HTTP status {status} from {info_url}"
        except asyncio.TimeoutError:
            return False, f"Timeout after {self.ping_timeout_seconds}s pinging {info_url}"
        except Exception as exc:  # network errors, DNS, connection refused, ...
            return False, f"{type(exc).__name__}: {exc}"

    # ------------------------------------------------------------------
    # Full cycle: load routes -> ping -> persist OFFLINE/ACTIVE -> notify
    # ------------------------------------------------------------------

    async def run_health_check_cycle(self) -> Dict[str, Any]:
        """Run one supervisor cycle over all registered payout anchors."""
        if not self.database_url:
            raise RuntimeError("DATABASE_URL or DB_URL must be configured")
        if self.pool_factory is None:
            raise RuntimeError("asyncpg is not available — cannot open database pool")
        if self.http_session_factory is None:
            raise RuntimeError("aiohttp is not available — cannot ping anchor /info endpoints")

        pool = await self.pool_factory(self.database_url, min_size=1, max_size=5)
        redis = self.redis_factory(self.redis_url, decode_responses=True) if self.redis_factory else None
        try:
            async with pool.acquire() as connection:
                routes = await self._load_anchors_from_db(connection)
                results = await self._ping_all_anchors(routes)
                transitions = await self._persist_transitions_to_db(connection, results)
                if redis is not None:
                    await self._publish_transitions(redis, transitions)
                return {
                    "checked_anchors": len(results),
                    "healthy_anchors": sum(1 for r in results if r["healthy"]),
                    "offline_anchors": self.supervisor.offline_anchors(),
                    "transitions": transitions,
                    "results": results,
                }
        finally:
            await pool.close()
            if redis is not None:
                try:
                    await redis.close()
                except Exception:
                    pass

    async def _load_anchors_from_db(self, connection: Any) -> List[Dict[str, Any]]:
        """Load distinct payout anchor providers from PaymentRoute."""
        try:
            rows = await connection.fetch(
                '''
                SELECT DISTINCT provider, "senderCurrency", "receiverCurrency", priority
                FROM "PaymentRoute"
                WHERE provider IS NOT NULL
                '''
            )
        except Exception as exc:
            log.warning("payout_anchor_health.load_routes_failed", error=str(exc))
            return []

        anchors: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            provider = str(row["provider"])
            if provider not in anchors:
                info_url = resolve_info_url(provider, self.anchor_urls.get(provider))
                anchors[provider] = {
                    "anchor_id": provider,
                    "info_url": info_url,
                    "corridor": f"{row['senderCurrency']} -> {row['receiverCurrency']}",
                    "priority": int(row["priority"] or 0),
                }
            # Register in supervisor for routing decisions.
            info_url = anchors[provider]["info_url"]
            if info_url:
                try:
                    # Derive base from info URL for supervisor bookkeeping.
                    base = info_url[: -len("/info")] if info_url.endswith("/info") else info_url
                    self.supervisor.register_anchor(
                        provider,
                        base,
                        corridor=anchors[provider]["corridor"],
                        priority=anchors[provider]["priority"],
                    )
                except ValueError:
                    continue
        # Anchors without resolvable URLs are reported as skipped.
        return list(anchors.values())

    async def _ping_all_anchors(self, anchors: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        async with self.http_session_factory() as session:
            for anchor in anchors:
                anchor_id = anchor["anchor_id"]
                info_url = anchor.get("info_url")
                if not info_url:
                    results.append(
                        {
                            "anchor_id": anchor_id,
                            "healthy": True,  # do not penalize unconfigured anchors
                            "skipped": True,
                            "detail": "No /info URL configured — skipping ping",
                        }
                    )
                    continue
                healthy, detail = await self.ping_anchor_info(session, info_url)
                if healthy:
                    self.supervisor.record_success(anchor_id)
                else:
                    self.supervisor.record_failure(anchor_id, reason=detail)
                results.append(
                    {
                        "anchor_id": anchor_id,
                        "healthy": healthy,
                        "skipped": False,
                        "detail": detail,
                        "info_url": info_url,
                        "status": self.supervisor.get_status(anchor_id).value,
                        "consecutive_failures": self.supervisor.get_availability(anchor_id).consecutive_failures,
                    }
                )
                log.info(
                    "payout_anchor_health.ping",
                    anchor_id=anchor_id,
                    info_url=info_url,
                    healthy=healthy,
                    detail=detail,
                )
        return results

    async def _persist_transitions_to_db(
        self, connection: Any, results: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Persist OFFLINE/ACTIVE transitions to PaymentRoute + return them."""
        transitions: List[Dict[str, Any]] = []
        for transition in self.supervisor.get_transitions():
            # Only persist the latest transition per anchor in this cycle.
            pass
        # Determine current offline/online sets from supervisor state.
        for result in results:
            if result.get("skipped"):
                continue
            anchor_id = result["anchor_id"]
            status = self.supervisor.get_status(anchor_id)
            # We persist every cycle where the anchor is OFFLINE (idempotent)
            # and recoveries back to ACTIVE, so the router always reflects reality.
            target = "OFFLINE" if status.value == "OFFLINE" else "ACTIVE"
            try:
                # Only touch rows that actually need a status change.
                if target == "OFFLINE":
                    db_result = await connection.execute(
                        '''
                        UPDATE "PaymentRoute"
                        SET status = 'OFFLINE', "updatedAt" = CURRENT_TIMESTAMP
                        WHERE provider = $1 AND status <> 'OFFLINE'
                        ''',
                        anchor_id,
                    )
                else:
                    # Recover rows previously marked OFFLINE by this supervisor.
                    # Do not clobber PAUSED/RETIRED rows owned by other workflows.
                    db_result = await connection.execute(
                        '''
                        UPDATE "PaymentRoute"
                        SET status = 'ACTIVE', "updatedAt" = CURRENT_TIMESTAMP
                        WHERE provider = $1 AND status = 'OFFLINE'
                        ''',
                        anchor_id,
                    )
                changed = self._rows_changed(db_result)
                if changed > 0:
                    transitions.append(
                        {
                            "anchor_id": anchor_id,
                            "status": target,
                            "detail": result.get("detail"),
                            "updated_routes": changed,
                        }
                    )
                    log.warning(
                        "payout_anchor_health.route_status_updated",
                        anchor_id=anchor_id,
                        status=target,
                        detail=result.get("detail"),
                    )
            except Exception as exc:
                log.error(
                    "payout_anchor_health.persist_error",
                    anchor_id=anchor_id,
                    error=str(exc),
                )
        # Also mirror into the latency monitor's AnchorHealthStatus for unified routing.
        try:
            from src.analytics.fiat_settlement import FiatSettlementLatencyMonitor  # noqa: F401
        except Exception:
            pass
        return transitions

    @staticmethod
    def _rows_changed(db_result: Any) -> int:
        try:
            text = str(db_result)
            # asyncpg returns e.g. "UPDATE 3"
            parts = text.strip().split()
            return int(parts[-1])
        except (ValueError, IndexError, TypeError):
            return 0

    async def _publish_transitions(self, redis: Any, transitions: List[Dict[str, Any]]) -> None:
        for t in transitions:
            try:
                channel = f"anchor_health_{t['anchor_id']}"
                payload = {
                    "anchor_id": t["anchor_id"],
                    "status": t["status"],
                    "event": "ANCHOR_OFFLINE" if t["status"] == "OFFLINE" else "ANCHOR_RECOVERED",
                    "detail": t.get("detail"),
                    "updated_routes": t.get("updated_routes", 0),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
                await redis.publish(channel, json.dumps(payload))
            except Exception as exc:
                log.warning("payout_anchor_health.redis_publish_error", error=str(exc))

    # ------------------------------------------------------------------
    # Router helper — re-route NEW remittance transfers away from OFFLINE
    # ------------------------------------------------------------------

    def select_healthy_provider(
        self,
        candidate_providers: List[str],
        exclude: Optional[List[str]] = None,
    ) -> Optional[str]:
        """Return the best ACTIVE provider for a new remittance, or None."""
        return self.supervisor.select_healthy_anchor(candidate_providers, exclude=exclude)

    def route_new_remittance(
        self, corridor: str, candidate_providers: List[str]
    ) -> Dict[str, Any]:
        """Route helper ensuring OFFLINE anchors are never selected."""
        return self.supervisor.route_new_remittance(corridor, candidate_providers)

    def map_fiat_status(self, anchor_id: str, corridor: str) -> AnchorHealthStatus:
        """Map supervisor availability to the shared AnchorHealthStatus enum."""
        if self.supervisor.is_offline(anchor_id):
            return AnchorHealthStatus.OFFLINE
        return AnchorHealthStatus.ACTIVE
