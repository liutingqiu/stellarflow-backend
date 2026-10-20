"""FastAPI router for ZK proof-generation queue (Issue #1262).

- POST /zk/proofs/generate — rate-limited, priority-routed enqueue.
  Remittance redemption proofs always land on ``zk_high_priority``.
- GET /zk/proofs/queues — queue topology + rate-limit config for ops.
"""

from __future__ import annotations

from typing import Any, List, Optional

import structlog
from fastapi import APIRouter, HTTPException, Response, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

log = structlog.get_logger(__name__)

router = APIRouter(prefix="/zk", tags=["ZK Proofs"])


class ZkProofGenerateRequest(BaseModel):
    user_id: str = Field(..., min_length=1, description="Requesting user id (rate-limit scope)")
    job_type: str = Field(
        default="standard",
        description="Job kind; remittance_redemption routes to high priority",
    )
    priority: str = Field(
        default="standard",
        description="Requested priority: standard | high (redemption always high)",
    )
    proof_hex: str = Field(default="", description="Hex-encoded proof inputs / witness")
    public_inputs: List[str] = Field(default_factory=list)
    proof_scheme: str = Field(default="groth16")
    transaction_id: Optional[str] = Field(default=None)


class ZkProofGenerateResponse(BaseModel):
    accepted: bool = True
    task_id: str
    queue: str
    task_name: str
    priority: str
    remaining: int
    retry_after: float = 0.0


@router.post(
    "/proofs/generate",
    response_model=ZkProofGenerateResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Enqueue a ZK proof-generation job (rate-limited, prioritized)",
)
async def enqueue_zk_proof(payload: ZkProofGenerateRequest, response: Response) -> Any:
    """Enforce per-user rate limits then route to the right Celery queue."""
    try:
        from app.services.zk_proof_queue import (
            RateLimitExceeded,
            request_proof_generation,
        )
    except ImportError as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail="proof queue unavailable") from exc

    try:
        result = request_proof_generation(
            user_id=payload.user_id,
            job_type=payload.job_type,
            priority=payload.priority,
            proof_hex=payload.proof_hex,
            public_inputs=payload.public_inputs,
            proof_scheme=payload.proof_scheme,
            transaction_id=payload.transaction_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except RateLimitExceeded as exc:  # type: ignore[misc]
        log.info(
            "zk_proof.rate_limited",
            user_id=payload.user_id,
            job_type=payload.job_type,
            retry_after=exc.retry_after,
        )
        return JSONResponse(
            status_code=429,
            content={
                "detail": str(exc),
                "retry_after": exc.retry_after,
                "limit": exc.limit,
            },
            headers={"Retry-After": str(int(exc.retry_after) or 1)},
        )
    except Exception as exc:
        log.exception("zk_proof.enqueue_error", error=str(exc))
        raise HTTPException(status_code=503, detail="proof queue unavailable") from exc

    response.headers["X-ZK-Queue"] = result.queue
    response.headers["X-RateLimit-Remaining"] = str(result.remaining)
    log.info(
        "zk_proof.enqueued",
        task_id=result.task_id,
        queue=result.queue,
        priority=result.priority,
        user_id=payload.user_id,
        job_type=payload.job_type,
    )
    return ZkProofGenerateResponse(
        accepted=True,
        task_id=result.task_id,
        queue=result.queue,
        task_name=result.task_name,
        priority=result.priority,
        remaining=result.remaining,
        retry_after=result.retry_after,
    )


@router.get(
    "/proofs/queues",
    summary="Describe ZK proof queues and rate-limit config",
)
async def describe_zk_queues() -> Any:
    from app.services.zk_proof_queue import describe_queues

    return describe_queues()
