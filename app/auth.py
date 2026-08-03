"""C1: Guard access key authentication middleware."""
import logging
import secrets

from fastapi import HTTPException, Request

from app.config import settings

logger = logging.getLogger(__name__)

# Paths that don't require auth
PUBLIC_PATHS = {"/health", "/ready", "/docs", "/openapi.json", "/redoc"}


async def verify_guard_key(request: Request) -> None:
    """Verify Guard access key from X-Access-Key header.

    In production, this service sits behind Guard which handles full auth.
    This is an additional layer for direct callers within the trust boundary.
    """
    if request.url.path in PUBLIC_PATHS:
        return

    if not settings.guard_access_key:
        # No key configured — skip auth (dev mode)
        return

    provided = request.headers.get("X-Access-Key", "")
    # Constant-time comparison — a plain != leaks key prefix length via timing
    if not secrets.compare_digest(provided, settings.guard_access_key):
        logger.warning("Unauthorized request to %s from %s", request.url.path, request.client)
        raise HTTPException(status_code=401, detail="Invalid or missing access key")
