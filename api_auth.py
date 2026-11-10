"""
api_auth.py: caller identity for the main UCANRR API.

Every request may carry `Authorization: Bearer <token>`, where the token is
either a Firebase ID token (mobile app) or a Google ID token (web pages).
The middleware verifies it and puts the verified email on
`request.state.auth_email`.

AUTH_MODE (App Service setting) decides what happens to a request without a
valid token:
    off      no checking at all
    report   (default) allow it, but log it, so callers can be found and fixed
    enforce  reject it with 401

Log lines never contain the token or the email, only the outcome, token kind,
method, path, origin and app user agent, so they are safe to keep.

Per-role rules (which client a caller may read) live in api_authz.py; once
a token verifies, the middleware asks it whether this caller may make this
request. A denial is logged as authz=deny, and is a 403 only in enforce.

Add to ucanrr1_api_with_roles_azure.py, after `app` is created and BEFORE the
CORS middleware, so CORS stays outermost and a 401 still carries CORS headers:

    from api_auth import install_auth
    install_auth(app)
"""
from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any, Dict, Optional, Tuple

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

log = logging.getLogger("ucanrr.auth")


def _env(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()


AUTH_MODE = _env("AUTH_MODE", "report").lower()
if AUTH_MODE not in ("off", "report", "enforce"):
    log.error("AUTH_MODE=%r is not off/report/enforce; using report", AUTH_MODE)
    AUTH_MODE = "report"

# Google ID tokens from the web pages must be issued to one of these OAuth
# clients. Defaults to the web client the study pages already use.
WEB_CLIENT_IDS = {c.strip() for c in _env(
    "WEB_GOOGLE_CLIENT_IDS",
    _env("STUDY_GOOGLE_CLIENT_ID") or _env("GOOGLE_CLIENT_ID"),
).split(",") if c.strip()}

# Paths that never need a token: health checks, the API root, the study
# router (it verifies its own tokens), and the app's current sign-in endpoint,
# which must stay open until the app update replaces it.
OPEN_PREFIXES = ("/study/",)
OPEN_PATHS = {"/", "/health", "/auth/firebase-token"}

_GOOGLE_ISSUERS = ("accounts.google.com", "https://accounts.google.com")

_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_cache_lock = threading.Lock()
_google_request = None


def _google_req():
    global _google_request
    if _google_request is None:
        import requests
        from google.auth.transport import requests as g_requests
        _google_request = g_requests.Request(session=requests.Session())
    return _google_request


def _verify_firebase(token: str) -> Optional[Dict[str, Any]]:
    try:
        from firebase_admin import auth as firebase_auth
        claims = firebase_auth.verify_id_token(token)
    except Exception:
        return None
    # Only a verified email counts. Sessions made from /auth/firebase-token
    # custom tokens carry no email claim (the uid is an unproven email), so
    # they are rejected here and show up as auth=invalid until retired.
    email = claims.get("email")
    if not email or not claims.get("email_verified"):
        return None
    return {"kind": "firebase", "email": email, "exp": float(claims.get("exp", 0))}


def _verify_google(token: str) -> Optional[Dict[str, Any]]:
    if not WEB_CLIENT_IDS:
        return None
    try:
        from google.oauth2 import id_token
        claims = id_token.verify_oauth2_token(token, _google_req(), audience=None)
    except Exception:
        return None
    if claims.get("aud") not in WEB_CLIENT_IDS or claims.get("iss") not in _GOOGLE_ISSUERS:
        return None
    if not claims.get("email") or not claims.get("email_verified"):
        return None
    return {"kind": "google", "email": claims["email"], "exp": float(claims.get("exp", 0))}


def verify_bearer(token: str) -> Optional[Dict[str, Any]]:
    """Return {'kind', 'email'} for a valid token, else None. Cached until expiry."""
    now = time.time()
    with _cache_lock:
        hit = _cache.get(token)
        if hit and hit[0] > now + 5:
            return hit[1]
    result = _verify_firebase(token) or _verify_google(token)
    if result:
        with _cache_lock:
            if len(_cache) > 5000:
                for k in [k for k, v in _cache.items() if v[0] <= now]:
                    _cache.pop(k, None)
            _cache[token] = (result["exp"], result)
    return result


def _is_open(path: str) -> bool:
    return path in OPEN_PATHS or path.startswith(OPEN_PREFIXES)


def install_auth(app: FastAPI) -> None:
    log.info("API auth installed: AUTH_MODE=%s, web client ids=%d", AUTH_MODE, len(WEB_CLIENT_IDS))
    if AUTH_MODE == "off":
        return

    @app.middleware("http")
    async def _auth(request: Request, call_next):
        request.state.auth_email = None
        request.state.auth_kind = None
        request.state.caller = None
        path = request.url.path
        if request.method == "OPTIONS" or _is_open(path):
            return await call_next(request)

        from starlette.concurrency import run_in_threadpool
        header = request.headers.get("authorization", "")
        outcome, kind = "missing", None
        if header.lower().startswith("bearer ") and header[7:].strip():
            # Token checks may fetch Google's certificates; keep them off the event loop.
            verified = await run_in_threadpool(verify_bearer, header[7:].strip())
            if verified:
                outcome, kind = "ok", verified["kind"]
                request.state.auth_email = verified["email"].lower()
                request.state.auth_kind = kind
            else:
                outcome = "invalid"

        if outcome == "ok":
            # Who they are is settled; now check what they may do (api_authz.py).
            from api_authz import decide
            try:
                reason, caller = await run_in_threadpool(
                    decide, request.method, path, dict(request.query_params), request.state.auth_email)
                request.state.caller = caller
            except Exception:
                log.exception("authz lookup failed %s %s", request.method, path)
                if AUTH_MODE == "enforce":
                    return JSONResponse({"detail": "Please try again in a moment."}, status_code=503)
                reason = None
            if reason:
                log.warning("authz=deny mode=%s kind=%s %s %s reason=%s", AUTH_MODE, kind,
                            request.method, path, reason)
                if AUTH_MODE == "enforce":
                    return JSONResponse({"detail": "You don't have access to this."}, status_code=403)

        if outcome != "ok":
            log.warning("auth=%s mode=%s %s %s origin=%s ua=%s", outcome, AUTH_MODE, request.method, path,
                        request.headers.get("origin", "-"), request.headers.get("user-agent", "-")[:60])
            if AUTH_MODE == "enforce":
                return JSONResponse({"detail": "Please sign in."}, status_code=401,
                                    headers={"WWW-Authenticate": "Bearer"})
        return await call_next(request)
