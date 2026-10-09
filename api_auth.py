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

Per-role rules (which client a caller may read) are a later step; this module
only answers "who is calling".

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
    # The app's current custom-token sign-in uses the email as the uid, so
    # fall back to it when the token carries no email claim.
    email = claims.get("email") or (claims.get("uid") if "@" in str(claims.get("uid", "")) else None)
    return {"kind": "firebase", "email": email, "exp": float(claims.get("exp", 0))} if email else None


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
        path = request.url.path
        if request.method == "OPTIONS" or _is_open(path):
            return await call_next(request)

        header = request.headers.get("authorization", "")
        outcome, kind = "missing", None
        if header.lower().startswith("bearer ") and header[7:].strip():
            # Token checks may fetch Google's certificates; keep them off the event loop.
            from starlette.concurrency import run_in_threadpool
            verified = await run_in_threadpool(verify_bearer, header[7:].strip())
            if verified:
                outcome, kind = "ok", verified["kind"]
                request.state.auth_email = verified["email"].lower()
                request.state.auth_kind = kind
            else:
                outcome = "invalid"

        if outcome != "ok":
            log.warning("auth=%s mode=%s %s %s origin=%s ua=%s", outcome, AUTH_MODE, request.method, path,
                        request.headers.get("origin", "-"), request.headers.get("user-agent", "-")[:60])
            if AUTH_MODE == "enforce":
                return JSONResponse({"detail": "Please sign in."}, status_code=401,
                                    headers={"WWW-Authenticate": "Bearer"})
        return await call_next(request)
