"""
api_authz.py: what a verified caller may do on the main UCANRR API.

api_auth.py answers "who is calling" (a verified email). This module answers
"may they do this": the email must belong to an active AuthorizedUsers row
with an active role, and then

    Admin        everything
    Therapist    their own Therapist row(s), the couples assigned to them, and
                 those couples' events, session dates and safety assessments
                 (a therapist is any user whose email is a TherapistUserName)
    Client       their own couple record and its events (the mobile app)
    anyone       their own AuthorizedUsers row, /auth/me, and read-only
                 study configuration (studies, assessments, questions, roles)

Everything not listed above is admin-only (default deny).

The decision follows AUTH_MODE like the identity check does: in `report` a
denial is only logged (authz=deny ...), so callers that would break can be
found and fixed first; in `enforce` it is a 403.

Request bodies are not read here. Endpoints that take a ClientId,
TherapistID or RoleID in the body call `require_body_access()` themselves.
"""
from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Optional, Set, Tuple

from fastapi import HTTPException, Request

log = logging.getLogger("ucanrr.authz")

_CACHE_SECONDS = 60
_cache: dict = {}
_cache_lock = threading.Lock()

ADMIN_ROLE_NAMES = {"admin"}
CLIENT_ROLE_NAMES = {"client"}

# Read-only for any authorized user; writes are admin-only.
CONFIG_READ_OPEN = {"roles", "studies", "assessments", "study-assessments", "study-questions"}
# Item routes whose ownership is the row's ClientId.
CLIENT_OWNED_TABLES = {"events": "Event", "session-dates": "SessionDate", "safety-assessments": "SafetyAssessment"}


@dataclass
class Caller:
    email: str
    user_id: Optional[int] = None
    role_id: Optional[int] = None
    role_name: str = ""
    active: bool = False
    therapist_ids: Set[int] = field(default_factory=set)
    own_client_ids: Set[int] = field(default_factory=set)      # couples the caller belongs to
    client_ids: Set[int] = field(default_factory=set)          # couples the caller may see

    @property
    def known(self) -> bool:
        return self.user_id is not None

    @property
    def is_admin(self) -> bool:
        return self.active and (self.role_id == 1 or self.role_name.lower() in ADMIN_ROLE_NAMES)

    @property
    def is_therapist(self) -> bool:
        return self.active and bool(self.therapist_ids)


def _connect():
    import pyodbc
    return pyodbc.connect(os.environ.get("AZURE_SQL_CONNECTION_STRING", ""))


def _load_caller(email: str) -> Caller:
    c = Caller(email=email)
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT au.UserID, au.RoleID, au.IsActive, r.RoleName, r.IsActive AS RoleActive "
            "FROM AuthorizedUsers au LEFT JOIN Roles r ON r.RoleID = au.RoleID "
            "WHERE LOWER(au.Email) = ?", email)
        row = cur.fetchone()
        if not row:
            return c
        c.user_id, c.role_id = int(row.UserID), row.RoleID
        c.role_name = (row.RoleName or "").strip()
        c.active = bool(row.IsActive) and row.RoleActive is not False

        cur.execute("SELECT Id FROM Therapist WHERE LOWER(LTRIM(RTRIM(TherapistUserName))) = ?", email)
        c.therapist_ids = {int(r.Id) for r in cur.fetchall()}

        # Usernames are nchar, so trim before comparing.
        cur.execute(
            "SELECT Id FROM Client WHERE LOWER(LTRIM(RTRIM(Client1UserName))) = ? "
            "OR LOWER(LTRIM(RTRIM(Client2UserName))) = ?", email, email)
        c.own_client_ids = {int(r.Id) for r in cur.fetchall()}
        c.client_ids = set(c.own_client_ids)
        if c.therapist_ids:
            marks = ",".join("?" * len(c.therapist_ids))
            cur.execute(f"SELECT Id FROM Client WHERE TherapistID IN ({marks})", *c.therapist_ids)
            c.client_ids |= {int(r.Id) for r in cur.fetchall()}
        return c
    finally:
        conn.close()


def get_caller(email: str) -> Caller:
    email = email.lower()
    now = time.time()
    with _cache_lock:
        hit = _cache.get(email)
        if hit and hit[0] > now:
            return hit[1]
    caller = _load_caller(email)
    with _cache_lock:
        if len(_cache) > 2000:
            _cache.clear()
        _cache[email] = (now + _CACHE_SECONDS, caller)
    return caller


def forget_caller(email: Optional[str] = None) -> None:
    """Drop cached lookups, e.g. after a role or couple assignment changes."""
    with _cache_lock:
        if email:
            _cache.pop(email.lower(), None)
        else:
            _cache.clear()


def _int(s: str) -> Optional[int]:
    try:
        return int(s)
    except (TypeError, ValueError):
        return None


def _item_client_id(table: str, item_id: int) -> Optional[int]:
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT ClientId FROM {table} WHERE Id = ?", item_id)
        row = cur.fetchone()
        return int(row.ClientId) if row and row.ClientId is not None else None
    finally:
        conn.close()


def decide(method: str, path: str, query: dict, email: str) -> Tuple[Optional[str], Caller]:
    """Return (deny_reason or None, caller). Runs in a worker thread (DB calls)."""
    caller = get_caller(email)
    segs = [s for s in path.strip("/").split("/") if s]
    head = segs[0] if segs else ""
    rest = segs[1:]
    m = method.upper()

    if head == "auth" and rest == ["me"]:
        return None, caller                       # /auth/me answers 403 itself
    # Login looks up the signed-in email; allow it even when unknown so the
    # page can show "not authorized" (404) instead of a generic error.
    if head == "authorized-users" and m == "GET" and len(rest) == 2 and rest[0] == "email":
        if rest[1].lower() == caller.email:
            return None, caller
        return (None if caller.is_admin else "another user's record"), caller

    if not caller.known:
        return "not an authorized user", caller
    if not caller.active:
        return "inactive user or role", caller
    if caller.is_admin:
        return None, caller

    if head == "authorized-users":
        own = str(caller.user_id)
        if rest and rest[0] == own:
            if m == "GET" and len(rest) in (1, 2) and (len(rest) == 1 or rest[1] == "with-role"):
                return None, caller
            if m == "PUT" and (len(rest) == 1 or rest[1:] == ["last-login"]):
                return None, caller               # endpoint limits which fields
            return "admin only", caller
        if m == "POST" and not rest and caller.is_therapist:
            return None, caller                   # endpoint limits the RoleID
        return "admin only", caller

    if head in CONFIG_READ_OPEN:
        return (None if m == "GET" else "admin only"), caller

    if head == "therapists":
        if not rest:
            return (None if m == "GET" and caller.is_therapist else "admin only"), caller
        tid = _int(rest[0])
        if m == "GET" and tid in caller.therapist_ids:
            return None, caller
        return "not your therapist record", caller

    if head == "clients":
        if not rest:
            if m == "POST" and caller.is_therapist:
                return None, caller               # endpoint checks TherapistID
            return "admin only", caller
        cid = _int(rest[0])
        if cid is None or cid not in caller.client_ids:
            return "not your client", caller
        if m == "DELETE":
            return "admin only", caller
        if m == "PUT" and len(rest) == 1 and not caller.is_therapist:
            return "therapist only", caller       # a client can't edit the couple record
        return None, caller

    if head in CLIENT_OWNED_TABLES:
        if not rest:
            if m == "POST":
                return None, caller               # endpoint checks body ClientId
            if m == "GET":
                cid = _int(query.get("client_id"))
                if cid is not None:
                    return (None if cid in caller.client_ids else "not your client"), caller
                if head == "session-dates" and caller.client_ids:
                    return None, caller           # endpoint filters to caller.client_ids
                return "client_id required", caller
            return "admin only", caller
        item = _int(rest[0])
        if item is None:
            return "not found", caller
        owner = _item_client_id(CLIENT_OWNED_TABLES[head], item)
        if owner is None:
            return None, caller                   # let the endpoint answer 404
        if owner not in caller.client_ids:
            return "not your client", caller
        if m == "DELETE" and not caller.is_therapist:
            return "therapist only", caller
        return None, caller

    return "admin only", caller


# ---------------------------------------------------------------------------
# Endpoint helpers (for checks that need the request body)
# ---------------------------------------------------------------------------

def _mode() -> str:
    from api_auth import AUTH_MODE
    return AUTH_MODE


def caller_of(request: Request) -> Optional[Caller]:
    return getattr(request.state, "caller", None)


def is_admin(request: Request) -> bool:
    c = caller_of(request)
    if c is None:
        return _mode() != "enforce"   # report/off: behave as before
    return c.is_admin


def require_body_access(request: Request, *, client_id: Optional[int] = None,
                        therapist_id: Optional[int] = None, reason: str = "") -> None:
    """Deny (enforce) or log (report) when the body names a couple or therapist
    the caller can't reach. Admins always pass."""
    mode = _mode()
    if mode == "off":
        return
    c = caller_of(request)
    ok = c is not None and (c.is_admin or (
        (client_id is None or client_id in c.client_ids) and
        (therapist_id is None or therapist_id in c.therapist_ids)))
    if ok:
        return
    deny(request, reason or "body names a client or therapist outside the caller's scope")


def deny(request: Request, reason: str) -> None:
    mode = _mode()
    log.warning("authz=deny mode=%s %s %s reason=%s", mode, request.method, request.url.path, reason)
    if mode == "enforce":
        raise HTTPException(status_code=403, detail="You don't have access to this.")


def visible_client_ids(request: Request) -> Optional[Set[int]]:
    """Client ids to filter list results to, or None for no filtering."""
    c = caller_of(request)
    if _mode() != "enforce" or c is None or c.is_admin:
        return None
    return set(c.client_ids)
