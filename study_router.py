"""
study_router.py: API endpoints for the UCANRR feasibility-study pages
(annotate.html, adjudicate.html, coordinator.html, monitor.html).

Add to ucanrr1_api_with_roles_azure.py, after `app` is created:

    from study_router import router as study_router
    app.include_router(study_router)

Security model
- Every request must carry a Google ID token (Authorization: Bearer <token>).
  The token is verified here against STUDY_GOOGLE_CLIENT_ID (falling back
  to GOOGLE_CLIENT_ID): signature, expiry, audience and a verified email. The page's localStorage is never trusted.
- The verified email is looked up in AuthorizedUsers + Roles. Both the user
  and the role must be active.
- Study data is reached through two dedicated database logins, never the
  server admin:
    STUDY_APP_CONNECTION_STRING    member of study_app (annotation procedures)
    STUDY_COORD_CONNECTION_STRING  member of study_coordinator
  AZURE_SQL_CONNECTION_STRING (the existing one) is used only to look up
  AuthorizedUsers, Roles, Client and Therapist.
- Each request opens its own connection and sets SESSION_CONTEXT
  'actor_user_id' (and 'annotator_id' for annotators) read-only, so
  row-level security and the audit log see the signed-in person.

Required App Service settings
    STUDY_GOOGLE_CLIENT_ID (the web client ID in study.js; if unset,
    GOOGLE_CLIENT_ID is used), STUDY_APP_CONNECTION_STRING,
    STUDY_COORD_CONNECTION_STRING
"""
from __future__ import annotations

import hashlib
import logging
import os
import re
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Iterator, List, Literal, Optional

import pyodbc
from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response
from pydantic import BaseModel, Field, field_validator

log = logging.getLogger("ucanrr.study")

# ----------------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------------

def _env(name: str) -> str:
    return (os.environ.get(name) or "").strip()

# The study pages sign in with their own web client, so GOOGLE_CLIENT_ID can stay
# set for the mobile app.
GOOGLE_CLIENT_ID = _env("STUDY_GOOGLE_CLIENT_ID") or _env("GOOGLE_CLIENT_ID")
MAIN_CS = _env("AZURE_SQL_CONNECTION_STRING")
STUDY_APP_CS = _env("STUDY_APP_CONNECTION_STRING")
STUDY_COORD_CS = _env("STUDY_COORD_CONNECTION_STRING")

# Roles table names -> annotator type in study.Annotator
ANNOTATOR_ROLES: Dict[str, str] = {
    "Study Therapist": "TREATING_THERAPIST",
    "Study Labeler": "SECOND_LABELER",
    "Study Adjudicator": "ADJUDICATOR",
}
COORDINATOR_ROLES = {"Study Coordinator"}
MONITOR_ROLES = {"Study Monitor", "Study Coordinator"}
STUDY_ROLES = set(ANNOTATOR_ROLES) | COORDINATOR_ROLES | MONITOR_ROLES

FLAGS = ("IsNeglect", "IsRepair", "IsBid", "IsCare", "IsShared")


def _no_store(response: Response) -> None:
    # Entry text must not sit in browser or proxy caches.
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"


router = APIRouter(prefix="/study", tags=["study"], dependencies=[Depends(_no_store)])


# ----------------------------------------------------------------------------
# Database helpers
# ----------------------------------------------------------------------------

def _connect(conn_str: str, name: str) -> pyodbc.Connection:
    if not conn_str:
        log.error("Study API: %s is not configured", name)
        raise HTTPException(503, "Study service is not configured.")
    # Autocommit: the stored procedures manage their own transactions, and
    # read procedures also write audit rows that must persist.
    return pyodbc.connect(conn_str, autocommit=True)


def _jsonable(v: Any) -> Any:
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, (bytes, bytearray)):
        return v.hex()
    return v


def _rows(cur: pyodbc.Cursor) -> List[Dict[str, Any]]:
    """All rows of the current result set, or [] when the statement returned none."""
    if cur.description is None:
        return []
    cols = [c[0] for c in cur.description]
    return [{c: _jsonable(v) for c, v in zip(cols, r)} for r in cur.fetchall()]


def _set_ctx(cur: pyodbc.Cursor, key: str, value: Any) -> None:
    cur.execute("EXEC sp_set_session_context @key = ?, @value = ?, @read_only = 1", key, value)


_SQL_ERR = re.compile(r"\((\d{3,6})\)\s*\(SQL")
_HTTP_FOR_SQL = {
    51001: 403, 51002: 404, 51003: 400, 51004: 400, 51005: 403,
    51006: 409, 51007: 400, 51008: 403, 2601: 409, 2627: 409, 547: 400,
}


def _raise_sql(e: pyodbc.Error) -> None:
    msg = str(e.args[1] if len(e.args) > 1 else e)
    m = _SQL_ERR.search(msg)
    num = int(m.group(1)) if m else 0
    if num in _HTTP_FOR_SQL:
        if 51000 <= num < 52000:
            # Our own THROW messages are written for users.
            text = msg.split("[SQL Server]")[-1]
            text = re.sub(r"\s*\(\d{3,6}\)\s*\(SQL\w*\)\s*$", "", text).strip()
        elif num in (2601, 2627):
            text = "That record already exists."
        else:
            text = "The request conflicts with existing study data."
        raise HTTPException(_HTTP_FOR_SQL[num], text)
    log.exception("Study API database error")
    raise HTTPException(500, "Database error.")


@contextmanager
def _sql() -> Iterator[None]:
    try:
        yield
    except pyodbc.Error as e:
        _raise_sql(e)


# ----------------------------------------------------------------------------
# Authentication: verified Google ID token -> AuthorizedUsers row
# ----------------------------------------------------------------------------

_token_cache: Dict[str, Dict[str, Any]] = {}
_token_lock = threading.Lock()
_google_request = None


def _google_req():
    global _google_request
    if _google_request is None:
        import requests
        from google.auth.transport import requests as g_requests
        session = requests.Session()
        try:  # cache Google's signing certs (cachecontrol ships with firebase-admin)
            from cachecontrol import CacheControl
            session = CacheControl(session)
        except ImportError:
            pass
        _google_request = g_requests.Request(session=session)
    return _google_request


def verify_google_token(token: str) -> Dict[str, Any]:
    """Return verified claims or raise 401. Results are cached until expiry."""
    if not GOOGLE_CLIENT_ID:
        log.error("Study API: STUDY_GOOGLE_CLIENT_ID / GOOGLE_CLIENT_ID is not configured")
        raise HTTPException(503, "Sign-in is not configured.")
    key = hashlib.sha256(token.encode()).hexdigest()
    now = time.time()
    with _token_lock:
        hit = _token_cache.get(key)
        if hit and hit["exp"] > now + 5:
            return hit
    from google.auth import exceptions as g_exc
    from google.oauth2 import id_token
    try:
        claims = id_token.verify_oauth2_token(token, _google_req(), GOOGLE_CLIENT_ID)
    except g_exc.TransportError:
        log.exception("Study API: could not fetch Google signing certificates")
        raise HTTPException(503, "Couldn't reach Google to verify your sign-in. Try again shortly.")
    except (ValueError, g_exc.GoogleAuthError):
        raise HTTPException(401, "Sign-in expired or invalid. Please sign in again.")
    if claims.get("iss") not in ("accounts.google.com", "https://accounts.google.com"):
        raise HTTPException(401, "Invalid token issuer.")
    if not claims.get("email") or not claims.get("email_verified"):
        raise HTTPException(401, "Google account email is not verified.")
    with _token_lock:
        if len(_token_cache) > 2000:
            for k in [k for k, v in _token_cache.items() if v["exp"] <= now]:
                _token_cache.pop(k, None)
        _token_cache[key] = claims
    return claims


@dataclass
class Principal:
    user_id: int
    email: str
    name: str
    role: str


def _lookup_user(email: str) -> Optional[Dict[str, Any]]:
    conn = _connect(MAIN_CS, "AZURE_SQL_CONNECTION_STRING")
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT au.UserID, au.Email, au.FullName, au.IsActive AS UserActive, "
            "       r.RoleName, r.IsActive AS RoleActive "
            "FROM AuthorizedUsers au JOIN Roles r ON r.RoleID = au.RoleID "
            "WHERE LOWER(au.Email) = ?", email.lower())
        rows = _rows(cur)
        return rows[0] if rows else None
    finally:
        conn.close()


def get_principal(authorization: str = Header(default="")) -> Principal:
    if not authorization.lower().startswith("bearer "):
        raise HTTPException(401, "Please sign in.")
    claims = verify_google_token(authorization[7:].strip())
    with _sql():
        user = _lookup_user(claims["email"])
    if not user:
        raise HTTPException(403, "This Google account is not authorized for UCANRR.")
    if not user["UserActive"]:
        raise HTTPException(403, "Your account is inactive.")
    role = (user["RoleName"] or "").strip()
    if role not in STUDY_ROLES:
        raise HTTPException(403, "Your role does not include access to study pages.")
    if not user["RoleActive"]:
        raise HTTPException(403, "This study role is not active yet.")
    return Principal(int(user["UserID"]), user["Email"], (user["FullName"] or user["Email"]).strip(), role)


# ----------------------------------------------------------------------------
# Sessions per role
# ----------------------------------------------------------------------------

@dataclass
class AnnotatorSession:
    conn: pyodbc.Connection
    principal: Principal
    annotator_id: int
    annotator_type: str
    can_view_restricted: bool

    def cursor(self) -> pyodbc.Cursor:
        return self.conn.cursor()


def annotator_session(p: Principal = Depends(get_principal)) -> Iterator[AnnotatorSession]:
    if p.role not in ANNOTATOR_ROLES:
        raise HTTPException(403, "This page is for study annotators.")
    conn = _connect(STUDY_APP_CS, "STUDY_APP_CONNECTION_STRING")
    try:
        with _sql():
            cur = conn.cursor()
            _set_ctx(cur, "actor_user_id", str(p.user_id))
            cur.execute("EXEC study.usp_Resolve_Annotator @Source_User_ID = ?", str(p.user_id))
            row = (_rows(cur) or [{}])[0]
        if row.get("Annotator_ID") is None:
            raise HTTPException(403, "No active study annotator record or signed privacy agreement "
                                     "for your account. Contact the study coordinator.")
        if row["Annotator_Type"] != ANNOTATOR_ROLES[p.role]:
            raise HTTPException(403, "Your study role and annotator registration don't match. "
                                     "Contact the study coordinator.")
        with _sql():
            _set_ctx(conn.cursor(), "annotator_id", int(row["Annotator_ID"]))
        yield AnnotatorSession(conn, p, int(row["Annotator_ID"]), row["Annotator_Type"],
                               bool(row["Can_View_Restricted"]))
    finally:
        conn.close()


def adjudicator_session(s: AnnotatorSession = Depends(annotator_session)) -> AnnotatorSession:
    if s.annotator_type != "ADJUDICATOR":
        raise HTTPException(403, "This page is for the study adjudicator.")
    return s


@dataclass
class StaffSession:
    conn: pyodbc.Connection
    principal: Principal

    def cursor(self) -> pyodbc.Cursor:
        return self.conn.cursor()


def _staff_session(p: Principal, allowed: set) -> Iterator[StaffSession]:
    if p.role not in allowed:
        raise HTTPException(403, "Your role does not include this page.")
    conn = _connect(STUDY_COORD_CS, "STUDY_COORD_CONNECTION_STRING")
    try:
        with _sql():
            _set_ctx(conn.cursor(), "actor_user_id", str(p.user_id))
        yield StaffSession(conn, p)
    finally:
        conn.close()


def coordinator_session(p: Principal = Depends(get_principal)) -> Iterator[StaffSession]:
    yield from _staff_session(p, COORDINATOR_ROLES)


def monitor_session(p: Principal = Depends(get_principal)) -> Iterator[StaffSession]:
    # Monitor endpoints only ever read the label-only views; no entry text.
    yield from _staff_session(p, MONITOR_ROLES)


# ----------------------------------------------------------------------------
# Who am I
# ----------------------------------------------------------------------------

@router.get("/me")
def me(p: Principal = Depends(get_principal)) -> Dict[str, Any]:
    out: Dict[str, Any] = {"userId": p.user_id, "email": p.email, "name": p.name, "role": p.role}
    if p.role in ANNOTATOR_ROLES:
        # Resolve now so the page can show a clear message before loading data.
        gen = annotator_session(p)
        s = next(gen)
        try:
            out.update(annotatorId=s.annotator_id, annotatorType=s.annotator_type,
                       canViewRestricted=s.can_view_restricted)
        finally:
            gen.close()
    return out


# ----------------------------------------------------------------------------
# Annotation (Study Therapist, Study Labeler, Study Adjudicator)
# ----------------------------------------------------------------------------

class LabelIn(BaseModel):
    IsNeglect: bool
    IsRepair: bool
    IsBid: bool
    IsCare: bool
    IsShared: bool
    Sentiment: int = Field(ge=-10, le=10)
    Safety_Tier: int = Field(ge=1, le=5)


class AnnotationIn(LabelIn):
    Notes: Optional[str] = Field(default=None, max_length=2000)

    @field_validator("Notes")
    @classmethod
    def _blank_to_none(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        return v.strip() or None


@router.get("/queue")
def queue(include_done: bool = False, s: AnnotatorSession = Depends(annotator_session)):
    with _sql():
        cur = s.cursor()
        cur.execute("EXEC study.usp_Get_Annotation_Queue @Include_Done = ?", int(include_done))
        return _rows(cur)


@router.get("/entries/{entry_id}")
def entry(entry_id: int, s: AnnotatorSession = Depends(annotator_session)):
    with _sql():
        cur = s.cursor()
        cur.execute("EXEC study.usp_Get_Entry @Study_Entry_ID = ?", entry_id)
        rows = _rows(cur)
        if not rows:
            raise HTTPException(404, "Entry not found or not assigned to you.")
        cur.execute("EXEC study.usp_Get_Entry_Annotations @Study_Entry_ID = ?", entry_id)
        anns = _rows(cur)
        gold = None
        if s.annotator_type == "ADJUDICATOR":
            cur.execute("EXEC study.usp_Get_Gold_Label @Study_Entry_ID = ?", entry_id)
            g = _rows(cur)
            gold = g[0] if g else None
    mine = next((a for a in anns if a["Annotator_ID"] == s.annotator_id), None)
    others = [a for a in anns if a["Annotator_ID"] != s.annotator_id]
    return {"entry": rows[0], "myAnnotation": mine,
            "otherAnnotations": others if s.annotator_type == "ADJUDICATOR" else [],
            "gold": gold}


@router.post("/entries/{entry_id}/annotations")
def save_annotation(entry_id: int, body: AnnotationIn, s: AnnotatorSession = Depends(annotator_session)):
    if s.annotator_type == "ADJUDICATOR":
        raise HTTPException(403, "The adjudicator records gold labels, not annotations.")
    with _sql():
        cur = s.cursor()
        cur.execute(
            "EXEC study.usp_Save_Annotation @Study_Entry_ID=?, @IsNeglect=?, @IsRepair=?, @IsBid=?, "
            "@IsCare=?, @IsShared=?, @Sentiment=?, @Safety_Tier=?, @Notes=?",
            entry_id, *(int(getattr(body, f)) for f in FLAGS), body.Sentiment, body.Safety_Tier, body.Notes)
        rows = _rows(cur)
    return {"versionNo": rows[0]["Version_No"] if rows else None}


# ----------------------------------------------------------------------------
# Adjudication (Study Adjudicator)
# ----------------------------------------------------------------------------

@router.get("/adjudication/queue")
def adjudication_queue(include_done: bool = False, s: AnnotatorSession = Depends(adjudicator_session)):
    with _sql():
        cur = s.cursor()
        cur.execute("EXEC study.usp_Get_Adjudication_Queue @Include_Done = ?", int(include_done))
        return _rows(cur)


def _same_labels(a: Dict[str, Any], b: LabelIn) -> bool:
    return all(bool(a[f]) == getattr(b, f) for f in FLAGS) \
        and int(a["Sentiment"]) == b.Sentiment and int(a["Safety_Tier"]) == b.Safety_Tier


@router.post("/entries/{entry_id}/gold")
def save_gold(entry_id: int, body: LabelIn, s: AnnotatorSession = Depends(adjudicator_session)):
    with _sql():
        cur = s.cursor()
        cur.execute("EXEC study.usp_Get_Entry_Annotations @Study_Entry_ID = ?", entry_id)
        anns = [a for a in _rows(cur) if a["Annotator_ID"] != s.annotator_id]
        # Method is decided here, not by the page: AGREEMENT only when the gold
        # label matches every current annotation exactly.
        method = "AGREEMENT" if anns and all(_same_labels(a, body) for a in anns) else "ADJUDICATED"
        cur.execute(
            "EXEC study.usp_Save_Gold_Label @Study_Entry_ID=?, @Method=?, @IsNeglect=?, @IsRepair=?, "
            "@IsBid=?, @IsCare=?, @IsShared=?, @Sentiment=?, @Safety_Tier=?",
            entry_id, method, *(int(getattr(body, f)) for f in FLAGS), body.Sentiment, body.Safety_Tier)
    return {"method": method}


# ----------------------------------------------------------------------------
# Coordinator (Study Coordinator)
# ----------------------------------------------------------------------------

def _main_rows(sql: str, *params: Any) -> List[Dict[str, Any]]:
    conn = _connect(MAIN_CS, "AZURE_SQL_CONNECTION_STRING")
    try:
        cur = conn.cursor()
        cur.execute(sql, *params)
        return _rows(cur)
    finally:
        conn.close()


def _strip(v: Any) -> Any:
    return v.strip() if isinstance(v, str) else v


def _study_users() -> List[Dict[str, Any]]:
    placeholders = ",".join("?" * len(STUDY_ROLES))
    rows = _main_rows(
        "SELECT au.UserID, au.Email, au.FullName, au.IsActive, r.RoleName "
        f"FROM AuthorizedUsers au JOIN Roles r ON r.RoleID = au.RoleID WHERE r.RoleName IN ({placeholders}) "
        "ORDER BY au.FullName, au.Email", *sorted(STUDY_ROLES))
    return [{k: _strip(v) for k, v in r.items()} for r in rows]


@router.get("/coord/overview")
def coord_overview(s: StaffSession = Depends(coordinator_session)):
    with _sql():
        cur = s.cursor()
        cur.execute(
            "SELECT Agreement_ID, Agreement_Version, Signer_Name, Signed_At, Countersigned_By, Countersigned_At, "
            "CONVERT(VARCHAR(64), Signed_File_SHA256, 2) AS Sha256, Signed_File_Location, Expires_At, "
            "Revoked_At, Revoked_Reason FROM study.Privacy_Agreement ORDER BY Agreement_ID DESC")
        agreements = _rows(cur)
        cur.execute(
            "SELECT a.Annotator_ID, a.Source_User_ID, a.Annotator_Type, a.Agreement_ID, a.Can_View_Restricted, "
            "a.Is_Active, pa.Signer_Name, pa.Expires_At AS Agreement_Expires_At, "
            "pa.Revoked_At AS Agreement_Revoked_At FROM study.Annotator a "
            "JOIN study.Privacy_Agreement pa ON pa.Agreement_ID = a.Agreement_ID ORDER BY a.Annotator_ID")
        annotators = _rows(cur)
        cur.execute(
            "SELECT sp.Study_Participant_ID, pl.Source_Client_ID, sp.Consent_Version, sp.Research_Consent_At, "
            "sp.Screening_Instrument, sp.Partner_Consent_Status, sp.Research_Paused_At, sp.Pause_Reason, "
            "sp.Withdrawn_At, sp.Created_At, ISNULL(p.Entries,0) AS Entries, "
            "ISNULL(p.Restricted_Entries,0) AS Restricted_Entries, ISNULL(p.Labeled_Once,0) AS Labeled_Once, "
            "ISNULL(p.Labeled_Twice,0) AS Labeled_Twice, ISNULL(p.Gold_Final,0) AS Gold_Final "
            "FROM study.Study_Participant sp "
            "LEFT JOIN study.Participant_Link pl ON pl.Study_Participant_ID = sp.Study_Participant_ID "
            "LEFT JOIN study.vw_Annotation_Progress p ON p.Study_Participant_ID = sp.Study_Participant_ID "
            "ORDER BY sp.Study_Participant_ID")
        participants = _rows(cur)
        cur.execute(
            "SELECT Assignment_ID, Annotator_ID, Study_Participant_ID, Assigned_At "
            "FROM study.Annotator_Assignment WHERE Revoked_At IS NULL ORDER BY Study_Participant_ID, Annotator_ID")
        assignments = _rows(cur)
        users = _study_users()
        clients = [{k: _strip(v) for k, v in r.items()} for r in _main_rows(
            "SELECT c.Id, c.TherapistID, c.Client1FirstName, c.Client2FirstName, t.TherapistUserName "
            "FROM Client c LEFT JOIN Therapist t ON t.Id = c.TherapistID ORDER BY c.Id")]
    by_user = {str(u["UserID"]): u for u in users}
    for a in annotators:
        u = by_user.get(str(a["Source_User_ID"]), {})
        a["FullName"], a["Email"], a["RoleName"] = u.get("FullName"), u.get("Email"), u.get("RoleName")
    return {"agreements": agreements, "annotators": annotators, "participants": participants,
            "assignments": assignments, "studyUsers": users, "clients": clients}


class AgreementIn(BaseModel):
    Agreement_Version: str = Field(min_length=1, max_length=20)
    Signer_Name: str = Field(min_length=1, max_length=200)
    Signed_At: datetime
    Countersigned_By: str = Field(min_length=1, max_length=200)
    Countersigned_At: datetime
    Signed_File_SHA256: str = Field(pattern=r"^[0-9a-fA-F]{64}$")
    Signed_File_Location: str = Field(min_length=1, max_length=400)
    Expires_At: Optional[datetime] = None


def _utc_naive(d: Optional[datetime]) -> Optional[datetime]:
    if d is None:
        return None
    if d.tzinfo is not None:
        d = d.astimezone(timezone.utc).replace(tzinfo=None)
    return d.replace(microsecond=0)


@router.post("/coord/agreements")
def coord_register_agreement(body: AgreementIn, s: StaffSession = Depends(coordinator_session)):
    with _sql():
        cur = s.cursor()
        cur.execute(
            "EXEC study.usp_Register_Agreement @Agreement_Version=?, @Signer_Name=?, @Signed_At=?, "
            "@Countersigned_By=?, @Countersigned_At=?, @Signed_File_SHA256=?, @Signed_File_Location=?, @Expires_At=?",
            body.Agreement_Version, body.Signer_Name, _utc_naive(body.Signed_At), body.Countersigned_By,
            _utc_naive(body.Countersigned_At), bytes.fromhex(body.Signed_File_SHA256), body.Signed_File_Location,
            _utc_naive(body.Expires_At))
        return _rows(cur)[0]


class ReasonIn(BaseModel):
    Reason: str = Field(min_length=1, max_length=400)


@router.post("/coord/agreements/{agreement_id}/revoke")
def coord_revoke_agreement(agreement_id: int, body: ReasonIn, s: StaffSession = Depends(coordinator_session)):
    with _sql():
        s.cursor().execute("EXEC study.usp_Revoke_Agreement @Agreement_ID=?, @Reason=?", agreement_id, body.Reason)
    return {"ok": True}


class AnnotatorIn(BaseModel):
    UserID: int
    Agreement_ID: int
    Can_View_Restricted: bool = False


@router.post("/coord/annotators")
def coord_register_annotator(body: AnnotatorIn, s: StaffSession = Depends(coordinator_session)):
    with _sql():
        user = next((u for u in _study_users() if int(u["UserID"]) == body.UserID), None)
    if not user or user["RoleName"] not in ANNOTATOR_ROLES:
        raise HTTPException(400, "That user doesn't hold Study Therapist, Study Labeler or Study Adjudicator.")
    with _sql():
        cur = s.cursor()
        cur.execute(
            "EXEC study.usp_Register_Annotator @Source_User_ID=?, @Annotator_Type=?, @Agreement_ID=?, "
            "@Can_View_Restricted=?",
            str(body.UserID), ANNOTATOR_ROLES[user["RoleName"]], body.Agreement_ID, int(body.Can_View_Restricted))
        return _rows(cur)[0]


class EnrollIn(BaseModel):
    Client_ID: int
    Partner: Literal[1, 2]
    Consent_Version: str = Field(min_length=1, max_length=20)
    Research_Consent_At: datetime
    Screening_Instrument: str = Field(min_length=1, max_length=50)
    Screening_Passed_At: datetime
    Partner_Consent_Status: Literal["BOTH_CONSENTED", "SINGLE"]
    Assign_Treating_Therapist: bool = True


@router.post("/coord/participants")
def coord_enroll(body: EnrollIn, s: StaffSession = Depends(coordinator_session)):
    # One participant per consenting person: "<Client.Id>-<partner>", so a
    # SINGLE consent never pulls in the other partner's entries.
    source_client_id = f"{body.Client_ID}-{body.Partner}"
    with _sql():
        clients = _main_rows(
            "SELECT c.Id, t.TherapistUserName FROM Client c LEFT JOIN Therapist t ON t.Id = c.TherapistID "
            "WHERE c.Id = ?", body.Client_ID)
    if not clients:
        raise HTTPException(404, "Client not found.")
    with _sql():
        cur = s.cursor()
        cur.execute(
            "EXEC study.usp_Enroll_Participant @Source_Client_ID=?, @Consent_Version=?, @Research_Consent_At=?, "
            "@Screening_Instrument=?, @Screening_Passed_At=?, @Partner_Consent_Status=?",
            source_client_id, body.Consent_Version, _utc_naive(body.Research_Consent_At),
            body.Screening_Instrument, _utc_naive(body.Screening_Passed_At), body.Partner_Consent_Status)
        pid = int(_rows(cur)[0]["Study_Participant_ID"])

        assigned_annotator = None
        note = None
        if body.Assign_Treating_Therapist:
            email = _strip(clients[0].get("TherapistUserName") or "")
            user = next((u for u in _study_users()
                         if (u["Email"] or "").lower() == email.lower() and u["RoleName"] == "Study Therapist"), None)
            if not user:
                note = "The client's therapist doesn't hold the Study Therapist role, so no one was assigned."
            else:
                cur.execute("SELECT Annotator_ID FROM study.Annotator WHERE Source_User_ID = ? AND Is_Active = 1 "
                            "AND Annotator_Type = 'TREATING_THERAPIST'", str(user["UserID"]))
                hit = _rows(cur)
                if not hit:
                    note = "The client's therapist isn't registered as a study annotator yet, so no one was assigned."
                else:
                    assigned_annotator = int(hit[0]["Annotator_ID"])
                    cur.execute("EXEC study.usp_Assign_Annotator @Annotator_ID=?, @Study_Participant_ID=?",
                                assigned_annotator, pid)
    return {"Study_Participant_ID": pid, "Source_Client_ID": source_client_id,
            "assignedAnnotatorId": assigned_annotator, "note": note}


class AssignmentIn(BaseModel):
    Annotator_ID: int
    Study_Participant_ID: int


@router.post("/coord/assignments")
def coord_assign(body: AssignmentIn, s: StaffSession = Depends(coordinator_session)):
    with _sql():
        s.cursor().execute("EXEC study.usp_Assign_Annotator @Annotator_ID=?, @Study_Participant_ID=?",
                           body.Annotator_ID, body.Study_Participant_ID)
    return {"ok": True}


@router.post("/coord/assignments/revoke")
def coord_revoke_assignment(body: AssignmentIn, s: StaffSession = Depends(coordinator_session)):
    with _sql():
        s.cursor().execute("EXEC study.usp_Revoke_Assignment @Annotator_ID=?, @Study_Participant_ID=?",
                           body.Annotator_ID, body.Study_Participant_ID)
    return {"ok": True}


class PauseIn(BaseModel):
    Reason: Literal["SAFETY_ESCALATION", "PARTICIPANT_REQUEST", "OTHER"]


@router.post("/coord/participants/{pid}/pause")
def coord_pause(pid: int, body: PauseIn, s: StaffSession = Depends(coordinator_session)):
    with _sql():
        s.cursor().execute("EXEC study.usp_Pause_Participant @Study_Participant_ID=?, @Reason=?", pid, body.Reason)
    return {"ok": True}


@router.post("/coord/participants/{pid}/resume")
def coord_resume(pid: int, s: StaffSession = Depends(coordinator_session)):
    with _sql():
        s.cursor().execute("EXEC study.usp_Resume_Participant @Study_Participant_ID=?", pid)
    return {"ok": True}


class WithdrawIn(BaseModel):
    Confirm_Participant_ID: int
    Delete_Linkage: bool = True


@router.post("/coord/participants/{pid}/withdraw")
def coord_withdraw(pid: int, body: WithdrawIn, s: StaffSession = Depends(coordinator_session)):
    if body.Confirm_Participant_ID != pid:
        raise HTTPException(400, "Confirmation ID doesn't match the participant.")
    with _sql():
        s.cursor().execute("EXEC study.usp_Withdraw_Participant @Study_Participant_ID=?, @Delete_Linkage=?",
                           pid, int(body.Delete_Linkage))
    return {"ok": True}


@router.post("/coord/entries/{entry_id}/restrict")
def coord_restrict(entry_id: int, s: StaffSession = Depends(coordinator_session)):
    with _sql():
        cur = s.cursor()
        cur.execute("EXEC study.usp_Restrict_Entry @Study_Entry_ID=?", entry_id)
        cur.execute("SELECT Is_Restricted FROM study.Study_Entry WHERE Study_Entry_ID = ?", entry_id)
        rows = _rows(cur)
    if not rows:
        raise HTTPException(404, "Entry not found.")
    return {"ok": True}


# ----------------------------------------------------------------------------
# Monitor (Study Monitor, Study Coordinator): labels and counts only
# ----------------------------------------------------------------------------

def cohen_kappa(a: List[Any], b: List[Any]) -> Optional[float]:
    n = len(a)
    if n == 0:
        return None
    cats = set(a) | set(b)
    po = sum(1 for x, y in zip(a, b) if x == y) / n
    pe = sum((a.count(c) / n) * (b.count(c) / n) for c in cats)
    if pe >= 1.0:
        return None  # both raters used one identical category; kappa undefined
    return round((po - pe) / (1 - pe), 3)


def agreement_stats(pairs: List[Dict[str, Any]]) -> Dict[str, Any]:
    n = len(pairs)
    out: Dict[str, Any] = {"pairs": n}
    if n == 0:
        return out
    t_tier = [int(p["T_Safety_Tier"]) for p in pairs]
    l_tier = [int(p["L_Safety_Tier"]) for p in pairs]
    out["tier"] = {
        "exactAgreement": round(sum(x == y for x, y in zip(t_tier, l_tier)) / n, 3),
        "kappa": cohen_kappa(t_tier, l_tier),
        # Safety-first: disagreements that cross the Crisis line (tier 4).
        "crisisLineDisagreements": sum((x >= 4) != (y >= 4) for x, y in zip(t_tier, l_tier)),
    }
    out["flags"] = {}
    for f in FLAGS:
        t = [bool(p["T_" + f]) for p in pairs]
        l = [bool(p["L_" + f]) for p in pairs]
        out["flags"][f] = {"agreement": round(sum(x == y for x, y in zip(t, l)) / n, 3),
                           "kappa": cohen_kappa(t, l)}
    diffs = [abs(int(p["T_Sentiment"]) - int(p["L_Sentiment"])) for p in pairs]
    out["sentiment"] = {"meanAbsDiff": round(sum(diffs) / n, 2),
                        "within2": round(sum(d <= 2 for d in diffs) / n, 3)}
    return out


@router.get("/monitor/summary")
def monitor_summary(days: int = Query(30, ge=1, le=365), s: StaffSession = Depends(monitor_session)):
    with _sql():
        cur = s.cursor()
        cur.execute(
            "SELECT sp.Study_Participant_ID, sp.Partner_Consent_Status, sp.Research_Paused_At, sp.Withdrawn_At, "
            "sp.Created_At, ISNULL(p.Entries,0) AS Entries, ISNULL(p.Restricted_Entries,0) AS Restricted_Entries, "
            "ISNULL(p.Labeled_Once,0) AS Labeled_Once, ISNULL(p.Labeled_Twice,0) AS Labeled_Twice, "
            "ISNULL(p.Gold_Final,0) AS Gold_Final FROM study.Study_Participant sp "
            "LEFT JOIN study.vw_Annotation_Progress p ON p.Study_Participant_ID = sp.Study_Participant_ID "
            "ORDER BY sp.Study_Participant_ID")
        progress = _rows(cur)
        cur.execute("SELECT * FROM study.vw_Label_Pairs")
        pairs = _rows(cur)
        cur.execute(
            "SELECT Event_Date, Annotator_ID, Actor_User_ID, Entries_Viewed, Annotations_Saved, Denied_Events, "
            "Participants_Touched FROM study.vw_Audit_Daily "
            "WHERE Event_Date >= DATEADD(DAY, -?, CAST(SYSUTCDATETIME() AS DATE)) "
            "AND (Entries_Viewed > 0 OR Annotations_Saved > 0 OR Denied_Events > 0) "
            "ORDER BY Event_Date DESC, Denied_Events DESC", days)
        audit = _rows(cur)
        names = {str(u["UserID"]): (u["FullName"] or u["Email"]) for u in _study_users()}
    for a in audit:
        a["Actor_Name"] = names.get(str(a["Actor_User_ID"])) if a["Actor_User_ID"] else None

    active = [p for p in progress if not p["Withdrawn_At"]]
    totals = {
        "participantsEnrolled": len(progress),
        "participantsActive": sum(1 for p in active if not p["Research_Paused_At"]),
        "participantsPaused": sum(1 for p in active if p["Research_Paused_At"]),
        "participantsWithdrawn": len(progress) - len(active),
    }
    for k in ("Entries", "Restricted_Entries", "Labeled_Once", "Labeled_Twice", "Gold_Final"):
        totals[k[0].lower() + k[1:].replace("_", "")] = sum(int(p[k]) for p in progress)
    return {"totals": totals, "progress": progress, "agreement": agreement_stats(pairs),
            "audit": audit, "auditDays": days}
