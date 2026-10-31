"""
UCANRR feasibility study: consistent surrogate substitution.

Takes raw journal text plus detected identifier spans (from Presidio / Azure AI
Language PII / LLM pass / reviewer) and produces:
  * de-identified text with realistic, per-participant-consistent surrogates
  * span records (offsets into the RAW text)     -> research.Deid_Span
  * new surrogate map rows (HMAC only, never raw) -> linkage.Surrogate_Map

Design choices
  * Mapping key = HMAC-SHA256(secret, participant_id | entity_type | normalized value).
    The secret lives in Key Vault; names are low-entropy, so a plain hash would be
    reversible by dictionary attack. A keyed hash is not.
  * Surrogates are scoped per participant: "Mark" -> "Jordan" in every one of that
    participant's entries, preserving who-did-what-to-whom for Bid/Repair/Shared.
  * PERSON surrogates default to gender-neutral names so they don't contradict
    pronouns in the text ("Sarah ... she" -> "Casey ... she" still reads naturally).
  * Dates are shifted by the participant's secret offset, not removed, so intervals
    between entries survive for longitudinal analysis.
  * Known limit: variants of one person ("Mike" / "Michael", "my husband Mark" vs
    "M.") are only unified if the detector or reviewer supplies a canonical form.
"""
from __future__ import annotations

import hashlib
import hmac
import re
import unicodedata
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Callable, Optional, Protocol

# --------------------------------------------------------------------------- pools
PERSON_POOL = [
    "Jordan", "Casey", "Riley", "Avery", "Quinn", "Morgan", "Taylor", "Rowan",
    "Emerson", "Parker", "Reese", "Sawyer", "Hayden", "Skyler", "Dakota", "Finley",
    "Harper", "Kendall", "Logan", "Peyton", "Remy", "Sage", "Blake", "Cameron",
]
LOCATION_POOL = ["Riverside", "Fairview", "Oak Hill", "Lakeview", "Maple Grove",
                 "Cedar Falls", "Brookfield", "Westfield", "Pine Ridge", "Ashford"]
ORG_POOL = ["Northgate Group", "Summit Partners", "Harbor Health", "Bluestone Co.",
            "Meridian Services", "Crescent Labs", "Keystone Works", "Evergreen LLC"]
GENERIC = {
    "EMAIL": "user@example.com",
    "PHONE": "555-0100",
    "URL": "https://example.com",
    "SSN": "[ID]",
    "ID": "[ID]",
}
POOLS = {"PERSON": PERSON_POOL, "LOCATION": LOCATION_POOL, "ORG": ORG_POOL}


# --------------------------------------------------------------------------- types
@dataclass(frozen=True)
class DetectedSpan:
    start: int
    end: int
    entity_type: str                 # PERSON, LOCATION, ORG, DATE, EMAIL, PHONE, ...
    detector: str                    # PRESIDIO | AZURE_PII | LLM | REVIEWER
    confidence: Optional[float] = None
    role_label: Optional[str] = None # PARTNER_1, CHILD_1 ... when known
    canonical: Optional[str] = None  # reviewer-supplied canonical form ("Michael" for "Mike")
    action: str = "SURROGATE"        # SURROGATE | GENERALIZE | KEEP
    generalized_text: Optional[str] = None  # used when action == GENERALIZE


@dataclass
class SurrogateRow:                  # -> linkage.Surrogate_Map
    study_participant_id: str
    entity_type: str
    original_hmac: bytes
    hmac_key_version: int
    role_label: Optional[str]
    surrogate_value: str


@dataclass
class SpanRow:                       # -> research.Deid_Span
    start_offset: int
    end_offset: int
    entity_type: str
    detector: str
    confidence: Optional[float]
    span_action: str
    replacement_text: Optional[str]
    original_hmac: Optional[bytes] = None  # resolve to Surrogate_Map_ID after upsert


@dataclass
class DeidResult:
    text: str
    spans: list[SpanRow] = field(default_factory=list)
    new_surrogates: list[SurrogateRow] = field(default_factory=list)


class SurrogateStore(Protocol):
    """Backed by linkage.Surrogate_Map in production."""
    def get(self, participant: str, entity_type: str, key_version: int, h: bytes) -> Optional[str]: ...
    def used_values(self, participant: str, entity_type: str) -> set[str]: ...
    def put(self, row: SurrogateRow) -> None: ...


class InMemoryStore:
    def __init__(self) -> None:
        self._m: dict[tuple, SurrogateRow] = {}

    def get(self, participant, entity_type, key_version, h):
        row = self._m.get((participant, entity_type, key_version, h))
        return row.surrogate_value if row else None

    def used_values(self, participant, entity_type):
        return {r.surrogate_value for (p, t, _, _), r in self._m.items()
                if p == participant and t == entity_type}

    def put(self, row):
        self._m[(row.study_participant_id, row.entity_type, row.hmac_key_version,
                 row.original_hmac)] = row


# --------------------------------------------------------------------------- helpers
_POSSESSIVE = re.compile(r"(?:'s|’s|')$")
_WS = re.compile(r"\s+")


def normalize(value: str, entity_type: str) -> str:
    """Canonical form used for matching. 'MARK', 'Mark ', "Mark's" -> 'mark'."""
    v = unicodedata.normalize("NFKC", value).strip()
    v = _WS.sub(" ", v)
    if entity_type in ("PERSON", "ORG", "LOCATION"):
        v = _POSSESSIVE.sub("", v)
    if entity_type == "PHONE":
        v = re.sub(r"\D", "", v)
    return v.casefold()


def keyed_hash(secret: bytes, participant: str, entity_type: str, normalized: str) -> bytes:
    msg = f"{participant}\x1f{entity_type}\x1f{normalized}".encode("utf-8")
    return hmac.new(secret, msg, hashlib.sha256).digest()


def _pick_surrogate(pool: list[str], h: bytes, used: set[str]) -> str:
    """Deterministic start point from the HMAC, then linear probe past used values."""
    start = int.from_bytes(h[:4], "big") % len(pool)
    for i in range(len(pool)):
        cand = pool[(start + i) % len(pool)]
        if cand not in used:
            return cand
    # pool exhausted for this participant: suffix to stay unique
    n = 2
    while f"{pool[start]} {n}" in used:
        n += 1
    return f"{pool[start]} {n}"


_DATE_FORMATS = ["%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%B %d, %Y", "%b %d, %Y", "%B %d", "%b %d"]


def shift_date_text(text: str, offset_days: int, reference_year: int) -> Optional[str]:
    """Shift an explicit date string; returns None if unparseable (caller generalizes)."""
    t = text.strip().rstrip(",.")
    for fmt in _DATE_FORMATS:
        try:
            d = datetime.strptime(t, fmt)
        except ValueError:
            continue
        has_year = "%Y" in fmt or "%y" in fmt
        if not has_year:
            d = d.replace(year=reference_year)
        shifted = (d + timedelta(days=offset_days)).date()
        return _format_like(shifted, fmt)
    return None


def _format_like(d: date, fmt: str) -> str:
    """Render in the source format; avoids platform-specific %-d."""
    month_full, month_abbr = d.strftime("%B"), d.strftime("%b")
    if fmt == "%B %d, %Y":
        return f"{month_full} {d.day}, {d.year}"
    if fmt == "%b %d, %Y":
        return f"{month_abbr} {d.day}, {d.year}"
    if fmt == "%B %d":
        return f"{month_full} {d.day}"
    if fmt == "%b %d":
        return f"{month_abbr} {d.day}"
    return d.strftime(fmt)


def resolve_overlaps(spans: list[DetectedSpan]) -> list[DetectedSpan]:
    """Keep the widest span when detectors overlap; REVIEWER spans always win."""
    rank = {"REVIEWER": 3, "LLM": 2, "AZURE_PII": 1, "PRESIDIO": 1}
    ordered = sorted(spans, key=lambda s: (-rank.get(s.detector, 0), -(s.end - s.start), s.start))
    kept: list[DetectedSpan] = []
    for s in ordered:
        if all(s.end <= k.start or s.start >= k.end for k in kept):
            kept.append(s)
    return sorted(kept, key=lambda s: s.start)


# --------------------------------------------------------------------------- main
def deidentify(
    raw_text: str,
    spans: list[DetectedSpan],
    participant_id: str,
    secret: bytes,
    key_version: int,
    date_offset_days: int,
    reference_year: int,
    store: SurrogateStore,
) -> DeidResult:
    result = DeidResult(text="")
    out: list[str] = []
    cursor = 0

    for s in resolve_overlaps(spans):
        original = raw_text[s.start:s.end]
        out.append(raw_text[cursor:s.start])
        cursor = s.end
        etype = s.entity_type.upper()

        if s.action == "KEEP":
            out.append(original)
            result.spans.append(SpanRow(s.start, s.end, etype, s.detector, s.confidence, "KEEP", None))
            continue

        if s.action == "GENERALIZE":
            repl = s.generalized_text or f"[{etype}]"
            out.append(repl)
            result.spans.append(SpanRow(s.start, s.end, etype, s.detector, s.confidence, "GENERALIZE", repl))
            continue

        if etype == "DATE":
            repl = shift_date_text(original, date_offset_days, reference_year)
            action = "DATE_SHIFT"
            if repl is None:                      # relative/unparseable ("last Easter") -> generalize
                repl, action = "[DATE]", "GENERALIZE"
            out.append(repl)
            result.spans.append(SpanRow(s.start, s.end, etype, s.detector, s.confidence, action, repl))
            continue

        norm = normalize(s.canonical or original, etype)
        h = keyed_hash(secret, participant_id, etype, norm)
        surrogate = store.get(participant_id, etype, key_version, h)
        if surrogate is None:
            pool = POOLS.get(etype)
            if pool:
                surrogate = _pick_surrogate(pool, h, store.used_values(participant_id, etype))
            else:
                surrogate = GENERIC.get(etype, f"[{etype}]")
            row = SurrogateRow(participant_id, etype, h, key_version, s.role_label, surrogate)
            store.put(row)
            result.new_surrogates.append(row)

        # keep a possessive the detector included: "Mark's" -> "Jordan's"
        m = _POSSESSIVE.search(original.strip()) if etype in POOLS else None
        rendered = surrogate + (m.group(0) if m else "")
        if original.isupper() and len(original) > 1:   # preserve SHOUTING ("MARK!!")
            rendered = rendered.upper()
        out.append(rendered)
        result.spans.append(SpanRow(s.start, s.end, etype, s.detector, s.confidence,
                                    "SURROGATE", rendered, original_hmac=h))

    out.append(raw_text[cursor:])
    result.text = "".join(out)
    return result
