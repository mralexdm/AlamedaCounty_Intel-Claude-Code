#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MADM Motivated-Seller Lead Scraper — Alameda County, California
==================================================================

Pulls newly recorded "distress" documents (lis pendens, foreclosure notices,
judgments, tax / mechanic liens, probate, etc.) from the Alameda County
Clerk-Recorder public portal, enriches each record with parcel data
(situs + mailing address) from the County ArcGIS parcel layer, scores each
record as a motivated-seller lead, and writes:

    dashboard/records.json   (for the static dashboard / GitHub Pages)
    data/records.json        (canonical data copy)
    dashboard/leads_ghl.csv  (Go High Level import)
    data/leads_ghl.csv

Ground truth (verified against the LIVE endpoints, June 2026)
------------------------------------------------------------
Clerk portal:  "Aumentum Recorder - Public Access Web UI" (Harris), behind an
F5/Shape "TSPD" anti-bot JavaScript challenge.

  * Challenge: the first navigation returns an obfuscated JS interstitial that
    fingerprints the browser (incl. `navigator.webdriver`). A vanilla Playwright
    browser fails it and gets a near-empty "support id" page. We pass it with a
    stealth context (webdriver masked, realistic UA/locale/timezone) + a settle
    /reload loop. THEN the disclaimer renders.
  * Flow:  SearchEntry.aspx?e=newSession  -> [challenge] -> disclaimer page
           -> click "acknowledge the disclaimer and enter the site"
           -> SearchEntry.aspx  (the real, anonymous search form).
  * Search form control ids:
        Party Name      cphNoMargin_f_txtGrantor
        Party Type      cphNoMargin_f_drbPartyType_0/1/2
        Date Filed From cphNoMargin_f_ddcDateFiledFrom  (Infragistics date picker
                        -> fill the inner <input>, then Tab; format mm/dd/yyyy)
        Date Filed To   cphNoMargin_f_ddcDateFiledTo
        Instrument #    cphNoMargin_f_txtInstrumentNoFrom / ...To
        Book / Page     cphNoMargin_f_txtBook / cphNoMargin_f_txtp1
        Doc-Type list   cphNoMargin_f_dclDocType  (a 280-item CheckBoxList)
        Search button   cphNoMargin_SearchButtons1_btnSearch
  * Results grid: table id "Table1", an Infragistics WebDataGrid with a fixed
    35-column schema per record (incl. machine cols GLOBAL_ID, NO_GRANTORS, ...).
    Useful columns: "Inst num" (doc #), "Book"/"Page", "Date Filed",
    "Document Type", "Name" (grantor/owner), "Associated Name" (grantee), "City".
    Document detail is POSTBACK-only (no stable GET url) — and the grid carries
    NO legal description / APN / street address. Pager: "<N> records found" +
    a "Next" image button (id contains "imgNext").

Parcel layer (ArcGIS FeatureServer 0):
  https://services5.arcgis.com/ROBnTHSNjoZ2Wm1P/arcgis/rest/services/Parcels/FeatureServer/0
  * maxRecordCount 2000, supportsPagination true.
  * Fields: APN, SitusStreetNumber/Name/Unit/City/Zip, SitusAddress,
    MailingAddressStreet/Unit/CityState/Zip, MailingAddress, TotalNetValue,
    LatestDocumentDate, BOOK, PAGE, PARCEL.
  * IMPORTANT: the parcel layer exposes NO owner-name field, and the clerk
    results carry no APN/address. So automatic owner->parcel enrichment is only
    possible when an APN/situs address is otherwise available (e.g. a future
    detail-page fetch, or a bulk Assessor roll wired into
    `ArcGISEnricher.find_parcels_by_owner` / `load_owner_index_from_dbf`).
    The owner name-variant logic (FIRST LAST / LAST FIRST / LAST, FIRST) is built
    and ready for that. Expect a portion of leads to have no enriched address
    until such a source is added; `with_address` reports the coverage per run.

Design notes
------------
* Resilient: real control ids are used as the primary selector with generic
  fallbacks, so the scraper survives portal skin updates.
* Category -> live label mapping: each lead type carries alias keywords matched
  (case-insensitive substring) against the live Document-Type checklist labels,
  plus an exclude list so broad categories don't swallow specific ones.
* Never crashes on a bad record. Every stage is tagged so a failure is logged as
  CHALLENGE / DISCLAIMER / SEARCH_FORM / RESULTS / PARSING / ENRICHMENT / EXPORT.
* Retry logic (3 attempts, exponential backoff) on every network operation.

Usage
-----
    python src/fetch.py                      # full run, headless
    python src/fetch.py --headed             # watch the browser
    python src/fetch.py --lookback-days 14   # widen the window
    python src/fetch.py --cats LP,NOFC       # only these categories
    python src/fetch.py --no-enrich          # skip ArcGIS enrichment
    python src/fetch.py --grouped            # one combined search

Dependencies: see src/requirements.txt  (run `playwright install chromium`).
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import os
import re
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

try:
    import requests
except Exception:  # pragma: no cover
    requests = None  # type: ignore

try:
    from bs4 import BeautifulSoup
except Exception:  # pragma: no cover
    BeautifulSoup = None  # type: ignore

# Playwright is imported inside the async scraper so that pure-data operations
# (scoring, exports, enrichment) run without a browser installed.


# ============================================================================ #
#  Configuration                                                               #
# ============================================================================ #

CLERK_BASE = "https://rechart1.acgov.org/RealEstate/"
CLERK_SEARCH_ENTRY = CLERK_BASE + "SearchEntry.aspx?e=newSession"  # forces gate
CLERK_SEARCH_FORM = CLERK_BASE + "SearchEntry.aspx"               # real form

ARCGIS_PARCEL_LAYER = (
    "https://services5.arcgis.com/ROBnTHSNjoZ2Wm1P/arcgis/rest/services/"
    "Parcels/FeatureServer/0"
)
ARCGIS_QUERY_URL = ARCGIS_PARCEL_LAYER + "/query"
ARCGIS_PAGE_SIZE = 2000  # == layer maxRecordCount

DEFAULT_LOOKBACK_DAYS = int(os.environ.get("LOOKBACK_DAYS", "7"))
RETRY_ATTEMPTS = 3
RETRY_BACKOFF = 2.0       # seconds, multiplied each attempt
HTTP_TIMEOUT = 45         # seconds for requests
NAV_TIMEOUT_MS = 45_000   # Playwright navigation/action timeout
RESULT_CAP = 300          # Aumentum caps a single search at ~300 rows (oldest-first);
                          # when hit, we re-run that category day-by-day for completeness.

SOURCE_LABEL = "Alameda County Clerk-Recorder (Aumentum Public Access) + ArcGIS Parcels"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# Stealth: defeats the F5/Shape fingerprint that otherwise blocks the portal.
LAUNCH_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--no-sandbox",
    "--disable-dev-shm-usage",
]
STEALTH_JS = (
    "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
    "Object.defineProperty(navigator,'languages',{get:()=>['en-US','en']});"
    "Object.defineProperty(navigator,'plugins',{get:()=>[1,2,3,4,5]});"
    "window.chrome={runtime:{}};"
    "const _q=window.navigator.permissions&&window.navigator.permissions.query;"
    "if(_q){window.navigator.permissions.query=(p)=>"
    "(p&&p.name==='notifications')?Promise.resolve({state:Notification.permission}):_q(p);}"
)

# Real search-form selectors (primary; generic fallbacks live in the code).
SEL_DATE_FROM = "#cphNoMargin_f_ddcDateFiledFrom"
SEL_DATE_TO = "#cphNoMargin_f_ddcDateFiledTo"
SEL_DOCTYPE_CONTAINER = "#cphNoMargin_f_dclDocType"
SEL_PARTY_NAME = "#cphNoMargin_f_txtGrantor"
SEL_SEARCH_BTN = "#cphNoMargin_SearchButtons1_btnSearch"
SEL_RESULTS_TABLE = "#Table1"

# Repo root = parent of the scraper/ directory that holds this file.
ROOT = Path(__file__).resolve().parents[1]
OUT_DASHBOARD = ROOT / "dashboard"
OUT_DATA = ROOT / "data"


class Stage:
    CHALLENGE = "CHALLENGE"
    DISCLAIMER = "DISCLAIMER"
    SEARCH_FORM = "SEARCH_FORM"
    RESULTS = "RESULTS"
    PARSING = "PARSING"
    ENRICHMENT = "ENRICHMENT"
    EXPORT = "EXPORT"
    NAV = "NAVIGATION"
    INIT = "INIT"


# ============================================================================ #
#  Lead-type category model                                                     #
#                                                                              #
#  Aliases/excludes are matched (case-insensitive substring) against the LIVE   #
#  Alameda Document-Type checklist labels (verified against the 280-item list). #
#  Some requested types (Tax Deed, Certified Judgment, Medicaid, HOA, Release   #
#  Lis Pendens) are NOT recorded under those names in Alameda and will match    #
#  zero live labels — that is logged, not faked.                                #
# ============================================================================ #

@dataclass(frozen=True)
class Category:
    code: str
    label: str
    aliases: Tuple[str, ...]
    flags: Tuple[str, ...] = ()
    exclude: Tuple[str, ...] = ()


CATEGORIES: Tuple[Category, ...] = (
    Category(
        "LP", "Lis Pendens",
        aliases=("notice action", "lis pendens"),
        flags=("Lis pendens",),
        exclude=("release", "withdrawal", "expungement", "cancel"),
    ),
    Category(
        "NOFC", "Notice of Foreclosure",
        aliases=("notice default", "notice of default", "notice of trustee sale",
                 "trustee sale", "order foreclosure", "substitution of trustee",
                 "sub of trustee", "default certification", "request notice default"),
        flags=("Pre-foreclosure",),
        exclude=("cancel", "release", "reconveyance", "redemption"),
    ),
    Category(
        "TAXDEED", "Tax Deed",
        aliases=("tax deed", "tax collector deed", "treasurer deed"),
        flags=("Tax lien",),
    ),
    Category(
        "JUD", "Judgment",
        aliases=("abstract of judgment", "judgment"),
        flags=("Judgment lien",),
        exclude=("release", "partial", "subordination", "certified",
                 "dissolution", "separation", "nullity"),
    ),
    Category(
        "CCJ", "Certified Judgment",
        aliases=("certified judgment", "certified copy of judgment"),
        flags=("Judgment lien",),
        exclude=("release", "satisfaction"),
    ),
    Category(
        "DRJUD", "Domestic Relations Judgment",
        aliases=("judgment dissolution", "dissolution marriage",
                 "legal separation", "judgment nullity", "decree divorce",
                 "separate maintenance", "decree dissolution"),
        flags=("Judgment lien",),
        exclude=("release", "partial"),
    ),
    Category(
        "LNCORPTX", "Tax Lien (State / County / City)",
        aliases=("tax lien (state)", "tax lien or extension (state)",
                 "tax lien (county)", "tax lien (city)",
                 "tax lien (others)", "tax lien (other)"),
        flags=("Tax lien",),
        exclude=("release", "partial", "cancel"),
    ),
    Category(
        "LNIRS", "IRS / Federal Tax Lien",
        aliases=("tax lien (fed)", "federal tax lien"),
        flags=("Tax lien",),
        exclude=("release", "partial"),
    ),
    Category(
        "LNFED", "Federal Lien",
        aliases=("notice of federal interest", "federal interest"),
        flags=("Tax lien",),
        exclude=("release", "partial"),
    ),
    Category(
        "LN", "Lien (general)",
        aliases=("notice lien", "lien agreement", "attachment"),
        flags=(),
        exclude=("mechanic", "mechanics", "tax", "federal", "release",
                 "partial", "subordination", "order", "non attachment",
                 "cancel", "bond"),
    ),
    Category(
        "LNMECH", "Mechanic's Lien",
        aliases=("mechanics lien", "mechanic"),
        flags=("Mechanic lien",),
        exclude=("release", "partial", "cancel"),
    ),
    Category(
        "LNHOA", "HOA Lien",
        aliases=("assessment lien", "hoa", "homeowner association",
                 "homeowners association", "association lien"),
        flags=(),
        exclude=("release", "partial"),
    ),
    Category(
        "MEDLN", "Medicaid / Medi-Cal Lien",
        aliases=("medicaid lien", "medi-cal lien", "dhcs lien"),
        flags=(),
        exclude=("release", "partial"),
    ),
    Category(
        "PRO", "Probate / Estate",
        aliases=("affidavit of death", "decree distribution",
                 "decree assigning estate", "letters testamentary",
                 "letters administ", "letters conservator", "letters guardian",
                 "transfer on death", "death deed", "decree terminating interest"),
        flags=("Probate / estate",),
        exclude=("release", "terminating guardianshi"),
    ),
    Category(
        "NOC", "Notice of Commencement / Completion",
        aliases=("notice completion", "notice commencement", "notice cessation"),
        flags=(),
        exclude=("cancel",),
    ),
    Category(
        "RELLP", "Release of Lis Pendens",
        aliases=("release of lis pendens", "withdrawal of lis pendens",
                 "release lis pendens", "withdrawal of notice of action",
                 "release of notice of action"),
        # A release/withdrawal CANCELS the lis pendens — it is NOT an active
        # distress signal, so it must not carry "Lis pendens" (which would inflate
        # the score and falsely trigger the LP+FC combo on the owner).
        flags=(),
    ),
)

CATEGORY_BY_CODE: Dict[str, Category] = {c.code: c for c in CATEGORIES}


# ============================================================================ #
#  Logging                                                                      #
# ============================================================================ #

logger = logging.getLogger("madm")


def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s",
                          datefmt="%H:%M:%S")
    )
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.setLevel(level)
    logger.propagate = False


def log_stage(stage: str, msg: str, level: int = logging.INFO) -> None:
    logger.log(level, "[%s] %s", stage, msg)


def log_failure(stage: str, msg: str, exc: Optional[BaseException] = None) -> None:
    detail = f"{msg}: {exc}" if exc else msg
    logger.error("[%s] FAILURE - %s", stage, detail)
    if exc is not None:
        logger.debug("[%s] traceback:\n%s", stage, traceback.format_exc())


# ============================================================================ #
#  Retry helpers                                                                #
# ============================================================================ #

def retry_sync(fn: Callable, *args, attempts: int = RETRY_ATTEMPTS,
               stage: str = Stage.NAV, what: str = "operation", **kwargs):
    """Run a synchronous callable with retries + exponential backoff."""
    last: Optional[BaseException] = None
    for i in range(1, attempts + 1):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001
            last = exc
            wait = RETRY_BACKOFF * i
            log_stage(stage, f"{what} attempt {i}/{attempts} failed ({exc}); "
                             f"retrying in {wait:.0f}s", logging.WARNING)
            if i < attempts:
                time.sleep(wait)
    log_failure(stage, f"{what} exhausted {attempts} attempts", last)
    raise last  # type: ignore[misc]


async def retry_async(coro_factory: Callable, *, attempts: int = RETRY_ATTEMPTS,
                      stage: str = Stage.NAV, what: str = "operation"):
    """Run an async callable factory (returns a coroutine) with retries."""
    last: Optional[BaseException] = None
    for i in range(1, attempts + 1):
        try:
            return await coro_factory()
        except Exception as exc:  # noqa: BLE001
            last = exc
            wait = RETRY_BACKOFF * i
            log_stage(stage, f"{what} attempt {i}/{attempts} failed ({exc}); "
                             f"retrying in {wait:.0f}s", logging.WARNING)
            if i < attempts:
                await asyncio.sleep(wait)
    log_failure(stage, f"{what} exhausted {attempts} attempts", last)
    raise last  # type: ignore[misc]


# ============================================================================ #
#  Date helpers                                                                 #
# ============================================================================ #

def date_range(lookback_days: int) -> Tuple[datetime, datetime]:
    today = datetime.now()
    start = today - timedelta(days=max(0, lookback_days))
    return start, today


def fmt_mdy(d: datetime) -> str:
    """MM/DD/YYYY — the format Aumentum date fields expect."""
    return d.strftime("%m/%d/%Y")


def parse_any_date(text: str) -> Optional[datetime]:
    if not text:
        return None
    text = text.strip()
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%m-%d-%Y",
                "%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    m = re.search(r"(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})", text)
    if m:
        mm, dd, yy = m.groups()
        yy = ("20" + yy) if len(yy) == 2 else yy
        try:
            return datetime(int(yy), int(mm), int(dd))
        except ValueError:
            return None
    return None


def epoch_ms_to_iso(ms: Optional[float]) -> Optional[str]:
    if ms is None:
        return None
    try:
        return datetime.fromtimestamp(float(ms) / 1000.0, tz=timezone.utc).date().isoformat()
    except (ValueError, OverflowError, OSError):
        return None


# ============================================================================ #
#  Text / owner / address parsing helpers                                       #
# ============================================================================ #

ENTITY_PATTERNS = re.compile(
    r"\b("
    r"LLC|L\.L\.C|INC|INCORPORATED|CORP|CORPORATION|COMPANY|CO\b|LP|L\.P|LLP|"
    r"LTD|TRUST|TR\b|ESTATE|BANK|N\.A|ASSN|ASSOCIATION|PARTNERS|PARTNERSHIP|"
    r"HOLDINGS|PROPERTIES|PROPERTY|INVESTMENT|INVESTMENTS|FUND|GROUP|CAPITAL|"
    r"ENTERPRISES|VENTURES|REALTY|HOMES|DEVELOPMENT|MANAGEMENT|SERVICES|"
    r"FOUNDATION|CHURCH|CITY OF|COUNTY OF|STATE OF|USA|DEPT|DEPARTMENT|MFG"
    r")\b",
    re.IGNORECASE,
)

# Alameda APN, e.g. "15-1338-24", "048-6770-008-00", "001-0001-001"
APN_RE = re.compile(r"\b(\d{1,3}-\d{3,4}-\d{1,3}(?:-\d{1,3})?)\b")
APN_LABELLED_RE = re.compile(
    r"(?:A\.?P\.?N\.?|ASSESSOR'?S?\s+PARCEL(?:\s+(?:NO|NUMBER))?)[:\s#]*"
    r"(\d[\d\- ]{4,})",
    re.IGNORECASE,
)
AMOUNT_RE = re.compile(r"\$\s?([\d,]+(?:\.\d{2})?)")
STATE_RE = re.compile(r"\b([A-Z]{2})\b\s*$")


# Strict business-entity tokens for the "LLC / corp owner" flag. Deliberately
# EXCLUDES TR/TRUST/ESTATE: those are overwhelmingly individuals holding title in
# a living trust or a decedent's estate (rampant in CA), NOT corporate owners.
CORP_PATTERNS = re.compile(
    r"\b("
    r"LLC|L\.L\.C|INC|INCORPORATED|CORP|CORPORATION|COMPANY|CO|LP|L\.P|LLP|LTD|"
    r"BANK|N\.A|ASSN|ASSOCIATION|PARTNERS|PARTNERSHIP|HOLDINGS|PROPERTIES|"
    r"PROPERTY|INVESTMENT|INVESTMENTS|FUND|GROUP|CAPITAL|ENTERPRISES|VENTURES|"
    r"REALTY|HOMES|DEVELOPMENT|MANAGEMENT|SERVICES|FOUNDATION|MFG|CHURCH|"
    r"CITY OF|COUNTY OF|STATE OF|USA|DEPT|DEPARTMENT"
    r")\b",
    re.IGNORECASE,
)


def is_entity(owner: str) -> bool:
    """Broad: any non-individual (incl. trusts/estates). Used for name splitting."""
    return bool(owner) and bool(ENTITY_PATTERNS.search(owner))


def is_corp_entity(owner: str) -> bool:
    """Strict: a true business/government entity (the 'LLC / corp owner' flag)."""
    return bool(owner) and bool(CORP_PATTERNS.search(owner))


def clean_ws(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def owner_name_variants(name: str) -> List[str]:
    """
    Build normalized owner-name lookup variants for matching against an
    owner-indexed dataset (Assessor secured roll, title plant, etc.).

    Produces (de-duplicated, upper-cased): "FIRST LAST", "LAST FIRST",
    "LAST, FIRST". Entities are returned as-is (single variant).
    """
    name = clean_ws(name).upper()
    if not name:
        return []
    if is_entity(name):
        return [name]

    if "," in name:  # already "LAST, FIRST [MIDDLE]"
        last, _, rest = name.partition(",")
        last, rest = clean_ws(last), clean_ws(rest)
        return _dedupe([f"{last}, {rest}".strip(", "),
                        f"{last} {rest}".strip(),
                        f"{rest} {last}".strip()])

    parts = name.split()
    if len(parts) == 1:
        return [name]
    first, last = parts[0], parts[-1]
    middle = " ".join(parts[1:-1])
    first_full = clean_ws(f"{first} {middle}")
    return _dedupe([clean_ws(f"{first_full} {last}"),
                    clean_ws(f"{last} {first_full}"),
                    clean_ws(f"{last}, {first_full}")])


def _dedupe(seq: Iterable[str]) -> List[str]:
    seen, out = set(), []
    for s in seq:
        s = s.strip()
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def extract_apns(*texts: str) -> List[str]:
    found: List[str] = []
    for t in texts:
        if not t:
            continue
        for m in APN_LABELLED_RE.finditer(t):
            found.append(re.sub(r"\s+", "", m.group(1)))
        for m in APN_RE.finditer(t):
            found.append(m.group(1))
    return _dedupe(found)


def normalize_apn(apn: str) -> List[str]:
    apn = apn.strip().upper()
    digits = re.sub(r"\D", "", apn)
    return _dedupe([apn, digits])


def extract_amount(*texts: str) -> Optional[float]:
    for t in texts:
        if not t:
            continue
        m = AMOUNT_RE.search(t)
        if m:
            try:
                return float(m.group(1).replace(",", ""))
            except ValueError:
                continue
    return None


def split_person_name(owner: str) -> Tuple[str, str]:
    """First/Last split for GHL export. Entities -> ('', full, original case)."""
    owner = clean_ws(owner)
    if not owner:
        return "", ""
    if is_entity(owner):
        return "", owner
    if "," in owner:  # LAST, FIRST
        last, _, first = owner.partition(",")
        return clean_ws(first).title(), clean_ws(last).title()
    parts = owner.split()
    if len(parts) == 1:
        return "", parts[0].title()
    # Recorder indexes as "LAST FIRST [MIDDLE]"; assume first token is the surname.
    return " ".join(parts[1:]).title(), parts[0].title()


def split_city_state(city_state: str) -> Tuple[str, str]:
    """'OAKLAND CA' -> ('OAKLAND', 'CA')."""
    cs = clean_ws(city_state)
    if not cs:
        return "", ""
    m = STATE_RE.search(cs)
    if m:
        return cs[: m.start()].strip().rstrip(","), m.group(1)
    return cs, ""


def clean_party(s: str) -> str:
    """Strip Aumentum role markers like '[R]', '[E]' and the '(+)' multi marker."""
    s = clean_ws(s)
    s = re.sub(r"\[[A-Za-z]\]", "", s)
    s = re.sub(r"\(\+\)", "", s)
    return clean_ws(s)


# ============================================================================ #
#  Record construction                                                          #
# ============================================================================ #

def blank_record() -> Dict[str, Any]:
    return {
        "doc_num": "", "doc_type": "", "filed": "", "cat": "", "cat_label": "",
        "owner": "", "grantee": "", "amount": None, "legal": "",
        "prop_address": "", "prop_city": "", "prop_state": "", "prop_zip": "",
        "mail_address": "", "mail_city": "", "mail_state": "", "mail_zip": "",
        "phone": "", "email": "",   # blank until skip-tracing fills them
        "clerk_url": "", "flags": [], "score": 0,
        # internal bookkeeping (stripped before output)
        "_cats": set(), "_apn": "", "_global_id": "",
    }


# ============================================================================ #
#  ArcGIS parcel enrichment                                                     #
# ============================================================================ #

class ArcGISEnricher:
    """Targeted parcel lookups against the Alameda County ArcGIS parcel layer."""

    def __init__(self, enabled: bool = True):
        self.enabled = enabled and requests is not None
        self.session = requests.Session() if requests is not None else None
        if self.session is not None:
            self.session.headers.update({"User-Agent": USER_AGENT})
        self._owner_field: Optional[str] = None
        self._cache: Dict[str, Optional[Dict[str, Any]]] = {}
        self.stats = {"queries": 0, "hits": 0, "misses": 0, "errors": 0}

    def _query(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        if not self.enabled or self.session is None:
            return []
        base = {"where": "1=1", "outFields": "*",
                "returnGeometry": "false", "f": "json"}
        base.update(params)

        results: List[Dict[str, Any]] = []
        offset = 0
        while True:
            page_params = dict(base)
            page_params["resultOffset"] = offset
            page_params["resultRecordCount"] = ARCGIS_PAGE_SIZE

            def _do():
                r = self.session.get(ARCGIS_QUERY_URL, params=page_params,
                                     timeout=HTTP_TIMEOUT)
                r.raise_for_status()
                return r.json()

            self.stats["queries"] += 1
            try:
                data = retry_sync(_do, stage=Stage.ENRICHMENT, what="ArcGIS query")
            except Exception as exc:  # noqa: BLE001
                self.stats["errors"] += 1
                log_failure(Stage.ENRICHMENT, "ArcGIS query failed", exc)
                break

            if not isinstance(data, dict):
                log_stage(Stage.ENRICHMENT,
                          f"ArcGIS returned non-dict JSON "
                          f"({type(data).__name__}); skipping", logging.DEBUG)
                break

            if data.get("error"):
                log_stage(Stage.ENRICHMENT, f"ArcGIS error: {data['error']}",
                          logging.DEBUG)
                break

            feats = (data or {}).get("features", []) or []
            results.extend(f.get("attributes", {}) for f in feats)

            if data.get("exceededTransferLimit") and len(feats) == ARCGIS_PAGE_SIZE:
                offset += ARCGIS_PAGE_SIZE
                continue
            break
        return results

    @staticmethod
    def _esc(value: str) -> str:
        return value.replace("'", "''")

    def find_by_apn(self, apn: str) -> Optional[Dict[str, Any]]:
        key = f"apn::{apn}"
        if key in self._cache:
            return self._cache[key]
        result: Optional[Dict[str, Any]] = None
        for variant in normalize_apn(apn):
            v = self._esc(variant)
            for where in (f"APN='{v}'", f"UPPER(APN)='{v.upper()}'",
                          f"APN_SORT='{v}'"):
                attrs = self._query({"where": where})
                if attrs:
                    result = attrs[0]
                    break
            if result:
                break
        self._cache[key] = result
        return result

    def find_by_situs(self, street_no: str, street_name: str,
                      city: str = "") -> Optional[Dict[str, Any]]:
        if not street_name:
            return None
        key = f"situs::{street_no}|{street_name}|{city}".upper()
        if key in self._cache:
            return self._cache[key]
        clauses = []
        if street_no:
            clauses.append(f"SitusStreetNumber='{self._esc(street_no)}'")
        clauses.append(
            f"UPPER(SitusStreetName) LIKE '%{self._esc(street_name).upper()}%'")
        if city:
            clauses.append(f"UPPER(SitusCity)='{self._esc(city).upper()}'")
        attrs = self._query({"where": " AND ".join(clauses)})
        result = attrs[0] if attrs else None
        self._cache[key] = result
        return result

    def find_parcels_by_owner(self, owner: str) -> List[Dict[str, Any]]:
        """
        Owner-name -> parcels. The live Alameda parcel layer exposes NO owner
        field, so this returns []. It auto-discovers a plausible owner field and
        tries every owner-name variant, so it "just works" if an owner-bearing
        field/layer is ever supplied. Degrades silently to [].
        """
        if not self.enabled:
            return []
        field_name = self._discover_owner_field()
        if not field_name:
            return []
        out: List[Dict[str, Any]] = []
        for variant in owner_name_variants(owner):
            attrs = self._query(
                {"where": f"UPPER({field_name}) LIKE '%{self._esc(variant).upper()}%'"})
            out.extend(attrs)
            if out:
                break
        return out

    def _discover_owner_field(self) -> Optional[str]:
        if self._owner_field is not None:
            return self._owner_field or None
        self._owner_field = ""
        if not self.enabled or self.session is None:
            return None
        try:
            r = self.session.get(ARCGIS_PARCEL_LAYER, params={"f": "json"},
                                 timeout=HTTP_TIMEOUT)
            r.raise_for_status()
            fields = [f.get("name", "") for f in r.json().get("fields", [])]
        except Exception:  # noqa: BLE001
            return None
        for candidate in ("OWNER", "OwnerName", "OWNER_NAME", "Owner",
                          "ASSESSEE", "AssesseeName", "OWN_NAME"):
            if candidate in fields:
                self._owner_field = candidate
                log_stage(Stage.ENRICHMENT, f"Owner field discovered: {candidate}")
                return candidate
        return None

    def enrich(self, rec: Dict[str, Any]) -> Dict[str, Any]:
        if not self.enabled:
            return rec
        parcel: Optional[Dict[str, Any]] = None

        # 1) APN from legal description / labelled text.
        for apn in extract_apns(rec.get("legal", ""), rec.get("doc_type", ""),
                                rec.get("grantee", "")):
            parcel = self.find_by_apn(apn)
            if parcel:
                rec["_apn"] = apn
                break

        # 2) Situs address if the record already carries one.
        if parcel is None and rec.get("prop_address") and rec.get("prop_state"):
            m = re.match(r"\s*(\d+)\s+(.*)", rec["prop_address"])
            if m:
                parcel = self.find_by_situs(m.group(1), m.group(2),
                                            rec.get("prop_city", ""))

        # 3) Owner-name -> parcel (no-op on the live layer; future-proofed).
        if parcel is None and rec.get("owner"):
            parcels = self.find_parcels_by_owner(rec["owner"])
            if len(parcels) == 1:
                parcel = parcels[0]

        if parcel is None:
            self.stats["misses"] += 1
            return rec
        self.stats["hits"] += 1
        self._apply_parcel(rec, parcel)
        return rec

    @staticmethod
    def _apply_parcel(rec: Dict[str, Any], p: Dict[str, Any]) -> None:
        if not rec.get("_apn") and p.get("APN"):
            rec["_apn"] = p.get("APN")

        situs_parts = [str(p.get("SitusStreetNumber") or "").strip(),
                       str(p.get("SitusStreetName") or "").strip(),
                       str(p.get("SitusUnit") or "").strip()]
        prop_addr = clean_ws(" ".join(x for x in situs_parts if x))
        if not prop_addr:
            prop_addr = clean_ws(str(p.get("SitusAddress") or ""))
        if prop_addr:
            rec["prop_address"] = prop_addr
            rec["prop_city"] = clean_ws(str(p.get("SitusCity") or "")) or rec["prop_city"]
            rec["prop_state"] = rec["prop_state"] or "CA"
            rec["prop_zip"] = clean_ws(str(p.get("SitusZip") or "")) or rec["prop_zip"]

        mail_parts = [str(p.get("MailingAddressStreet") or "").strip(),
                      str(p.get("MailingAddressUnit") or "").strip()]
        mail_addr = clean_ws(" ".join(x for x in mail_parts if x))
        if not mail_addr:
            mail_addr = clean_ws(str(p.get("MailingAddress") or ""))
        if mail_addr:
            rec["mail_address"] = mail_addr
            city, state = split_city_state(str(p.get("MailingAddressCityState") or ""))
            rec["mail_city"] = city or rec["mail_city"]
            rec["mail_state"] = state or rec["mail_state"]
            rec["mail_zip"] = clean_ws(str(p.get("MailingAddressZip") or "")) or rec["mail_zip"]


# ============================================================================ #
#  Scoring                                                                      #
# ============================================================================ #

LP_FLAG = "Lis pendens"
FC_FLAG = "Pre-foreclosure"
COMBO_FLAG = "Lis pendens + foreclosure combo"


def derive_flags(rec: Dict[str, Any], lookback_days: int,
                 today: Optional[datetime] = None) -> List[str]:
    flags: List[str] = []
    cats = rec.get("_cats") or {rec.get("cat")}
    for code in cats:
        cat = CATEGORY_BY_CODE.get(code)
        if cat:
            flags.extend(cat.flags)
    if is_corp_entity(rec.get("owner", "")):
        flags.append("LLC / corp owner")
    if _is_new_this_week(rec, lookback_days, today):
        flags.append("New this week")
    return _dedupe(flags)


def _is_new_this_week(rec: Dict[str, Any], lookback_days: int,
                      today: Optional[datetime]) -> bool:
    today = today or datetime.now()
    filed = parse_any_date(rec.get("filed", ""))
    if filed is None:
        return True  # we only ever search within the lookback window
    # "New this week" is a FIXED 7-day recency tier, independent of how wide the
    # lookback is — otherwise --lookback-days 30 would tag everything as "new".
    return (today - filed).days <= 7


def score_record(rec: Dict[str, Any], combo: bool) -> int:
    """
    Seller score (0-100):
      base 30; +10 per flag; +20 LP+FC combo (same owner);
      +15 amount > $100k (tiered, takes precedence over the $50k tier);
      +10 amount > $50k; +5 new this week; +5 has a usable address.
    """
    flags = rec.get("flags", [])
    score = 30 + 10 * len(flags)
    if combo:
        score += 20
    amount = rec.get("amount") or 0
    if amount > 100_000:
        score += 15
    elif amount > 50_000:
        score += 10
    if "New this week" in flags:
        score += 5
    if rec.get("prop_address") or rec.get("mail_address"):
        score += 5
    return max(0, min(100, score))


def score_all(records: List[Dict[str, Any]], lookback_days: int) -> None:
    today = datetime.now()
    for rec in records:
        rec["flags"] = derive_flags(rec, lookback_days, today)

    owner_flag_sets: Dict[str, set] = {}
    for rec in records:
        key = _owner_key(rec)
        if key:
            owner_flag_sets.setdefault(key, set()).update(rec["flags"])
    combo_owners = {k for k, fs in owner_flag_sets.items()
                    if LP_FLAG in fs and FC_FLAG in fs}

    # Score on the BASE flags (so the +20 combo isn't also a +10 flag), THEN
    # surface the combo as a display flag.
    for rec in records:
        combo = _owner_key(rec) in combo_owners
        rec["score"] = score_record(rec, combo)
        if combo and COMBO_FLAG not in rec["flags"]:
            rec["flags"].append(COMBO_FLAG)


def _owner_key(rec: Dict[str, Any]) -> str:
    # Combo is keyed on owner name + property/mailing ZIP when available, to
    # reduce false merges of distinct people who share a common name. With no
    # parcel link in the clerk data the ZIP is usually absent, so the match
    # degrades to name-only — which can over-merge very common names (a known
    # limitation, tightened automatically once enrichment/an owner index lands).
    owner = clean_ws(rec.get("owner", "")).upper()
    if not owner:
        return ""
    z = clean_ws(rec.get("prop_zip", "")) or clean_ws(rec.get("mail_zip", ""))
    return f"{owner}|{z}" if z else owner


# ============================================================================ #
#  Clerk portal scraper (Playwright, async)                                     #
# ============================================================================ #

# In-page JS: enumerate the document-type checklist (id + best-guess label).
JS_GATHER_DOCTYPES = r"""
() => {
  const root = document.getElementById('cphNoMargin_f_dclDocType') || document;
  const cbs = Array.from(root.querySelectorAll('input[type=checkbox]'));
  return cbs.map(cb => {
    let label = '';
    if (cb.id) { const l = document.querySelector('label[for="'+cb.id+'"]'); if (l) label = l.textContent.trim(); }
    if (!label) { const p = cb.closest('label'); if (p) label = p.textContent.trim(); }
    if (!label) { const td = cb.closest('td,li,span,div'); if (td) label = td.textContent.trim(); }
    return { id: cb.id, label: label };
  }).filter(x => x.id);
}
"""

# In-page JS: extract the Infragistics results grid (fixed 35-col schema). Data
# rows are <tr> with exactly schemaLen direct-child <td> cells.
JS_EXTRACT_RESULTS = r"""
() => {
  const grid = document.getElementById('Table1');
  if (!grid) return { err: 'no-grid' };
  const ths = Array.from(grid.querySelectorAll('th')).map(h => h.innerText.trim().replace(/\s+/g, ' '));
  if (!ths.length) return { err: 'no-headers' };
  let schemaLen = ths.length;
  for (let i = 1; i < ths.length; i++) { if (ths[i] === '#') { schemaLen = i; break; } }
  const headers = ths.slice(0, schemaLen);
  const colIndex = {};
  headers.forEach((h, i) => { if (!(h in colIndex)) colIndex[h] = i; });
  const rows = [];
  grid.querySelectorAll('tr').forEach(tr => {
    const tds = Array.from(tr.children).filter(c => c.tagName === 'TD');
    if (tds.length === schemaLen) rows.push(tds.map(td => td.innerText.trim().replace(/\s+/g, ' ')));
  });
  let total = null;
  const m = (document.body.innerText || '').match(/(\d[\d,]*)\s+records?\s+found/i);
  if (m) total = parseInt(m[1].replace(/,/g, ''), 10);
  return { schemaLen, colIndex, headers, rows, total };
}
"""


def rows_to_records(data: Dict[str, Any], primary: Optional[Category],
                    cats: Sequence[Category]) -> List[Dict[str, Any]]:
    """Convert one extracted results page into normalized records."""
    col = data.get("colIndex", {}) or {}
    rows = data.get("rows", []) or []

    # Case-insensitive header index so a portal re-skin that only changes header
    # CASING or wording (e.g. 'Inst Num', 'Date Recorded', 'Grantor') still maps.
    norm = {clean_ws(k).lower(): v for k, v in col.items()}

    def resolve(*candidates: str) -> Optional[int]:
        for c in candidates:                       # exact (normalized) match first
            if c in norm:
                return norm[c]
        for c in candidates:                       # then substring fallback
            for hk, hidx in norm.items():
                if c in hk:
                    return hidx
        return None

    idx = {
        "doc_num": resolve("inst num", "instrument # book-page", "instrument #",
                           "instrument number", "document number", "doc #"),
        "book": resolve("book"),
        "page": resolve("page"),
        "filed": resolve("date filed", "recording date", "date recorded", "recorded"),
        "doc_type": resolve("document type", "doc type", "type"),
        "owner": resolve("name", "grantor", "party 1", "first party"),
        "grantee": resolve("associated name", "grantee", "party 2", "second party"),
        "city": resolve("city"),
        "gid": resolve("global_id"),
    }
    if idx["doc_num"] is None and idx["owner"] is None:
        log_stage(Stage.PARSING,
                  "results grid headers unrecognized (doc# and name columns both "
                  "unresolved); rows may be dropped — portal skin may have changed",
                  logging.WARNING)

    def cell(row: List[str], key: str) -> str:
        i = idx.get(key)
        return clean_ws(row[i]) if (i is not None and i < len(row)) else ""

    out: List[Dict[str, Any]] = []
    for row in rows:
        try:
            rec = blank_record()
            rec["doc_num"] = cell(row, "doc_num")
            book, page_no = cell(row, "book"), cell(row, "page")
            rec["filed"] = cell(row, "filed")
            d = parse_any_date(rec["filed"])
            if d:
                rec["filed"] = fmt_mdy(d)
            rec["doc_type"] = cell(row, "doc_type")
            rec["owner"] = clean_party(cell(row, "owner"))
            rec["grantee"] = clean_party(cell(row, "grantee"))
            city = cell(row, "city")
            if city:
                rec["prop_city"] = city
                rec["prop_state"] = "CA"
            if book or page_no:
                rec["legal"] = clean_ws(f"BK {book} PG {page_no}")
            rec["_global_id"] = cell(row, "gid")
            rec["clerk_url"] = CLERK_SEARCH_ENTRY  # detail is postback-only

            if primary:
                rec["cat"], rec["cat_label"] = primary.code, primary.label
                rec["_cats"] = {primary.code}
            else:
                inferred = _infer_category(rec["doc_type"], cats)
                if inferred:
                    rec["cat"], rec["cat_label"] = inferred.code, inferred.label
                    rec["_cats"] = {inferred.code}

            if not rec["doc_num"] and not rec["owner"]:
                continue
            out.append(rec)
        except Exception as exc:  # noqa: BLE001 — never crash on a bad row
            log_stage(Stage.PARSING, f"skipped bad row ({exc})", logging.DEBUG)
    return out


class ClerkScraper:
    """Drives the Aumentum Recorder Public Access portal (with stealth)."""

    def __init__(self, headless: bool = True,
                 lookback_days: int = DEFAULT_LOOKBACK_DAYS,
                 max_result_pages: int = 25):
        self.headless = headless
        self.lookback_days = lookback_days
        self.max_result_pages = max_result_pages
        self.start, self.end = date_range(lookback_days)
        self.per_category_counts: Dict[str, int] = {}

    async def run(self, categories: Sequence[Category],
                  grouped: bool = False) -> List[Dict[str, Any]]:
        from playwright.async_api import async_playwright

        records: List[Dict[str, Any]] = []
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=self.headless, args=LAUNCH_ARGS)
            context = await browser.new_context(
                user_agent=USER_AGENT, locale="en-US",
                timezone_id="America/Los_Angeles",
                viewport={"width": 1500, "height": 1000},
                ignore_https_errors=True,
            )
            await context.add_init_script(STEALTH_JS)
            context.set_default_timeout(NAV_TIMEOUT_MS)
            page = await context.new_page()
            try:
                if not await self._open_session(page):
                    log_failure(Stage.CHALLENGE,
                                "could not pass bot challenge / disclaimer gate")
                    return []
                if grouped:
                    records.extend(await self._run_search(page, categories, "GROUPED"))
                else:
                    for cat in categories:
                        try:
                            recs = await self._run_search(page, [cat], cat.code)
                            self.per_category_counts[cat.code] = len(recs)
                            records.extend(recs)
                        except Exception as exc:  # noqa: BLE001
                            self.per_category_counts[cat.code] = -1
                            log_failure(Stage.RESULTS,
                                        f"category {cat.code} search failed", exc)
            finally:
                await context.close()
                await browser.close()
        self._log_summary(grouped)
        return records

    def _log_summary(self, grouped: bool) -> None:
        log_stage(Stage.RESULTS, "----- search summary -----")
        if grouped:
            log_stage(Stage.RESULTS, "grouped search mode (single query)")
        for code, n in self.per_category_counts.items():
            cat = CATEGORY_BY_CODE.get(code)
            name = cat.label if cat else code
            status = "ERROR" if n < 0 else f"{n} rows"
            log_stage(Stage.RESULTS, f"  {code:<9} {name:<34} -> {status}")

    # -------------------------------------------------- session / gate ------ #
    async def _open_session(self, page) -> bool:
        for attempt in range(1, RETRY_ATTEMPTS + 1):
            try:
                await page.goto(CLERK_SEARCH_ENTRY, wait_until="domcontentloaded",
                                timeout=NAV_TIMEOUT_MS)
            except Exception as exc:  # noqa: BLE001
                log_stage(Stage.NAV, f"goto newSession failed ({exc})", logging.WARNING)
                await asyncio.sleep(RETRY_BACKOFF * attempt)
                continue
            if await self._pass_challenge(page):
                await self._handle_disclaimer(page)
                if await self._goto_form(page):
                    log_stage(Stage.INIT, "session established (challenge + disclaimer passed)")
                    return True
            log_stage(Stage.CHALLENGE,
                      f"session attempt {attempt}/{RETRY_ATTEMPTS} did not settle; retrying",
                      logging.WARNING)
            await asyncio.sleep(RETRY_BACKOFF * attempt)
        return False

    async def _pass_challenge(self, page) -> bool:
        """Wait out the F5/Shape JS interstitial; reload if it shows the error page."""
        for rnd in range(8):
            await page.wait_for_timeout(2500)
            try:
                body = await page.evaluate("document.body ? document.body.innerText : ''")
            except Exception:  # noqa: BLE001
                body = ""
            low = body.lower()
            if "acknowledge" in low or "disclaimer" in low:
                return True
            if await self._form_present(page):
                return True
            if ("something went wrong" in low or "support id" in low
                    or len(body.strip()) < 40):
                log_stage(Stage.CHALLENGE,
                          f"bot-challenge interstitial (round {rnd + 1}); reloading",
                          logging.DEBUG)
                try:
                    await page.reload(wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
                except Exception:  # noqa: BLE001
                    pass
        return await self._form_present(page)

    async def _handle_disclaimer(self, page) -> bool:
        candidates = [
            "a:has-text('acknowledge the disclaimer')",
            "a:has-text('acknowledge')",
            "[id*='lnkAccept']",
            "a:has-text('enter the site')",
            "input[type='submit'][value*='Accept' i]",
            "button:has-text('Accept')",
            "a:has-text('I Agree')",
            "a:has-text('Continue')",
        ]
        for sel in candidates:
            try:
                loc = page.locator(sel).first
                if await loc.count() == 0 or not await loc.is_visible():
                    continue
                log_stage(Stage.DISCLAIMER, f"acknowledging disclaimer ('{sel}')")

                async def _click():
                    await loc.click(timeout=NAV_TIMEOUT_MS)
                    try:
                        await page.wait_for_load_state("networkidle", timeout=NAV_TIMEOUT_MS)
                    except Exception:  # noqa: BLE001
                        pass
                await retry_async(_click, stage=Stage.DISCLAIMER,
                                  what="click disclaimer accept")
                await page.wait_for_timeout(800)
                return True
            except Exception as exc:  # noqa: BLE001
                log_stage(Stage.DISCLAIMER, f"candidate '{sel}' not usable ({exc})",
                          logging.DEBUG)
        log_stage(Stage.DISCLAIMER, "no disclaimer to acknowledge (already inside)")
        return True

    async def _goto_form(self, page) -> bool:
        try:
            await page.goto(CLERK_SEARCH_FORM, wait_until="domcontentloaded",
                            timeout=NAV_TIMEOUT_MS)
        except Exception as exc:  # noqa: BLE001
            log_stage(Stage.NAV, f"goto search form failed ({exc})", logging.WARNING)
        await page.wait_for_timeout(1200)
        await self._handle_disclaimer(page)  # in case the gate reappears
        if await self._form_present(page):
            return True
        for sel in ("a:has-text('Search Real Estate Index')",
                    "a[href$='/RealEstate/SearchEntry.aspx']",
                    "a:has-text('Real Estate')"):
            try:
                loc = page.locator(sel).first
                if await loc.count():
                    await loc.click(timeout=NAV_TIMEOUT_MS)
                    await page.wait_for_timeout(1200)
                    if await self._form_present(page):
                        return True
            except Exception:  # noqa: BLE001
                continue
        return await self._form_present(page)

    async def _form_present(self, page) -> bool:
        for sel in (SEL_DOCTYPE_CONTAINER, SEL_DATE_FROM, SEL_PARTY_NAME):
            try:
                if await page.locator(sel).count():
                    return True
            except Exception:  # noqa: BLE001
                pass
        return False

    # ---------------------------------------------------- one search -------- #
    async def _run_search(self, page, cats: Sequence[Category],
                          label: str) -> List[Dict[str, Any]]:
        """Search the full window; if the portal cap is hit, chunk by day."""
        recs, total = await self._search_window(page, cats, label,
                                                self.start, self.end)
        window_days = (self.end.date() - self.start.date()).days
        if total is not None and total >= RESULT_CAP and window_days >= 1:
            log_stage(Stage.RESULTS,
                      f"{label}: portal cap (~{RESULT_CAP}) hit ({total}); "
                      f"re-running day-by-day for completeness", logging.WARNING)
            chunked: List[Dict[str, Any]] = []
            cur = self.start
            while cur.date() <= self.end.date():
                d_recs, d_total = await self._search_window(
                    page, cats, f"{label}@{cur:%m/%d}", cur, cur)
                if d_total is not None and d_total >= RESULT_CAP:
                    log_stage(Stage.RESULTS,
                              f"{label}@{cur:%m/%d}: STILL at cap ({d_total}); "
                              f"some records for this day may be missed",
                              logging.WARNING)
                chunked.extend(d_recs)
                cur = cur + timedelta(days=1)
            # Return the UNION of the (capped) full-window rows and the per-day
            # rows; global dedup merges overlaps. Never discard the day-chunked
            # rows on a raw-count comparison — a day whose chunk run transiently
            # failed is still backfilled by the capped full-window recs, and the
            # newest filings the cap dropped are recovered from the chunks.
            return recs + chunked
        return recs

    async def _search_window(self, page, cats: Sequence[Category], label: str,
                             start: datetime, end: datetime
                             ) -> Tuple[List[Dict[str, Any]], Optional[int]]:
        """One search over an explicit [start, end] window. Returns (recs, total)."""
        if not await self._goto_form(page):
            log_failure(Stage.SEARCH_FORM, f"{label}: search form unavailable")
            return [], None

        ok_from = await self._set_date(page, SEL_DATE_FROM, fmt_mdy(start))
        ok_to = await self._set_date(page, SEL_DATE_TO, fmt_mdy(end))
        log_stage(Stage.SEARCH_FORM,
                  f"{label}: date {fmt_mdy(start)} -> {fmt_mdy(end)} "
                  f"(from={'ok' if ok_from else 'MISS'}, to={'ok' if ok_to else 'MISS'})")

        matched = await self._select_doc_types(page, cats)
        if not matched:
            log_stage(Stage.SEARCH_FORM,
                      f"{label}: no live doc-type labels matched "
                      f"{[c.code for c in cats]} (Alameda may not record these "
                      f"under those names) - skipping", logging.INFO)
            return [], None
        log_stage(Stage.SEARCH_FORM,
                  f"{label}: selected {len(matched)} doc type(s): {matched}")

        if not await self._submit(page):
            log_failure(Stage.RESULTS, f"{label}: search submit failed")
            return [], None

        primary = cats[0] if len(cats) == 1 else None
        return await self._collect(page, primary, cats, label)

    async def _set_date(self, page, container_sel: str, value: str) -> bool:
        # Infragistics WebDatePicker: the container is a <table>; fill the inner input.
        for sel in (f"{container_sel} input:not([type=hidden])",
                    f"{container_sel}_input",
                    f"input[id*='{container_sel[1:]}']:not([type=hidden])"):
            try:
                loc = page.locator(sel).first
                if await loc.count() == 0:
                    continue
                await loc.click(timeout=5_000)
                await loc.fill("")
                await loc.type(value, delay=15)
                await loc.press("Tab")
                return True
            except Exception as exc:  # noqa: BLE001
                log_stage(Stage.SEARCH_FORM, f"date fill '{sel}' failed ({exc})",
                          logging.DEBUG)
        # last resort: by visible label
        try:
            loc = page.get_by_label(re.compile("date filed", re.IGNORECASE)).first
            if await loc.count():
                await loc.fill(value)
                return True
        except Exception:  # noqa: BLE001
            pass
        return False

    async def _select_doc_types(self, page, cats: Sequence[Category]) -> List[str]:
        try:
            meta = await page.evaluate(JS_GATHER_DOCTYPES)
        except Exception as exc:  # noqa: BLE001
            log_failure(Stage.SEARCH_FORM, "could not read doc-type checklist", exc)
            return []
        if not meta:
            log_stage(Stage.SEARCH_FORM, "no doc-type checklist found", logging.WARNING)
            return []

        ids, labels = [], []
        for item in meta:
            label = (item.get("label") or "").strip()
            low = label.lower()
            if not low:
                continue
            for cat in cats:
                if any(a in low for a in cat.aliases) and \
                        not any(x in low for x in cat.exclude):
                    ids.append(item["id"])
                    labels.append(label)
                    break

        for cid in ids:
            try:
                box = page.locator(f"#{cid}")
                await box.scroll_into_view_if_needed(timeout=4_000)
                if not await box.is_checked():
                    await box.check(force=True, timeout=4_000)
            except Exception as exc:  # noqa: BLE001
                # JS fallback (some IG skins overlay the native checkbox)
                try:
                    await page.evaluate(
                        "(id)=>{const e=document.getElementById(id); if(e&&!e.checked)e.click();}",
                        cid)
                except Exception:  # noqa: BLE001
                    log_stage(Stage.SEARCH_FORM, f"could not check {cid} ({exc})",
                              logging.DEBUG)
        return _dedupe(labels)

    async def _submit(self, page) -> bool:
        candidates = [
            SEL_SEARCH_BTN, "[id*='SearchButtons1_btnSearch']", "[id*='btnSearch']",
            "input[type='submit'][value='Search']",
            "input[type='submit'][value*='Search' i]",
        ]
        for sel in candidates:
            try:
                loc = page.locator(sel).first
                if await loc.count() == 0 or not await loc.is_visible():
                    continue

                async def _go():
                    await loc.click(timeout=NAV_TIMEOUT_MS)
                    await self._wait_results(page)
                await retry_async(_go, stage=Stage.RESULTS, what="submit search")
                return True
            except Exception as exc:  # noqa: BLE001
                log_stage(Stage.RESULTS, f"submit '{sel}' failed ({exc})", logging.DEBUG)
        return False

    async def _wait_results(self, page) -> None:
        try:
            await page.wait_for_load_state("networkidle", timeout=NAV_TIMEOUT_MS)
        except Exception:  # noqa: BLE001
            pass
        try:
            await page.wait_for_selector(
                f"{SEL_RESULTS_TABLE}, text=/records?\\s+found/i, "
                f"text=/no\\s+records/i, text=/no\\s+results/i",
                timeout=NAV_TIMEOUT_MS)
        except Exception:  # noqa: BLE001
            pass
        await page.wait_for_timeout(800)

    async def _collect(self, page, primary: Optional[Category],
                       cats: Sequence[Category], label: str) -> List[Dict[str, Any]]:
        recs: List[Dict[str, Any]] = []
        total: Optional[int] = None
        pages = 0
        while pages < self.max_result_pages:
            pages += 1
            try:
                data = await page.evaluate(JS_EXTRACT_RESULTS)
            except Exception as exc:  # noqa: BLE001
                log_failure(Stage.PARSING, f"{label}: results extract failed", exc)
                break

            if not data or data.get("err"):
                # generic BeautifulSoup fallback
                try:
                    html = await page.content()
                    fb = parse_results_html(html, CLERK_BASE, primary, cats)
                    recs.extend(fb)
                    if not fb:
                        log_stage(Stage.PARSING, f"{label}: no results grid found "
                                  f"({(data or {}).get('err')})", logging.DEBUG)
                except Exception as exc:  # noqa: BLE001
                    log_failure(Stage.PARSING, f"{label}: fallback parse failed", exc)
                break

            if total is None:
                total = data.get("total")
                if total is not None:
                    log_stage(Stage.RESULTS, f"{label}: portal reports {total} record(s)")

            recs.extend(rows_to_records(data, primary, cats))
            if not await self._next_page(page, len(recs), total):
                break

        if total is not None and len(recs) < total:
            capped = pages >= self.max_result_pages
            log_stage(Stage.RESULTS,
                      f"{label}: captured {len(recs)} of {total}"
                      + (f" (stopped at page cap {self.max_result_pages})" if capped else ""),
                      logging.WARNING if capped else logging.INFO)
        else:
            log_stage(Stage.RESULTS, f"{label}: captured {len(recs)} record(s)")
        return recs, total

    async def _next_page(self, page, got: int, total: Optional[int]) -> bool:
        if total is not None and got >= total:
            return False
        for sel in ("[id$='imgNext']", "[id*='imgNext']",
                    "input[id*='Next'][type='image']", "a:has-text('Next')",
                    "input[type='submit'][value*='Next' i]"):
            try:
                loc = page.locator(sel).first
                if await loc.count() == 0 or not await loc.is_visible():
                    continue
                if await loc.get_attribute("disabled") is not None:
                    continue
                await loc.click(timeout=NAV_TIMEOUT_MS)
                await self._wait_results(page)
                return True
            except Exception:  # noqa: BLE001
                continue
        return False


# ============================================================================ #
#  Generic results parser (BeautifulSoup) — FALLBACK only                       #
#  Primary extraction is ClerkScraper._collect via JS_EXTRACT_RESULTS.          #
# ============================================================================ #

HEADER_KEYWORDS = {
    "doc_num": ("inst num", "document number", "doc number", "document #",
                "doc #", "instrument", "recording number", "document no", "doc no"),
    "filed": ("recording date", "record date", "date recorded", "date filed",
              "filed", "file date"),
    "doc_type": ("document type", "doc type", "type"),
    "owner": ("grantor", "name", "from", "first party", "party 1"),
    "grantee": ("grantee", "associated name", "to", "second party", "party 2"),
    "legal": ("legal", "description", "legal description"),
    "amount": ("amount", "consideration", "debt"),
    "city": ("city",),
}


def _classify_header(text: str) -> Optional[str]:
    t = clean_ws(text).lower()
    if not t:
        return None
    for field_name, kws in HEADER_KEYWORDS.items():
        if any(k == t for k in kws):
            return field_name
    for field_name, kws in HEADER_KEYWORDS.items():
        if any(k in t for k in kws):
            return field_name
    return None


def parse_results_html(html: str, base_url: str, cat: Optional[Category],
                       cats: Sequence[Category]) -> List[Dict[str, Any]]:
    if BeautifulSoup is None:
        log_failure(Stage.PARSING, "BeautifulSoup not installed")
        return []
    soup = BeautifulSoup(html, "lxml")

    best_table, best_map, best_score = None, {}, 0
    for table in soup.find_all("table"):
        header_cells = _find_header_cells(table)
        if not header_cells:
            continue
        col_map: Dict[str, int] = {}
        for i, cell in enumerate(header_cells):
            field_name = _classify_header(cell.get_text(" ", strip=True))
            if field_name and field_name not in col_map:
                col_map[field_name] = i
        score = len(set(col_map) & {"doc_num", "filed", "doc_type", "owner", "grantee"})
        if score > best_score:
            best_score, best_table, best_map = score, table, col_map

    if not best_table or best_score < 2:
        if re.search(r"no\s+(records|results|rows|matches)",
                     soup.get_text(" ", strip=True), re.IGNORECASE):
            return []
        return []

    primary_cat = cat
    records: List[Dict[str, Any]] = []
    for tr in _data_rows(best_table):
        try:
            rec = _row_to_record(tr, best_map, base_url, primary_cat, cats)
            if rec is not None:
                records.append(rec)
        except Exception as exc:  # noqa: BLE001
            log_stage(Stage.PARSING, f"skipped bad row ({exc})", logging.DEBUG)
    return records


def _find_header_cells(table) -> list:
    thead = table.find("thead")
    if thead:
        thr = thead.find("tr")
        if thr and thr.find_all(["th", "td"]):
            return thr.find_all(["th", "td"])
    first_tr = table.find("tr")
    if first_tr and first_tr.find_all("th"):
        return first_tr.find_all("th")
    return []


def _data_rows(table) -> list:
    tbody = table.find("tbody")
    trs = tbody.find_all("tr") if tbody else table.find_all("tr")
    out = []
    for tr in trs:
        if tr.find("th") and not tr.find("td"):
            continue
        if tr.find_all("td"):
            out.append(tr)
    return out


def _row_to_record(tr, col_map: Dict[str, int], base_url: str,
                   primary_cat: Optional[Category],
                   cats: Sequence[Category]) -> Optional[Dict[str, Any]]:
    cells = tr.find_all("td")
    if not cells:
        return None

    def cell_text(field_name: str) -> str:
        idx = col_map.get(field_name)
        if idx is None or idx >= len(cells):
            return ""
        return clean_ws(cells[idx].get_text(" ", strip=True))

    rec = blank_record()
    rec["doc_num"] = cell_text("doc_num")
    rec["filed"] = cell_text("filed")
    rec["doc_type"] = cell_text("doc_type")
    rec["owner"] = clean_party(cell_text("owner"))
    rec["grantee"] = clean_party(cell_text("grantee"))
    rec["legal"] = cell_text("legal")
    amt = cell_text("amount")
    rec["amount"] = extract_amount(amt) if amt else None
    city = cell_text("city")
    if city:
        rec["prop_city"], rec["prop_state"] = city, "CA"

    d = parse_any_date(rec["filed"])
    if d:
        rec["filed"] = fmt_mdy(d)

    if primary_cat:
        rec["cat"], rec["cat_label"] = primary_cat.code, primary_cat.label
        rec["_cats"] = {primary_cat.code}
    else:
        inferred = _infer_category(rec["doc_type"], cats)
        if inferred:
            rec["cat"], rec["cat_label"] = inferred.code, inferred.label
            rec["_cats"] = {inferred.code}

    rec["clerk_url"] = _row_link(tr, base_url) or CLERK_SEARCH_ENTRY
    if not rec["doc_num"] and not rec["owner"] and not rec["doc_type"]:
        return None
    return rec


def _infer_category(doc_type: str, cats: Sequence[Category]) -> Optional[Category]:
    low = (doc_type or "").lower()
    if not low:
        return None
    for cat in cats:
        if any(a in low for a in cat.aliases) and not any(x in low for x in cat.exclude):
            return cat
    for cat in CATEGORIES:
        if any(a in low for a in cat.aliases) and not any(x in low for x in cat.exclude):
            return cat
    return None


def _row_link(tr, base_url: str) -> str:
    from urllib.parse import urljoin
    a = tr.find("a", href=True)
    if a:
        href = a["href"].strip()
        if href and not href.lower().startswith("javascript"):
            return urljoin(base_url, href)
    return ""


# ============================================================================ #
#  Dedup                                                                        #
# ============================================================================ #

def dedupe_records(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    order: List[str] = []
    no_key: List[Dict[str, Any]] = []
    for rec in records:
        key = clean_ws(rec.get("doc_num", "")).upper()
        if not key:
            # No instrument number: fall back to a composite identity so genuine
            # duplicates (same party/date/type) still merge instead of doubling.
            alt = "|".join(clean_ws(rec.get(f, "")).upper()
                           for f in ("owner", "filed", "doc_type", "grantee"))
            key = alt if alt.strip("|") else ""
        if not key:
            no_key.append(rec)
            continue
        if key not in out:
            out[key] = rec
            order.append(key)
        else:
            base = out[key]
            base.setdefault("_cats", set()).update(rec.get("_cats") or set())
            for f in ("owner", "grantee", "legal", "doc_type", "filed",
                      "prop_address", "prop_city", "mail_address", "clerk_url"):
                if not base.get(f) and rec.get(f):
                    base[f] = rec[f]
            if base.get("amount") is None and rec.get("amount") is not None:
                base["amount"] = rec["amount"]
    return [out[k] for k in order] + no_key


# ============================================================================ #
#  Output: JSON + GHL CSV                                                       #
# ============================================================================ #

def _public_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in rec.items() if not k.startswith("_")}


def build_payload(records: List[Dict[str, Any]], start: datetime,
                  end: datetime) -> Dict[str, Any]:
    public = [_public_record(r) for r in records]
    with_addr = sum(1 for r in public if r.get("prop_address") or r.get("mail_address"))
    return {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "source": SOURCE_LABEL,
        "date_range": {"from": start.date().isoformat(),
                       "to": end.date().isoformat(),
                       "lookback_days": (end - start).days},
        "total": len(public),
        "with_address": with_addr,
        "records": public,
    }


def write_json(payload: Dict[str, Any], paths: Sequence[Path]) -> None:
    for path in paths:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(payload, indent=2, ensure_ascii=False),
                            encoding="utf-8")
            log_stage(Stage.EXPORT, f"wrote {len(payload['records'])} records -> {path}")
        except Exception as exc:  # noqa: BLE001
            log_failure(Stage.EXPORT, f"could not write {path}", exc)


GHL_COLUMNS = [
    "First Name", "Last Name", "Phone", "Email",
    "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
    "Property Address", "Property City", "Property State", "Property Zip",
    "Lead Type", "Document Type", "Date Filed", "Document Number",
    "Amount/Debt Owed", "Seller Score", "Motivated Seller Flags",
    "Source", "Public Records URL",
]
# Phone/Email are intentionally blank here — public records never contain them.
# Populate via skip tracing (BatchSkipTracing / PropStream / REISkip) using the
# name + mailing address, then re-import. GHL maps the "Phone"/"Email" headers.


def export_ghl_csv(records: List[Dict[str, Any]], paths: Sequence[Path]) -> None:
    rows: List[List[str]] = []
    for rec in records:
        first, last = split_person_name(rec.get("owner", ""))
        amount = rec.get("amount")
        rows.append([
            first, last,
            rec.get("phone", ""), rec.get("email", ""),
            rec.get("mail_address", ""), rec.get("mail_city", ""),
            rec.get("mail_state", ""), rec.get("mail_zip", ""),
            rec.get("prop_address", ""), rec.get("prop_city", ""),
            rec.get("prop_state", ""), rec.get("prop_zip", ""),
            rec.get("cat_label", "") or rec.get("cat", ""),
            rec.get("doc_type", ""), rec.get("filed", ""), rec.get("doc_num", ""),
            (f"{amount:.2f}" if isinstance(amount, (int, float)) else ""),
            str(rec.get("score", "")),
            "; ".join(rec.get("flags", [])),
            SOURCE_LABEL, rec.get("clerk_url", ""),
        ])
    for path in paths:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("w", newline="", encoding="utf-8-sig") as fh:
                writer = csv.writer(fh)
                writer.writerow(GHL_COLUMNS)
                writer.writerows(rows)
            log_stage(Stage.EXPORT, f"wrote GHL CSV ({len(rows)} rows) -> {path}")
        except Exception as exc:  # noqa: BLE001
            log_failure(Stage.EXPORT, f"could not write CSV {path}", exc)


# ============================================================================ #
#  Optional bulk DBF fallback (only if a .dbf is supplied)                      #
# ============================================================================ #

def load_owner_index_from_dbf(dbf_path: Path) -> Dict[str, Dict[str, Any]]:
    """
    OPTIONAL: if the Assessor publishes a bulk DBF with owner names keyed by APN,
    load it here so owner/address enrichment can run offline. No-op if the file
    or `dbfread` is missing. Returns {APN: {fields...}}.
    """
    index: Dict[str, Dict[str, Any]] = {}
    if not dbf_path or not dbf_path.exists():
        return index
    try:
        from dbfread import DBF  # type: ignore
    except Exception:  # noqa: BLE001
        log_stage(Stage.ENRICHMENT, "dbfread not installed; skipping DBF", logging.WARNING)
        return index
    try:
        for row in DBF(str(dbf_path), load=True, ignore_missing_memofile=True):
            apn = str(row.get("APN") or row.get("PARCEL") or "").strip()
            if apn:
                index[apn] = dict(row)
        log_stage(Stage.ENRICHMENT, f"loaded {len(index)} parcels from DBF")
    except Exception as exc:  # noqa: BLE001
        log_failure(Stage.ENRICHMENT, f"DBF load failed ({dbf_path})", exc)
    return index


# ============================================================================ #
#  Orchestration                                                                #
# ============================================================================ #

def select_categories(codes_arg: Optional[str]) -> List[Category]:
    if not codes_arg:
        return list(CATEGORIES)
    wanted = [c.strip().upper() for c in codes_arg.split(",") if c.strip()]
    out = []
    for code in wanted:
        cat = CATEGORY_BY_CODE.get(code)
        if cat:
            out.append(cat)
        else:
            log_stage(Stage.INIT, f"unknown category code '{code}' ignored", logging.WARNING)
    return out or list(CATEGORIES)


async def run_pipeline(args: argparse.Namespace) -> Dict[str, Any]:
    cats = select_categories(args.cats)
    log_stage(Stage.INIT,
              f"categories: {[c.code for c in cats]} | lookback={args.lookback_days}d "
              f"| headless={not args.headed} | enrich={not args.no_enrich}")
    start, end = date_range(args.lookback_days)

    # ---- 1) Scrape the clerk portal --------------------------------------- #
    records: List[Dict[str, Any]] = []
    if not args.dry_run:
        scraper = ClerkScraper(headless=not args.headed,
                               lookback_days=args.lookback_days,
                               max_result_pages=args.max_pages)
        try:
            records = await scraper.run(cats, grouped=args.grouped)
        except Exception as exc:  # noqa: BLE001
            log_failure(Stage.RESULTS, "clerk scrape aborted", exc)
    else:
        log_stage(Stage.INIT, "dry-run: skipping live clerk scrape")

    # ---- 2) Dedup --------------------------------------------------------- #
    records = dedupe_records(records)
    log_stage(Stage.PARSING, f"{len(records)} unique document(s) after dedup")

    # ---- 3) Enrich -------------------------------------------------------- #
    if not args.no_enrich and records:
        enricher = ArcGISEnricher(enabled=True)
        for rec in records:
            try:
                enricher.enrich(rec)
            except Exception as exc:  # noqa: BLE001
                log_failure(Stage.ENRICHMENT,
                            f"enrich failed for doc {rec.get('doc_num')}", exc)
        log_stage(Stage.ENRICHMENT,
                  f"enrichment: {enricher.stats['hits']} hit / "
                  f"{enricher.stats['misses']} miss / {enricher.stats['errors']} err "
                  f"({enricher.stats['queries']} queries)")

    # ---- 4) Score --------------------------------------------------------- #
    try:
        score_all(records, args.lookback_days)
    except Exception as exc:  # noqa: BLE001
        log_failure(Stage.PARSING, "scoring failed", exc)
    records.sort(key=lambda r: r.get("score", 0), reverse=True)

    # ---- 5) Output -------------------------------------------------------- #
    payload = build_payload(records, start, end)
    write_json(payload, [OUT_DASHBOARD / "records.json", OUT_DATA / "records.json"])
    export_ghl_csv([_public_record(r) for r in records],
                   [OUT_DASHBOARD / "leads_ghl.csv", OUT_DATA / "leads_ghl.csv"])
    log_stage(Stage.EXPORT,
              f"DONE - {payload['total']} leads, {payload['with_address']} with address")
    return payload


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="MADM motivated-seller lead scraper (Alameda County, CA).")
    p.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS,
                   help=f"days to look back (default {DEFAULT_LOOKBACK_DAYS})")
    p.add_argument("--cats", type=str, default=None,
                   help="comma-separated category codes (default: all). Options: "
                        f"{','.join(c.code for c in CATEGORIES)}")
    p.add_argument("--grouped", action="store_true",
                   help="run ONE combined search instead of one per category")
    p.add_argument("--headed", action="store_true",
                   help="show the browser window (default: headless)")
    p.add_argument("--no-enrich", action="store_true",
                   help="skip ArcGIS parcel enrichment")
    p.add_argument("--max-pages", type=int, default=25,
                   help="max result pages to paginate per search (default 25)")
    p.add_argument("--dry-run", action="store_true",
                   help="skip the live scrape; just (re)write empty outputs")
    p.add_argument("--verbose", action="store_true", help="debug logging")
    return p


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)
    setup_logging(verbose=args.verbose)
    log_stage(Stage.INIT, "MADM lead scraper starting")
    try:
        asyncio.run(run_pipeline(args))
        return 0
    except KeyboardInterrupt:
        log_stage(Stage.INIT, "interrupted by user", logging.WARNING)
        return 130
    except Exception as exc:  # noqa: BLE001 — top-level guard, never hard-crash CI
        log_failure(Stage.INIT, "fatal error in pipeline", exc)
        try:
            start, end = date_range(args.lookback_days)
            write_json(build_payload([], start, end),
                       [OUT_DASHBOARD / "records.json", OUT_DATA / "records.json"])
            # Keep CSV consistent with the emptied JSON (don't leave a stale CSV).
            export_ghl_csv([], [OUT_DASHBOARD / "leads_ghl.csv",
                                OUT_DATA / "leads_ghl.csv"])
        except Exception:  # noqa: BLE001
            pass
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
