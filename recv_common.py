#!/usr/bin/env python3
# -*- coding: ascii -*-
# MOSS M12 recv - shared helpers for the receiving bot.
# ASCII-only source, UTF-8 at runtime. Style follows m3_telegram_ingest.py and
# m5_photo_verify.py: logs to stderr, --dry aware, gspread HEADER_ROW=2,
# LLM via `openclaw infer`, Telegram via `openclaw message send`,
# Dotypos over a tiny urllib client (based on /tmp/recv_audit.py).
#
# This module writes NOTHING to Dotypos by itself. The only live write lives in
# m12_recv_post.py and is gated behind an explicit --live flag (see sec.0 of brief).

import hashlib
import json
import os
import re
import sys
import time
import subprocess
import urllib.request
import urllib.parse
import urllib.error

try:
    import fcntl  # POSIX only; present on the Linux VPS the bot runs on.
except ImportError:  # pragma: no cover - non-POSIX dev boxes
    fcntl = None

# ---------------------------------------------------------------------------
# Constants (this server / this cloud). No secrets here - tokens are read from
# files in SECRETS_DIR at runtime.
# ---------------------------------------------------------------------------
SECRETS_DIR = "/data/.openclaw/secrets"
DOTY_SECRET = os.path.join(SECRETS_DIR, "dotykacka.json")
GOOGLE_SA = os.path.join(SECRETS_DIR, "google_sa.json")
PROMPT_FILE = os.path.join(SECRETS_DIR, "MOSS_M12_Recv_Prompt.md")

RECV_SHEET_ID = "1A8Hk9ePINaI3pbwo47lfCDOX105ULW5dDghApunxjsI"
DRIVE_WZ_INBOX_FOLDER_ID = "1bhXw-wI1xR1aYrRTssXHlKc7HqmpKhpE"

DOTY_API_ROOT = "https://api.dotykacka.cz/v2"
DOTY_CLOUD_ID = "352797827"
DOTY_CLOUD_BASE = DOTY_API_ROOT + "/clouds/" + DOTY_CLOUD_ID
DOTY_WAREHOUSE_ID = "144068569"  # MAGAZYN - the single warehouse

# Known category ids (rest are resolved via GET /categories at runtime).
CAT_SKLADNIKI = "1548073505771091"
CAT_SKLEP_KAWA_HERB = "1083937491148571"

# Telegram Admin sink (group + thread), same as m5.
TG_TARGET = "-1004228327097"
TG_THREAD = "1"

# LLM models (see brief sec.2).
LLM_VISION = "openai/gpt-5.5"
LLM_TEXT = "deepseek/deepseek-v4-flash"
LLM_TEXT_FALLBACK = "openai/gpt-5.5"

MATCH_THRESHOLD = 0.75
HEADER_ROW = 2  # column names live in row 2; data starts at row 3

# The buyer is ALWAYS this NIP (Kawiarnia Moss / Fortbolt). A parsed document
# that puts this NIP as the supplier has the parties swapped - the ingest fixes
# it (brief batch2 sec.2.5). Kept here so both parse post-processing and any
# future check share one source of truth.
BUYER_NIP = "5252948161"

# Server-side self-check thresholds (brief batch2 sec.2.2). Constants for now;
# a later batch may move them to a config tab.
#  rachunek?  -> |qty_doc*price_doc_net - total_doc_net| > max(abs, rel*total)
#  cena?      -> price_skl outside [ref*CENA_LO .. ref*CENA_HI]
RACHUNEK_ABS_PLN = 0.02
RACHUNEK_REL = 0.005
CENA_LO = 0.5
CENA_HI = 2.0

# Document-sum control (brief batch5 task2): |sum of line totals - printed
# "Razem netto"| above this -> flag_suma on Recv_Docs. Advisory only, never
# blocks Zatwierdz. Control is skipped when the document prints no total.
SUMA_TOL_PLN = float(os.environ.get("M12_SUMA_TOL", "0.02") or "0.02")

# Sheet tab names (contract with the manager app, brief sec.4).
TAB_DOCS = "Recv_Docs"
TAB_LINES = "Recv_Lines"
TAB_DICT = "Recv_Dictionary"
TAB_SUPPLIERS = "Recv_Suppliers"
TAB_CATALOG = "Recv_Catalog"
TAB_KOSZT = "Recv_Koszt"
TAB_TASKS = "Recv_Tasks"
TAB_CONTROL = "Recv_Control"
TAB_FILES = "Recv_Files"
TAB_CSV_TEMPLATES = "Recv_CsvTemplates"

# Recv_CsvTemplates - per-supplier CSV column mapping (brief batch2 sec.2.4). The
# first CSV of a supplier is mapped by the LLM; the confirmed mapping is stored
# here (keyed by supplier NIP + a signature of the header row) so subsequent CSVs
# parse deterministically with no LLM call. header_signature is a stable hash of
# the lowercased, trimmed column names.
CSV_TEMPLATES_HEADERS = ["supplier_nip", "header_signature", "mapping_json",
                         "confirmed", "created_at"]

# Recv_Files - registry of processed source files (exact-duplicate guard, audit).
# One row per physical file the bot has parsed. sha256/md5 are content hashes of
# the downloaded bytes; a byte-identical re-upload matches and is skipped
# silently (brief item 2). Never used to reject a mere re-scan of the same
# drive_file_id (that is handled by known_file_ids), only true content dups.
FILES_HEADERS = ["sha256", "md5", "doc_id", "drive_file_id",
                 "filename", "doc_number", "supplier", "ts",
                 # batch2 sec.5: file/doc claim for the double-processing guard.
                 "claimed_at", "claimed_by"]

# How long a claim (Recv_Docs.claimed_at / Recv_Files.claimed_at) is honored: a
# doc claimed more recently than this by ANOTHER run is skipped (brief batch2
# sec.5). Long enough to cover a slow parse, short enough that a crashed run
# does not wedge a document forever.
CLAIM_FRESH_SECONDS = int(os.environ.get("M12_CLAIM_FRESH_SECONDS", "900") or "900")

# Cross-process mutex so the */5 ingest cron and the */3 --if-requested trigger
# never parse the same inbox concurrently (brief batch2 sec.5). A whole run holds
# the lock; a second run that cannot get it exits at once.
INGEST_LOCK_PATH = "/tmp/m12_ingest.lock"

# Subfolder of the WZ inbox where successfully parsed files are moved so the
# inbox always shows only what still needs work (brief item 4).
DRIVE_PROCESSED_SUBFOLDER = "Przetworzone"

# Recv_Control - single-row control channel between the manager app and this bot.
# The app writes scan_request (epoch ms) when the manager taps "Rozpoznaj"; the
# trigger cron (m12_recv_ingest --if-requested) claims it by writing scan_done.
# Epoch-ms tokens are timezone-independent, so the freshness math is safe no
# matter what timezone the Gateway container runs in.
CONTROL_HEADERS = ["key", "scan_request", "scan_requested_by",
                   "scan_done", "updated_at", "note",
                   # batch2 sec.8: "Przygotuj do wysylki" (post --dry) request on
                   # the SAME single control row. post_dry_request carries a token
                   # "<doc_id>#<epoch_ms>" so re-requesting the same doc re-fires;
                   # the trigger mirrors it into post_dry_done when handled.
                   "post_dry_request", "post_dry_requested_by",
                   "post_dry_done", "post_dry_note",
                   # batch5 task4: "Rozpoznaj ponownie" - re-parse ONE doc with a
                   # FORCED route. Token "<doc_id>#<pdf-text|vision>#<epoch_ms>";
                   # the trigger claims it via rescan_done (scan-style).
                   "rescan_request", "rescan_requested_by",
                   "rescan_done", "rescan_note"]


# ---------------------------------------------------------------------------
# logging
# ---------------------------------------------------------------------------
def log(msg):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    sys.stderr.write("[%s] %s\n" % (ts, msg))
    sys.stderr.flush()


def today_str():
    return time.strftime("%Y-%m-%d")


# Sheets quota / 429 backoff (brief batch2 sec.7). The bot shares the Sheets quota
# with the console; a burst can return HTTP 429 (RESOURCE_EXHAUSTED). Instead of a
# fatal Telegram alert we retry with exponential backoff and only fail (raise) if
# all attempts are exhausted. Env-tunable.
SHEETS_RETRY_TRIES = int(os.environ.get("M12_SHEETS_RETRY_TRIES", "3") or "3")
SHEETS_RETRY_BASE_S = float(os.environ.get("M12_SHEETS_RETRY_BASE_S", "20") or "20")


def _is_quota_error(e):
    code = getattr(getattr(e, "response", None), "status_code", None)
    if code == 429:
        return True
    s = str(e).lower()
    return ("429" in s or "resource_exhausted" in s or "quota exceeded" in s
            or "rate limit" in s or "ratelimit" in s or "too many requests" in s)


def with_backoff(fn, what="sheets", tries=None, base=None):
    # Run fn(); on a 429/quota error retry with exponential backoff (e.g. 20/40/80
    # s). Non-quota errors propagate immediately. Raises the last error only after
    # all attempts fail (caller decides whether that is fatal).
    tries = SHEETS_RETRY_TRIES if tries is None else tries
    delay = SHEETS_RETRY_BASE_S if base is None else base
    for i in range(tries):
        try:
            return fn()
        except Exception as e:
            if not _is_quota_error(e) or i == tries - 1:
                raise
            log("%s 429/quota (try %d/%d) -> backoff %ds: %s"
                % (what, i + 1, tries, int(delay), str(e)[:140]))
            time.sleep(delay)
            delay *= 2


# Held for the process lifetime so the OS releases it automatically on exit; kept
# in a module global so the file object is not garbage-collected mid-run.
_INGEST_LOCK_FH = None


def acquire_singleton_lock(path=INGEST_LOCK_PATH):
    # Non-blocking exclusive flock (brief batch2 sec.5). Returns True if THIS
    # process now holds the lock, False if another run holds it. On a platform
    # without fcntl we degrade to "always acquired" (dev boxes; the VPS is POSIX).
    global _INGEST_LOCK_FH
    if fcntl is None:
        return True
    try:
        fh = open(path, "w")
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (IOError, OSError):
        return False
    _INGEST_LOCK_FH = fh  # keep the fd open for the whole run
    try:
        fh.write("%d\n" % os.getpid())
        fh.flush()
    except Exception:
        pass
    return True


def backup_file(path):
    # Make a dated .bak before overwriting an existing file (brief sec.2).
    if os.path.exists(path):
        bak = "%s.bak_%s" % (path, time.strftime("%Y%m%d"))
        try:
            with open(path, "rb") as src, open(bak, "wb") as dst:
                dst.write(src.read())
            log("backup: %s -> %s" % (path, bak))
        except Exception as e:
            log("backup failed for %s: %s" % (path, e))


# ---------------------------------------------------------------------------
# Dotypos client (read-only helpers + a gated write). urllib only.
# ---------------------------------------------------------------------------
class Doty(object):
    def __init__(self):
        with open(DOTY_SECRET, "r") as f:
            sec = json.load(f)
        self.refresh_token = sec["refresh_token"]
        self.cloud_id = str(sec.get("cloud_id", DOTY_CLOUD_ID))
        self._token = None
        self._token_ts = 0

    def _access_token(self):
        # Cache the access token for a few minutes.
        if self._token and (time.time() - self._token_ts) < 300:
            return self._token
        body = json.dumps({"_cloudId": self.cloud_id}).encode("utf-8")
        req = urllib.request.Request(
            DOTY_API_ROOT + "/signin/token",
            data=body,
            method="POST",
        )
        req.add_header("Content-Type", "application/json")
        req.add_header("Authorization", "User " + self.refresh_token)
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        self._token = data["accessToken"]
        self._token_ts = time.time()
        return self._token

    def _request(self, method, path, body=None, etag=None):
        # path is relative to the cloud base unless it starts with http.
        url = path if path.startswith("http") else DOTY_CLOUD_BASE + path
        data = None
        if body is not None:
            data = json.dumps(body).encode("utf-8")
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Authorization", "Bearer " + self._access_token())
        req.add_header("Accept", "application/json")
        if data is not None:
            req.add_header("Content-Type", "application/json")
        if etag:
            req.add_header("If-Match", etag)
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                raw = resp.read().decode("utf-8")
                hdrs = dict(resp.headers.items())
                parsed = json.loads(raw) if raw.strip() else {}
                return resp.status, parsed, hdrs
        except urllib.error.HTTPError as e:
            detail = ""
            try:
                detail = e.read().decode("utf-8")
            except Exception:
                pass
            raise RuntimeError("Dotypos %s %s -> HTTP %s %s"
                               % (method, url, e.code, detail[:400]))

    def get(self, path):
        _, data, hdrs = self._request("GET", path)
        return data, hdrs

    def get_all(self, path, params=None):
        # Handle both raw-array and {"data":[...]} responses, page from 1.
        out = []
        page = 1
        while True:
            q = dict(params or {})
            q["page"] = page
            q["limit"] = 100
            sep = "&" if "?" in path else "?"
            _, data, _ = self._request("GET", path + sep + urllib.parse.urlencode(q))
            items = data.get("data") if isinstance(data, dict) else data
            if not items:
                break
            out.extend(items)
            if len(items) < 100:
                break
            page += 1
            if page > 500:
                log("get_all: page guard hit for %s" % path)
                break
        return out

    def post(self, path, body, etag=None):
        # Raw POST. Callers in m12_recv_post gate this behind --live.
        status, data, hdrs = self._request("POST", path, body=body, etag=etag)
        return status, data, hdrs

    def put(self, path, body, etag=None):
        status, data, hdrs = self._request("PUT", path, body=body, etag=etag)
        return status, data, hdrs

    # convenience reads used by catalog / post
    def categories(self):
        return self.get_all("/categories")

    def products(self):
        return self.get_all("/products")

    def warehouse_products(self):
        return self.get_all("/warehouses/" + DOTY_WAREHOUSE_ID + "/products")


# ---------------------------------------------------------------------------
# Google Sheets (gspread, HEADER_ROW=2)
# ---------------------------------------------------------------------------
_GC = None


def gc():
    global _GC
    if _GC is None:
        import gspread
        _GC = gspread.service_account(filename=GOOGLE_SA)
    return _GC


# Per-process caches to cut Sheets READ quota. gspread's sh.worksheet(title)
# fetches the ENTIRE spreadsheet metadata on EVERY call, so a run that opens 5
# tabs used to spend 5 metadata reads; we fetch the worksheet list ONCE and reuse
# it. Each cron invocation is a fresh process, so there is no stale-cache risk
# across runs. open_by_key itself is lazy (no API call until data is read).
_SH = None
_WS_CACHE = {}  # sheet_id -> {title: Worksheet}


def _spreadsheet(sheet_id=RECV_SHEET_ID):
    global _SH
    if _SH is None or _SH.id != sheet_id:
        _SH = gc().open_by_key(sheet_id)  # lazy: no metadata fetch here
    return _SH


def _worksheets_map(sh):
    m = _WS_CACHE.get(sh.id)
    if m is None:
        # ONE metadata read, with 429 backoff (brief batch2 sec.7).
        wss = with_backoff(sh.worksheets, what="worksheets meta")
        m = dict((ws.title, ws) for ws in wss)
        _WS_CACHE[sh.id] = m
    return m


def open_ws(tab, headers=None, sheet_id=RECV_SHEET_ID):
    # Open a worksheet, creating it with a HEADER_ROW=2 layout if missing. Reuses
    # a per-process worksheet map so repeated opens in one run do not re-fetch the
    # spreadsheet metadata.
    sh = _spreadsheet(sheet_id)
    wsmap = _worksheets_map(sh)
    ws = wsmap.get(tab)
    if ws is None:
        ws = with_backoff(
            lambda: sh.add_worksheet(title=tab, rows=1000,
                                     cols=max(10, len(headers or []) + 2)),
            what="add_worksheet %s" % tab)
        wsmap[tab] = ws
    if headers:
        cur = with_backoff(lambda: ws.row_values(HEADER_ROW), what="header %s" % tab)
        if [h.strip() for h in cur[:len(headers)]] != headers:
            # write column names into row 2 (row 1 stays as a human title/comment)
            end = _col_letter(len(headers))
            with_backoff(
                lambda: ws.update("A%d:%s%d" % (HEADER_ROW, end, HEADER_ROW), [headers]),
                what="write header %s" % tab)
    return ws


def ensure_columns(ws, headers, cols):
    # Append any of `cols` that are MISSING from the header row (HEADER_ROW=2) to
    # the END of it, and return the (possibly extended) header list. Append-only:
    # never reorders or renames existing columns (brief batch4 sec.1.2). The bot
    # needs this because it opens tabs without a header contract (open_ws(TAB)) and
    # therefore cannot rely on the console having created a new column yet - e.g.
    # supplier_nip on Recv_Docs (batch4): without this, _set_doc silently dropped
    # the write because the column did not exist in the sheet.
    missing = [c for c in cols if c not in headers]
    if not missing:
        return headers
    new_headers = list(headers) + missing
    title = getattr(ws, "title", "?")
    # batch5.1 task2: writing past the sheet's PHYSICAL grid is a 400 "exceeds
    # grid limits" (a default tab has 26 columns; Recv_Docs died on AA2:AD2).
    # Extend the grid FIRST, then write the header cells. Still append-only.
    try:
        cur_cols = int(ws.col_count or 0)
    except Exception:
        cur_cols = 0
    if cur_cols and len(new_headers) > cur_cols:
        add = len(new_headers) - cur_cols
        with_backoff(lambda: ws.add_cols(add), what="add_cols %s" % title)
        log("ensure_columns: grid extended %d->%d (%s)"
            % (cur_cols, len(new_headers), title))
    start = _col_letter(len(headers) + 1)
    end = _col_letter(len(new_headers))
    rng = "%s%d:%s%d" % (start, HEADER_ROW, end, HEADER_ROW)
    with_backoff(lambda: ws.update(rng, [missing]),
                 what="ensure_columns %s" % title)
    log("ensure_columns: added %s to %s" % (missing, title))
    return new_headers


def control_row():
    # ONE Sheets read: fetch JUST the Recv_Control range. No metadata fetch, no
    # header validation, no other tabs -- this keeps the every-minute
    # --if-requested trigger at a single read request. Returns
    # (row_dict, physical_row_number) for the key="control" row, or (None, None)
    # if the tab is absent/empty/unreadable (the trigger then simply no-ops).
    try:
        sh = _spreadsheet(RECV_SHEET_ID)
        resp = with_backoff(
            lambda: sh.values_get("%s!A%d:N" % (TAB_CONTROL, HEADER_ROW)),
            what="control read")
    except Exception as e:
        log("control read skipped: %s" % e)
        return None, None
    values = resp.get("values", []) or []
    if len(values) < 2:
        return None, None
    headers = [h.strip() for h in values[0]]
    for i, raw in enumerate(values[1:]):
        rec = {}
        for j, h in enumerate(headers):
            rec[h] = raw[j] if j < len(raw) else ""
        if (rec.get("key") or "").strip() == "control":
            return rec, HEADER_ROW + 1 + i  # physical row for set_cell
    return None, None


def _col_letter(n):
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def read_records(ws):
    # Return list of dicts using row 2 as headers, data from row 3 down.
    vals = with_backoff(ws.get_all_values, what="read %s" % getattr(ws, "title", "?"))
    if len(vals) < HEADER_ROW:
        return [], []
    headers = [h.strip() for h in vals[HEADER_ROW - 1]]
    rows = []
    for i, raw in enumerate(vals[HEADER_ROW:]):
        rec = {}
        for j, h in enumerate(headers):
            rec[h] = raw[j] if j < len(raw) else ""
        rec["_row"] = HEADER_ROW + 1 + i  # physical row number for update_cell
        rows.append(rec)
    return headers, rows


def append_rows(ws, headers, dict_rows):
    if not dict_rows:
        return
    matrix = []
    for d in dict_rows:
        matrix.append([_cell(d.get(h, "")) for h in headers])
    with_backoff(
        lambda: ws.append_rows(matrix, value_input_option="RAW",
                               table_range="A%d" % HEADER_ROW),
        what="append %s" % getattr(ws, "title", "?"))


def delete_rows_where(ws, headers, column_name, value):
    # Delete every DATA row whose `column_name` equals `value`. Used to make a
    # re-parse idempotent: clear a doc's old Recv_Lines before writing the fresh
    # set, so N re-runs give ONE line set, not the sum. Rows of other docs are
    # untouched. One fresh read + one delete call per contiguous block; blocks are
    # deleted bottom-up so physical row numbers stay valid while deleting.
    if column_name not in headers:
        return 0
    col = headers.index(column_name)  # 0-based into each row list
    vals = with_backoff(ws.get_all_values, what="read %s" % getattr(ws, "title", "?"))
    targets = []
    for i in range(HEADER_ROW, len(vals)):  # data starts at 0-based index HEADER_ROW
        row = vals[i]
        cell = row[col] if col < len(row) else ""
        if cell == value:
            targets.append(i + 1)  # physical 1-based row number
    if not targets:
        return 0
    targets.sort(reverse=True)
    deleted = 0
    lo = hi = targets[0]
    for r in targets[1:]:
        if r == lo - 1:
            lo = r
        else:
            with_backoff(lambda a=lo, b=hi: ws.delete_rows(a, b), what="delete rows")
            deleted += hi - lo + 1
            lo = hi = r
    with_backoff(lambda a=lo, b=hi: ws.delete_rows(a, b), what="delete rows")
    deleted += hi - lo + 1
    return deleted


def set_cell(ws, headers, row_number, column_name, value):
    if column_name not in headers:
        raise RuntimeError("column not found: " + column_name)
    col = headers.index(column_name) + 1
    with_backoff(lambda: ws.update_cell(row_number, col, _cell(value)),
                 what="set_cell %s" % column_name)


def _cell(v):
    if v is True:
        return "TRUE"
    if v is False:
        return "FALSE"
    if v is None:
        return ""
    return v if isinstance(v, str) else str(v)


# ---------------------------------------------------------------------------
# Google Drive (download / list) via the service account bearer token
# ---------------------------------------------------------------------------
_SA_TOKEN = None
_SA_TOKEN_TS = 0


def _sa_bearer():
    # Cache the service-account access token for the process (tokens live ~1h).
    # We now make several Drive calls per parsed page (download + get_meta +
    # move); minting a fresh token for each was wasteful. 45-min TTL is safely
    # under the token lifetime.
    global _SA_TOKEN, _SA_TOKEN_TS
    if _SA_TOKEN and (time.time() - _SA_TOKEN_TS) < 2700:
        return _SA_TOKEN
    from google.oauth2.service_account import Credentials
    from google.auth.transport.requests import Request
    scopes = ["https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(GOOGLE_SA, scopes=scopes)
    creds.refresh(Request())
    _SA_TOKEN = creds.token
    _SA_TOKEN_TS = time.time()
    return _SA_TOKEN


def drive_list(folder_id=DRIVE_WZ_INBOX_FOLDER_ID):
    # List non-trashed files in a Shared Drive folder. md5Checksum lets us skip a
    # byte-identical re-upload WITHOUT downloading it (exact-dup guard, item 2).
    # Folders are excluded so the "Przetworzone" subfolder is never mistaken for
    # an inbox document.
    token = _sa_bearer()
    q = urllib.parse.quote(
        "'%s' in parents and trashed = false "
        "and mimeType != 'application/vnd.google-apps.folder'" % folder_id)
    url = ("https://www.googleapis.com/drive/v3/files?q=%s"
           "&fields=files(id,name,mimeType,createdTime,md5Checksum,size)"
           "&supportsAllDrives=true&includeItemsFromAllDrives=true"
           "&corpora=allDrives&pageSize=1000" % q)
    req = urllib.request.Request(url)
    req.add_header("Authorization", "Bearer " + token)
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    return data.get("files", [])


def _drive_request(method, url, body=None):
    token = _sa_bearer()
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", "Bearer " + token)
    req.add_header("Accept", "application/json")
    if data is not None:
        req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=60) as resp:
        raw = resp.read().decode("utf-8")
        return json.loads(raw) if raw.strip() else {}


def drive_get_meta(file_id):
    # Fetch name/mime/parents for a single file (used to keep the extension and
    # find the current parent when moving a processed file).
    url = ("https://www.googleapis.com/drive/v3/files/%s"
           "?fields=id,name,mimeType,md5Checksum,parents&supportsAllDrives=true"
           % file_id)
    return _drive_request("GET", url)


def drive_ensure_folder(name, parent_id):
    # Return the id of subfolder `name` under parent_id, creating it if missing.
    q = urllib.parse.quote(
        "'%s' in parents and name = '%s' "
        "and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
        % (parent_id, name.replace("'", "\\'")))
    url = ("https://www.googleapis.com/drive/v3/files?q=%s"
           "&fields=files(id,name)&supportsAllDrives=true"
           "&includeItemsFromAllDrives=true&corpora=allDrives" % q)
    found = _drive_request("GET", url).get("files", [])
    if found:
        return found[0].get("id", "")
    body = {"name": name, "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_id]}
    created = _drive_request(
        "POST",
        "https://www.googleapis.com/drive/v3/files?supportsAllDrives=true&fields=id",
        body)
    return created.get("id", "")


def drive_move_rename(file_id, new_name, add_parent, remove_parent):
    # Move file into add_parent (out of remove_parent) and rename it. Files are
    # only MOVED, never deleted (audit trail, brief item 4).
    url = ("https://www.googleapis.com/drive/v3/files/%s"
           "?addParents=%s&removeParents=%s&supportsAllDrives=true&fields=id,name"
           % (file_id, urllib.parse.quote(add_parent),
              urllib.parse.quote(remove_parent)))
    return _drive_request("PATCH", url, {"name": new_name})


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def md5_file(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def drive_download(file_id, dest_path):
    token = _sa_bearer()
    url = ("https://www.googleapis.com/drive/v3/files/%s?alt=media"
           "&supportsAllDrives=true" % file_id)
    req = urllib.request.Request(url)
    req.add_header("Authorization", "Bearer " + token)
    with urllib.request.urlopen(req, timeout=120) as resp:
        blob = resp.read()
    with open(dest_path, "wb") as f:
        f.write(blob)
    return dest_path


# ---------------------------------------------------------------------------
# LLM via `openclaw infer` (never call OpenAI/DeepSeek directly)
# ---------------------------------------------------------------------------
# The openclaw CLI is resolved by ABSOLUTE PATH; we never trust $PATH. Cron jobs
# and `sh -lc` login shells routinely run with a stripped or profile-overwritten
# PATH - the classic "works in a terminal, fails under cron" trap (the same
# reason m12_recv_ingest resolves the poppler binaries explicitly). Order:
# OPENCLAW_BIN env override, the known npm-global install on this Gateway, then a
# best-effort PATH lookup as a last resort.
def _resolve_openclaw():
    cand = (os.environ.get("OPENCLAW_BIN") or "").strip()
    if cand and os.path.exists(cand):
        return cand
    known = "/data/.npm-global/bin/openclaw"
    if os.path.exists(known):
        return known
    from shutil import which
    return which("openclaw") or "openclaw"


OPENCLAW_BIN = _resolve_openclaw()


def _run(cmd):
    log("run: " + " ".join(cmd[:6]) + (" ..." if len(cmd) > 6 else ""))
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return p.returncode, p.stdout.decode("utf-8", "replace"), p.stderr.decode("utf-8", "replace")


def _extract_model_json(stdout):
    # stdout is JSON from openclaw: outputs[0]["text"] holds the model text,
    # from which we pull the first {...} block (robust parse, like m3).
    try:
        env = json.loads(stdout)
        txt = env["outputs"][0]["text"]
    except Exception:
        txt = stdout
    m = re.search(r"\{.*\}", txt, re.S)
    if not m:
        raise RuntimeError("no JSON object in model output")
    return json.loads(m.group(0))


def infer_text(prompt):
    cmd = [OPENCLAW_BIN, "infer", "model", "run", "--prompt", prompt,
           "--model", LLM_TEXT, "--json"]
    rc, out, err = _run(cmd)
    if rc != 0:
        log("text LLM primary failed (%s), fallback. stderr: %s" % (LLM_TEXT, err[:300]))
        cmd = [OPENCLAW_BIN, "infer", "model", "run", "--prompt", prompt,
               "--model", LLM_TEXT_FALLBACK, "--json"]
        rc, out, err = _run(cmd)
        if rc != 0:
            raise RuntimeError("text LLM failed: " + err[:300])
    return _extract_model_json(out)


def infer_vision(prompt, file_path):
    cmd = [OPENCLAW_BIN, "infer", "model", "run", "--model", LLM_VISION,
           "--file", file_path, "--prompt", prompt, "--json"]
    rc, out, err = _run(cmd)
    if rc != 0:
        raise RuntimeError("vision LLM failed: " + err[:300])
    return _extract_model_json(out)


# ---------------------------------------------------------------------------
# Telegram (Admin sink) via `openclaw message send`
# ---------------------------------------------------------------------------
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
# Noise lines emitted by the openclaw CLI that must not leak into alerts.
_NOISE_SUBSTR = ("legacy state migration", "state migration", "migrating state")


def clean_msg(s):
    # Strip ANSI colour codes and drop openclaw warning noise; keep the essence.
    s = _ANSI_RE.sub("", s or "")
    keep = []
    for ln in s.splitlines():
        low = ln.strip().lower()
        if not low:
            continue
        if any(n in low for n in _NOISE_SUBSTR):
            continue
        keep.append(ln.strip())
    return " ".join(keep).strip()[:400]


def tg(message):
    msg = clean_msg(message) or "(pusty komunikat)"
    cmd = [OPENCLAW_BIN, "message", "send", "--channel", "telegram",
           "--target", TG_TARGET, "--thread-id", TG_THREAD, "--message", msg]
    rc, out, err = _run(cmd)
    if rc != 0:
        log("telegram send failed: " + err[:300])
    return rc == 0


def tg_long(message):
    # Send a possibly long text to Telegram in <=3900-char chunks (Telegram caps a
    # message at 4096). clean_msg() flattens to one line, so for multi-line dry
    # summaries we DO NOT clean - we send the raw text split on line boundaries.
    text = (message or "").strip() or "(pusty komunikat)"
    text = _ANSI_RE.sub("", text)
    chunks = []
    buf = ""
    for ln in text.splitlines():
        if len(buf) + len(ln) + 1 > 3900:
            chunks.append(buf)
            buf = ""
        buf += ln + "\n"
    if buf.strip():
        chunks.append(buf)
    ok = True
    for c in (chunks or [text[:3900]]):
        cmd = [OPENCLAW_BIN, "message", "send", "--channel", "telegram",
               "--target", TG_TARGET, "--thread-id", TG_THREAD, "--message", c]
        r, _out, err = _run(cmd)
        if r != 0:
            log("telegram long send failed: " + err[:300])
            ok = False
    return ok


def _tg_bot_token():
    # Raw Telegram Bot API token for sendDocument (brief batch3 sec.8). openclaw
    # 2026.6.5 has no --file for message send, so we call the Bot API directly.
    # Order: env override, then a few likely secrets files/keys. "" if not found.
    cand = (os.environ.get("M12_TG_BOT_TOKEN") or "").strip()
    if cand:
        return cand
    for fname in ("telegram.json", "tg.json", "telegram_bot.json", "openclaw.json"):
        path = os.path.join(SECRETS_DIR, fname)
        if not os.path.exists(path):
            continue
        try:
            with open(path, "r") as f:
                data = json.load(f)
        except Exception:
            continue
        for key in ("bot_token", "token", "telegram_bot_token", "tg_bot_token"):
            v = (data.get(key) if isinstance(data, dict) else None) or ""
            if isinstance(v, str) and v.strip():
                return v.strip()
    return ""


def _multipart_body(fields, file_field, filename, file_bytes, content_type):
    # Build a minimal multipart/form-data body (no external deps). ASCII boundary.
    boundary = "----m12recv%d" % (os.getpid())
    crlf = b"\r\n"
    out = []
    for k, v in fields.items():
        out.append(("--" + boundary).encode())
        out.append(('Content-Disposition: form-data; name="%s"' % k).encode())
        out.append(b"")
        out.append(str(v).encode("utf-8"))
    out.append(("--" + boundary).encode())
    out.append(('Content-Disposition: form-data; name="%s"; filename="%s"'
                % (file_field, filename)).encode())
    out.append(("Content-Type: %s" % content_type).encode())
    out.append(b"")
    out.append(file_bytes)
    out.append(("--" + boundary + "--").encode())
    out.append(b"")
    return crlf.join(out), boundary


def tg_document(file_path, caption=""):
    # Attach a file to Telegram as a real document (sendDocument), used for the
    # FULL stockup JSON body (routinely > 4096 chars). Direct Bot API multipart
    # when a token is available; otherwise inline fallback via tg_long().
    token = _tg_bot_token()
    if token:
        try:
            with open(file_path, "rb") as f:
                blob = f.read()
            fields = {"chat_id": TG_TARGET}
            if TG_THREAD:
                fields["message_thread_id"] = TG_THREAD
            if caption:
                fields["caption"] = clean_msg(caption)[:1024]
            body, boundary = _multipart_body(
                fields, "document", os.path.basename(file_path), blob,
                "application/json")
            url = "https://api.telegram.org/bot%s/sendDocument" % token
            req = urllib.request.Request(url, data=body, method="POST")
            req.add_header("Content-Type",
                           "multipart/form-data; boundary=%s" % boundary)
            with urllib.request.urlopen(req, timeout=60) as resp:
                ok = 200 <= resp.status < 300
            if ok:
                return True
            log("telegram sendDocument non-2xx -> inline fallback")
        except Exception as e:
            log("telegram sendDocument failed (%s) -> inline fallback" % str(e)[:200])
    else:
        log("telegram: no bot token (set M12_TG_BOT_TOKEN) -> inline fallback")
    try:
        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            body_txt = f.read()
        return tg_long((caption + "\n" if caption else "") + body_txt)
    except Exception as e:
        log("telegram inline fallback failed: %s" % e)
        return False


# ---------------------------------------------------------------------------
# NBP FX (public API, no key) - last working day BEFORE the document date.
# ---------------------------------------------------------------------------
def nbp_rate(currency, doc_date):
    # doc_date "YYYY-MM-DD". Returns (rate_to_pln, rate_date) or (None, None).
    cur = (currency or "").strip().upper()
    if cur in ("", "PLN"):
        return None, None
    # NBP tables/{table}/{code}/last/{n} gives recent rates; we pick the last
    # published rate on or before the day before doc_date.
    url = "https://api.nbp.pl/api/exchangerates/rates/A/%s/last/10/?format=json" % cur
    try:
        req = urllib.request.Request(url)
        req.add_header("Accept", "application/json")
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
        rates = data.get("rates", [])
    except Exception as e:
        log("NBP fetch failed for %s: %s" % (cur, e))
        return None, None
    # choose the newest effectiveDate strictly before doc_date
    best = None
    for r in rates:
        d = r.get("effectiveDate", "")
        if d and d < doc_date:
            if best is None or d > best.get("effectiveDate", ""):
                best = r
    if best is None and rates:
        best = rates[-1]
    if best is None:
        return None, None
    return best.get("mid"), best.get("effectiveDate")


# ---------------------------------------------------------------------------
# small utils
# ---------------------------------------------------------------------------
def to_float(v):
    s = str(v or "").strip().replace(",", ".")
    if s == "":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def is_true(v):
    return str(v or "").strip().lower() in ("true", "prawda", "1", "yes", "tak")


def mem_norm_name(s):
    # SKU-memory name normalization (brief batch2 sec.2.3): trim, lower, collapse
    # runs of whitespace. Deliberately simpler than match norm_name (which strips
    # punctuation) so the memory key stays close to the printed name.
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def mem_key(supplier_nip, raw_name, raw_supplier_code=""):
    # Memory lookup key (brief batch2 sec.2.3). A supplier article code / EAN, when
    # present, is more reliable than the name and takes priority. Otherwise
    # supplier_nip + normalized name. Independent of document type (WZ or faktura).
    nip = re.sub(r"\D", "", supplier_nip or "")
    code = (raw_supplier_code or "").strip().lower()
    if code:
        return "%s|code|%s" % (nip, code)
    return "%s|name|%s" % (nip, mem_norm_name(raw_name))


def csv_header_signature(header_cells):
    # Stable signature of a CSV header row for Recv_CsvTemplates (brief batch2
    # sec.2.4): lowercased, trimmed, order-preserving, joined, hashed.
    cells = [re.sub(r"\s+", " ", (c or "").strip().lower()) for c in (header_cells or [])]
    basis = "|".join(cells)
    return hashlib.sha1(basis.encode("utf-8")).hexdigest()[:16]


def load_cursor(name):
    path = os.path.join(SECRETS_DIR, name)
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_cursor(name, obj, dry=False):
    path = os.path.join(SECRETS_DIR, name)
    if dry:
        log("dry: would save cursor %s (%d keys)" % (name, len(obj)))
        return
    backup_file(path)
    with open(path, "w") as f:
        json.dump(obj, f)
