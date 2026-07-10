#!/usr/bin/env python3
# -*- coding: ascii -*-
# MOSS M12 recv - ingest: parse -> match -> draft. Writes ONLY to Google Sheets
# (Recv_Docs / Recv_Lines). Never writes to Dotypos. Reads the Recv_Catalog tab
# for matching (not the Dotypos API), per brief sec.8.
#
# Pipeline (brief sec.5 ingest):
#   1. scan Drive Moss_WZ_Inbox for new files -> Recv_Docs(status=parsing);
#      also pick up app uploads with status=new
#   2. parse by source: image/photo -> vision; pdf -> text layer (pdftotext) or
#      vision; ksef_xml -> deterministic XML; manual/paragon -> skip parse
#   3. header: supplier by NIP, number, date, currency, is_foreign_wnt
#   4. dedup_key -> duplicate
#   5. match lines: Recv_Dictionary, then LLM vs Recv_Catalog, threshold 0.75
#   6. normalize canonical_qty (+ unit_flag guard)
#   7. FX for non-PLN via NBP (last working day before doc_date)
#   8. write Recv_Lines; doc -> needs_review
#
# Use --dry to parse and print WITHOUT writing to Sheets.
#   python3 m12_recv_ingest.py --dry
#   python3 m12_recv_ingest.py --dry --doc <doc_id>
#   python3 m12_recv_ingest.py            # live (writes Sheets only)

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import time
import uuid
import xml.etree.ElementTree as ET

import recv_common as rc

CURSOR = "m12_recv_ingest_cursor.json"

IMAGE_MIMES = ("image/jpeg", "image/png", "image/webp", "image/heic", "image/heif")

# Match confidence bands (brief update): auto-accept only >= GREEN; the band
# [REVIEW, GREEN) is a suggestion the manager must check; below REVIEW is
# unmatched. Do not collapse everything to 0.75 (that was the old bug).
GREEN = 0.85
REVIEW = 0.70

# --if-requested honors a scan request only if it is newer than the last one we
# claimed AND recent enough. The app writes epoch-ms tokens (Date.now()), which
# are timezone-independent, so this stays correct regardless of the container's
# timezone. Overridable via env for tuning without a redeploy.
SCAN_FRESH_SECONDS = int(os.environ.get("M12_SCAN_FRESH_SECONDS", "3600") or "3600")


def now_ts():
    return time.strftime("%Y-%m-%d %H:%M:%S")


def norm_name(s):
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


# ---------------------------------------------------------------------------
# parsing by source
# ---------------------------------------------------------------------------
def load_prompts():
    # The prompt file holds two blocks. The markers [[PARSE]] and [[MATCH]] must
    # each sit ALONE on their own line (a line-anchored match) so that any
    # mention of the tokens in prose does not split the file.
    with open(rc.PROMPT_FILE, "r") as f:
        txt = f.read()
    parse_p = _section(txt, "PARSE", "MATCH")
    match_p = _section(txt, "MATCH", None)
    return parse_p.strip(), match_p.strip()


def _section(txt, start, end):
    # Find a marker line that is exactly "[[START]]" and return text up to the
    # next such marker line (or the "[[END]]" marker line / EOF).
    sm = re.search(r"(?m)^\[\[%s\]\]\s*$" % start, txt)
    if not sm:
        return ""
    i = sm.end()
    if end:
        em = re.search(r"(?m)^\[\[%s\]\]\s*$" % end, txt[i:])
        j = i + em.start() if em else len(txt)
    else:
        j = len(txt)
    return txt[i:j]


# poppler binaries may live under a Homebrew prefix that is not on PATH for the
# cron/python process; resolve them explicitly.
BREW_BINDIRS = [
    "/home/linuxbrew/.linuxbrew/bin",
    "/opt/homebrew/bin",
    "/usr/local/bin",
]


def _which(name):
    from shutil import which
    p = which(name)
    if p:
        return p
    for d in BREW_BINDIRS:
        cand = os.path.join(d, name)
        if os.path.exists(cand):
            return cand
    return ""


def pdf_extract_text(path):
    # Try, in order: PyMuPDF (pip, no system pkg), poppler pdftotext, pypdf.
    try:
        import fitz  # PyMuPDF
        doc = fitz.open(path)
        txt = "\n".join(pg.get_text() for pg in doc)
        doc.close()
        if txt.strip():
            rc.log("pdf text via PyMuPDF (%d chars)" % len(txt))
            return txt
    except Exception as e:
        rc.log("PyMuPDF text not available: %s" % e)
    exe = _which("pdftotext")
    if exe:
        try:
            p = subprocess.run([exe, "-layout", path, "-"],
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if p.returncode == 0 and p.stdout.strip():
                return p.stdout.decode("utf-8", "replace")
        except Exception as e:
            rc.log("pdftotext failed: %s" % e)
    try:
        from pypdf import PdfReader
        r = PdfReader(path)
        txt = "\n".join((pg.extract_text() or "") for pg in r.pages)
        if txt.strip():
            rc.log("pdf text via pypdf (%d chars)" % len(txt))
            return txt
    except Exception as e:
        rc.log("pypdf text not available: %s" % e)
    return ""


def pdf_render_png(path):
    # Render page 1 to PNG for vision. Try PyMuPDF first (no system pkg), then
    # poppler pdftoppm/pdftocairo. Returns PNG path or "".
    out = path.rsplit(".", 1)[0] + "_p1"
    png = out + ".png"
    try:
        import fitz  # PyMuPDF
        doc = fitz.open(path)
        n = doc.page_count
        pix = doc.load_page(0).get_pixmap(dpi=200)
        pix.save(png)
        doc.close()
        if n > 1:
            rc.log("pdf has %d pages; rendered page 1 via PyMuPDF" % n)
        if os.path.exists(png):
            return png
    except Exception as e:
        rc.log("PyMuPDF render not available: %s" % e)
    for name in ("pdftoppm", "pdftocairo"):
        exe = _which(name)
        if not exe:
            continue
        try:
            p = subprocess.run([exe, "-png", "-singlefile", "-r", "200", path, out],
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if p.returncode == 0 and os.path.exists(png):
                return png
            rc.log("pdf render %s rc=%s" % (name, p.returncode))
        except Exception as e:
            rc.log("pdf render %s failed: %s" % (name, e))
    return ""


def parse_ksef_xml(path):
    # Deterministic FA(2)/FA(3) parse. Namespace-agnostic (tags matched by
    # local name). VERIFY against a real KSeF file before trusting field map.
    def local(tag):
        return tag.rsplit("}", 1)[-1]

    tree = ET.parse(path)
    root = tree.getroot()
    idx = {}
    for el in root.iter():
        idx.setdefault(local(el.tag), []).append(el)

    def first_text(name):
        els = idx.get(name)
        return (els[0].text or "").strip() if els else ""

    header = {
        "supplier_name": first_text("Nazwa") or first_text("PelnaNazwa"),
        "supplier_nip": re.sub(r"\D", "", first_text("NIP")),
        "doc_number": first_text("P_2"),
        "doc_date": first_text("P_1")[:10],
        "currency": first_text("KodWaluty") or "PLN",
        "is_foreign": (first_text("KodWaluty") or "PLN") != "PLN",
        "lines": [],
    }
    for w in idx.get("FaWiersz", []):
        row = {}
        for child in w.iter():
            row[local(child.tag)] = (child.text or "").strip()
        header["lines"].append({
            "raw_name": row.get("P_7", ""),
            "raw_supplier_code": row.get("P_6A", ""),
            "raw_qty": row.get("P_8B", ""),
            "raw_unit": row.get("P_8A", ""),
            "raw_unit_price": row.get("P_9A", ""),
            "raw_line_total": row.get("P_11", ""),
            "vat_rate": row.get("P_12", ""),
        })
    return header


def parse_document(source, local_path, prompt):
    if source in ("image", "wz_photo"):
        return rc.infer_vision(prompt, local_path)
    if source == "ksef_xml":
        return parse_ksef_xml(local_path)
    if source == "pdf":
        txt = pdf_extract_text(local_path)
        if txt.strip():
            return rc.infer_text(prompt + "\n\n=== DOCUMENT TEXT ===\n" + txt)
        rc.log("pdf: no text layer, rendering page to image for vision")
        png = pdf_render_png(local_path)
        if not png:
            raise RuntimeError(
                "PDF bez warstwy tekstowej; brak PyMuPDF/poppler do renderu strony")
        try:
            return rc.infer_vision(prompt, png)
        finally:
            try:
                os.remove(png)
            except Exception:
                pass
    return None  # manual / paragon: lines already present


# ---------------------------------------------------------------------------
# matching
# ---------------------------------------------------------------------------
def build_dict_index(dict_rows):
    idx = {}
    for r in dict_rows:
        key = (r.get("key") or "").strip().lower()
        if not key:
            continue
        idx[key] = {
            "match_productId": r.get("match_productId", ""),
            "canonical_unit": r.get("canonical_unit", ""),
            "pack_factor": rc.to_float(r.get("pack_factor")) or 1.0,
            "confidence": rc.to_float(r.get("confidence")) or 0.9,
            "domain": r.get("domain", ""),
        }
    return idx


def dict_match(line, supplier_id, dict_idx):
    code = (line.get("raw_supplier_code") or "").strip()
    keys = []
    if supplier_id and code:
        keys.append(("%s|%s" % (supplier_id, code)).lower())
    keys.append(norm_name(line.get("raw_name")))
    for k in keys:
        if k in dict_idx:
            return dict_idx[k]
    return None


def llm_match_batch(lines, catalog, match_prompt):
    # One LLM call: map raw line names to catalog productIds using the MATCH
    # prompt (confidence bands + type-mismatch penalty live in that prompt).
    # Returns {line_index: {"productId":..., "confidence":...}}. Empty on failure.
    if not lines or not catalog:
        return {}
    cat = catalog[:800]
    cat_lines = "\n".join(
        "%s\t%s\t%s" % (c.get("productId", ""), c.get("name", ""), c.get("domain", ""))
        for c in cat
    )
    items = "\n".join("%d\t%s" % (i, (l.get("raw_name") or "")) for i, l in enumerate(lines))
    prompt = (
        match_prompt
        + "\n\nCATALOG (productId<TAB>name<TAB>domain):\n" + cat_lines
        + "\n\nLINES (index<TAB>raw_name):\n" + items + "\n"
    )
    try:
        obj = rc.infer_text(prompt)
    except Exception as e:
        rc.log("llm match failed: %s" % e)
        return {}
    out = {}
    for m in obj.get("matches", []):
        try:
            idx = int(m["index"])
        except Exception:
            continue
        cands = []
        # New shape: candidates[]; tolerate the old single productId shape too.
        raw_cands = m.get("candidates")
        if raw_cands is None and m.get("productId") is not None:
            raw_cands = [{"productId": m.get("productId"), "confidence": m.get("confidence", 0.0)}]
        for c in (raw_cands or []):
            pid = str(c.get("productId", "") or "")
            try:
                conf = float(c.get("confidence", 0.0))
            except Exception:
                conf = 0.0
            if pid:
                cands.append({"productId": pid, "confidence": conf})
        cands.sort(key=lambda x: x["confidence"], reverse=True)
        out[idx] = cands[:3]
    return out


# ---------------------------------------------------------------------------
# normalization / fx
# ---------------------------------------------------------------------------
def _fmt(x):
    return ("%g" % x) if x is not None else ""


# Unit normalization: map a unit string to (dimension, factor-to-base).
# base of mass = kg, base of volume = l, base of count = szt.
_UNIT_MAP = {
    # mass
    "kg": ("mass", 1.0), "kilogram": ("mass", 1.0), "kilogramy": ("mass", 1.0),
    "dag": ("mass", 0.01), "dekagram": ("mass", 0.01),
    "g": ("mass", 0.001), "gram": ("mass", 0.001), "gramy": ("mass", 0.001), "gr": ("mass", 0.001),
    "mg": ("mass", 0.000001),
    # volume
    "l": ("vol", 1.0), "litr": ("vol", 1.0), "litry": ("vol", 1.0), "liter": ("vol", 1.0),
    "dl": ("vol", 0.1), "cl": ("vol", 0.01),
    "ml": ("vol", 0.001), "mililitr": ("vol", 0.001),
    # count
    "szt": ("count", 1.0), "szt.": ("count", 1.0), "sztuka": ("count", 1.0),
    "sztuki": ("count", 1.0), "pcs": ("count", 1.0), "pc": ("count", 1.0),
    "opak": ("count", 1.0), "opakowanie": ("count", 1.0), "op": ("count", 1.0),
}


def normalize_unit(u):
    key = (u or "").strip().lower().rstrip(".")
    if key in _UNIT_MAP:
        return _UNIT_MAP[key]
    # also try without trailing '.' variant present in map
    if (key + ".") in _UNIT_MAP:
        return _UNIT_MAP[key + "."]
    return (None, None)


def compute_qty(line, dict_packf, target_unit):
    # Quantity in the MATCHED INGREDIENT's unit (target_unit from Recv_Catalog).
    # Converts across the same dimension (g<->kg, ml<->l). If the source cannot
    # be converted to the ingredient unit (e.g. pieces to kg with no weight),
    # we DO NOT guess: unit_flag + empty qty ("przelicz recznie").
    # Returns (canonical_qty, canonical_unit, qty_breakdown, unit_flag).
    count = rc.to_float(line.get("pack_count"))
    size = rc.to_float(line.get("pack_size"))
    raw_qty = rc.to_float(line.get("raw_qty"))
    punit = (line.get("pack_unit") or "").strip()
    runit = (line.get("raw_unit") or "").strip()
    tdim, tfac = normalize_unit(target_unit)
    tunit = (target_unit or "").strip()

    def to_target(amount, unit):
        # amount in `unit` -> value in target unit, or (None,None) if it cannot
        # be converted to the ingredient's dimension.
        if amount is None:
            return None, None
        sdim, sfac = normalize_unit(unit)
        if tdim is None:
            # unknown ingredient unit -> keep source unit as-is (best effort)
            return amount, (unit or tunit or "")
        if sdim == tdim and sfac is not None:
            return amount * sfac / tfac, tunit
        return None, None  # dimension mismatch

    def flag_manual():
        hint = " (jednostka skladnika: %s)" % tunit if tunit else ""
        return "", (tunit or punit or runit or ""), "przelicz recznie" + hint, True

    # 1) clean pack decomposition: count x size in pack_unit (or raw unit)
    if count and size and count > 0 and size > 0:
        src_unit = punit or runit
        val, u = to_target(count * size, src_unit)
        if val is None:
            return flag_manual()
        bd = "%s x %s %s = %s %s" % (_fmt(count), _fmt(size), src_unit or "", _fmt(val), u)
        return _fmt(val), u, bd, bool(val >= 100000)

    # 2) learned pack factor from the dictionary (assumed already in target unit)
    if dict_packf and dict_packf != 1.0 and raw_qty is not None:
        canon = raw_qty * dict_packf
        bd = "%s x %s = %s %s" % (_fmt(raw_qty), _fmt(dict_packf), _fmt(canon), tunit)
        return _fmt(canon), (tunit or runit or ""), bd, bool(canon >= 100000 or dict_packf >= 1000)

    # 3) plain quantity in the raw unit -> convert to ingredient unit
    name = line.get("raw_name", "")
    hint = re.search(r"(/\s*\d+|\d+\s*[x/]\s*\d+|\bx\s*\d+)", name, re.I)
    if raw_qty is not None and not hint:
        val, u = to_target(raw_qty, runit)
        if val is not None:
            bd = "" if (u == runit or not runit) else "%s %s = %s %s" % (_fmt(raw_qty), runit, _fmt(val), u)
            return _fmt(val), u, bd, bool(val >= 100000)
        if tdim is not None:
            # raw unit incompatible with ingredient unit (e.g. szt vs kg) -> flag
            return flag_manual()
        return _fmt(raw_qty), (runit or ""), "", bool(raw_qty >= 100000)

    # 4) not trustworthy -> flag, NO auto value
    return flag_manual()


def normalize_line(line, packf, canonical_unit):
    raw_qty = rc.to_float(line.get("raw_qty"))
    if raw_qty is None:
        return "", False
    canon = raw_qty * (packf or 1.0)
    # unit guard: implausible blow-up (e.g. x1000 mistakes) -> flag
    flag = canon >= 100000 or (packf and (packf >= 1000 or packf <= 0.0001))
    return ("%g" % canon), bool(flag)


# ---------------------------------------------------------------------------
# --if-requested trigger: claim a fresh scan request from Recv_Control
# ---------------------------------------------------------------------------
def _stamp_control(row_num, patch):
    # Write path (rare): runs only when there is a fresh/stale request to stamp.
    # Kept out of the idle path so the every-minute tick stays read-only.
    ws = rc.open_ws(rc.TAB_CONTROL, rc.CONTROL_HEADERS)
    for k, v in patch.items():
        rc.set_cell(ws, rc.CONTROL_HEADERS, row_num, k, v)


def claim_scan_request(dry=False):
    # Read ONLY the single Recv_Control row via one cheap values.get (no heavy
    # tabs, no metadata). Return the request token to run on, or None when there
    # is nothing fresh to do. On a live claim we stamp scan_done FIRST so the
    # next 1-minute tick will not re-run the same request. The idle path (the
    # common every-minute case) is exactly ONE Sheets read and nothing else.
    row, row_num = rc.control_row()
    if not row:
        return None
    req = str(row.get("scan_request") or "").strip()
    done = str(row.get("scan_done") or "").strip()
    if not req or req == done:
        return None  # nothing new since we last ran -> one read, done
    try:
        req_ms = int(float(req))
    except (TypeError, ValueError):
        req_ms = 0
    now_ms = int(time.time() * 1000)
    if req_ms and (now_ms - req_ms) > SCAN_FRESH_SECONDS * 1000:
        # Stale request (e.g. the bot was down for a long time): do not surprise
        # the manager with an old scan. Clear it so we stop re-checking.
        rc.log("if-requested: request %s is stale (%ds old), ignored"
               % (req, (now_ms - req_ms) // 1000))
        if not dry:
            _stamp_control(row_num, {"scan_done": req,
                                     "note": "stale request ignored",
                                     "updated_at": now_ts()})
        return None
    if dry:
        rc.log("if-requested (dry): would claim scan request %s" % req)
        return req
    _stamp_control(row_num, {"scan_done": req,
                             "note": "scan started " + now_ts(),
                             "updated_at": now_ts()})
    return req


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry", action="store_true", help="parse and print, no Sheets writes")
    ap.add_argument("--doc", default="", help="process only this doc_id")
    ap.add_argument("--limit", type=int, default=25, help="max docs per run")
    ap.add_argument("--force", action="store_true",
                    help="reprocess regardless of status (skips posted/posting)")
    ap.add_argument("--redo", default="",
                    help="reprocess one doc_id for re-testing (implies --doc + --force)")
    ap.add_argument("--if-requested", action="store_true",
                    help="light trigger (1-min cron): run only when the app set a "
                         "fresh Recv_Control.scan_request; otherwise exit at once")
    args = ap.parse_args()

    if args.redo:
        args.doc = args.redo
        args.force = True

    # Light trigger for the "Rozpoznaj" button: bail out immediately unless the
    # app queued a fresh request. Checked BEFORE loading prompts / opening the
    # data tabs / scanning Drive, so an idle every-minute run costs one tiny read.
    if args.if_requested:
        token = claim_scan_request(args.dry)
        if not token:
            rc.log("if-requested: no fresh scan request; nothing to do")
            return 0
        rc.log("if-requested: fresh scan request %s -> running ingest" % token)

    parse_prompt, match_prompt = load_prompts()
    if not parse_prompt:
        rc.log("FATAL: empty PARSE prompt - check the [[PARSE]] marker in %s"
               % rc.PROMPT_FILE)
        rc.tg("M12 ingest: pusty PARSE prompt - sprawdz plik promptu")
        return 1
    if args.force and not args.dry:
        rc.log("WARNING: --force without --dry may append duplicate lines for "
               "documents that already have rows")

    docs_ws = rc.open_ws(rc.TAB_DOCS)
    lines_ws = rc.open_ws(rc.TAB_LINES)
    doc_headers, doc_rows = rc.read_records(docs_ws)
    line_headers, _ = rc.read_records(lines_ws)
    _, dict_rows = rc.read_records(rc.open_ws(rc.TAB_DICT))
    _, sup_rows = rc.read_records(rc.open_ws(rc.TAB_SUPPLIERS))
    _, cat_rows = rc.read_records(rc.open_ws(rc.TAB_CATALOG))

    dict_idx = build_dict_index(dict_rows)
    sup_by_nip = {re.sub(r"\D", "", (s.get("nip") or "")): s for s in sup_rows if s.get("nip")}

    known_file_ids = set((d.get("drive_file_id") or "").strip() for d in doc_rows)
    known_dedup = set((d.get("dedup_key") or "").strip() for d in doc_rows if d.get("dedup_key"))

    # --- step 1: scan Drive for new files -> candidate docs ----------------
    # The scan always runs so that --redo/--doc can also target a file that is
    # not yet a Recv_Docs row (a fresh Drive drop). New rows are only persisted
    # on a normal run (not when redoing a single doc).
    new_doc_rows = []
    try:
        files = rc.drive_list()
    except Exception as e:
        rc.log("drive_list failed: %s" % e)
        files = []
    for f in files:
        fid = f.get("id", "")
        if not fid or fid in known_file_ids:
            continue
        mime = f.get("mimeType", "")
        if mime == "application/pdf":
            source = "pdf"
        elif "xml" in mime:
            source = "ksef_xml"
        elif mime in IMAGE_MIMES:
            source = "wz_photo"
        else:
            rc.log("skip unsupported file %s (%s)" % (f.get("name"), mime))
            continue
        rec = {
            "doc_id": str(uuid.uuid4()),
            "created_at": now_ts(),
            "updated_at": now_ts(),
            "created_by": "bot",
            "source": source,
            "status": "parsing",
            "drive_file_id": fid,
            "drive_file_link": "https://drive.google.com/file/d/%s/view" % fid,
            "currency": "PLN",
            "_name": f.get("name", ""),  # local only, for --redo matching
        }
        new_doc_rows.append(rec)
        known_file_ids.add(fid)
    rc.log("scan: %d new file(s) in inbox" % len(new_doc_rows))
    if new_doc_rows and not args.dry and not args.doc:
        rc.append_rows(docs_ws, doc_headers, new_doc_rows)
        doc_headers, doc_rows = rc.read_records(docs_ws)
        persisted = True
    else:
        persisted = False

    # Pool: persisted docs, plus in-memory new docs when they were not written.
    pool = list(doc_rows)
    if not persisted:
        pool = new_doc_rows + pool

    # --- choose docs to parse ---------------------------------------------
    todo = []
    if args.doc:
        # --redo/--doc: match by doc_id (exact or prefix), drive_file_id, or a
        # filename fragment - regardless of status. This is the force path.
        target = args.doc.strip()
        tlow = target.lower()
        for d in pool:
            did = (d.get("doc_id") or "").strip()
            fid = (d.get("drive_file_id") or "").strip()
            nm = (d.get("_name") or "").lower()
            if (did == target or fid == target
                    or (len(target) >= 6 and did.startswith(target))
                    or (len(tlow) >= 4 and tlow in nm)):
                todo.append(d)
        if todo:
            for d in todo:
                rc.log("redo: selected doc_id=%s status=%s source=%s file=%s"
                       % (d.get("doc_id"), d.get("status"), d.get("source"),
                          d.get("_name") or d.get("drive_file_id") or ""))
        else:
            rc.log("redo: '%s' NOT found (checked %d docs + %d inbox files)."
                   % (target, len(doc_rows), len(new_doc_rows)))
            rc.log("redo: available (doc_id | status | source | supplier):")
            for d in doc_rows[:25]:
                rc.log("  %s | %s | %s | %s"
                       % (d.get("doc_id", ""), d.get("status", ""), d.get("source", ""),
                          d.get("supplier_name_raw") or d.get("supplier_id") or ""))
    else:
        for d in pool:
            st = (d.get("status") or "").strip()
            if args.force:
                if st in ("posted", "posting"):
                    continue
                todo.append(d)
            elif st in ("parsing", "new"):
                todo.append(d)
    todo = todo[:args.limit]
    rc.log("parse: %d document(s) to process" % len(todo))

    for d in todo:
        try:
            process_doc(d, parse_prompt, match_prompt, dict_idx, sup_by_nip, cat_rows,
                        docs_ws, doc_headers, lines_ws, line_headers,
                        known_dedup, args.dry)
        except Exception as e:
            rc.log("doc %s FAILED: %s" % (d.get("doc_id"), e))
            if not args.dry and d.get("_row"):
                try:
                    rc.set_cell(docs_ws, doc_headers, d["_row"], "status", "error")
                    rc.set_cell(docs_ws, doc_headers, d["_row"], "error_msg", str(e)[:300])
                except Exception:
                    pass
            rc.tg("M12 ingest blad doc %s: %s" % (d.get("doc_id"), str(e)[:200]))

    return 0


def process_doc(d, parse_prompt, match_prompt, dict_idx, sup_by_nip, cat_rows,
                docs_ws, doc_headers, lines_ws, line_headers,
                known_dedup, dry):
    doc_id = d.get("doc_id")
    source = (d.get("source") or "").strip()
    rc.log("--- doc %s source=%s ---" % (doc_id, source))

    parsed = None
    if source in ("manual", "paragon"):
        rc.log("manual/paragon: lines already present, skip parse")
    else:
        fid = (d.get("drive_file_id") or "").strip()
        if not fid:
            raise RuntimeError("no drive_file_id to parse")
        ext = {"pdf": ".pdf", "ksef_xml": ".xml"}.get(source, ".bin")
        tmp = "/tmp/m12_recv_%s%s" % (doc_id, ext)
        rc.drive_download(fid, tmp)
        parsed = parse_document(source, tmp, parse_prompt)
        try:
            os.remove(tmp)
        except Exception:
            pass

    if parsed is None:
        # manual/paragon: just move to needs_review
        _set_doc(docs_ws, doc_headers, d, {"status": "needs_review", "updated_at": now_ts()}, dry)
        return

    lines = parsed.get("lines", []) or []
    # Never a silent zero: if the parser returned nothing usable, say why.
    parser_note = ""
    if not lines:
        header_empty = not any(parsed.get(k) for k in
                               ("supplier_name", "doc_number", "doc_date"))
        parser_note = ("parser: 0 pozycji%s - sprawdz jakosc zdjecia/skanu"
                       % (" i pusta glowka" if header_empty else ""))
        rc.log("doc %s: %s | parsed header: supplier=%r nr=%r date=%r cur=%r"
               % (doc_id, parser_note, parsed.get("supplier_name", ""),
                  parsed.get("doc_number", ""), parsed.get("doc_date", ""),
                  parsed.get("currency", "")))
        rc.tg("M12 ingest: dokument %s - %s" % (doc_id, parser_note))

    supplier_nip = re.sub(r"\D", "", parsed.get("supplier_nip", ""))
    sup = sup_by_nip.get(supplier_nip)
    supplier_id = (sup.get("supplier_id") if sup else "") or (d.get("supplier_id") or "")
    currency = (parsed.get("currency") or d.get("currency") or "PLN").strip().upper()
    doc_date = (parsed.get("doc_date") or "").strip()
    is_foreign = bool(parsed.get("is_foreign")) or (sup and rc.is_true(sup.get("is_foreign_wnt")))

    # dedup
    basis = "%s|%s|%s|%s" % (
        supplier_id or parsed.get("supplier_name", ""),
        parsed.get("doc_number", ""),
        doc_date,
        hashlib.sha1(json.dumps(lines, sort_keys=True).encode("utf-8")).hexdigest()[:12],
    )
    dedup_key = hashlib.sha1(basis.encode("utf-8")).hexdigest()[:16]
    status = "duplicate" if dedup_key in known_dedup else "needs_review"
    known_dedup.add(dedup_key)

    # fx
    fx_rate, fx_date = (None, None)
    if currency != "PLN" and doc_date:
        fx_rate, fx_date = rc.nbp_rate(currency, doc_date)

    # match + normalize
    catalog = cat_rows
    cat_by_id = {c.get("productId"): c for c in catalog}

    def alt_json(cands):
        # Serialize up to 2 alternative candidates (beyond the primary) with the
        # catalog name/category so the app can offer one-tap swaps.
        out = []
        for c in cands:
            cat = cat_by_id.get(c["productId"], {})
            out.append({
                "productId": c["productId"],
                "name": cat.get("name", ""),
                "category": cat.get("category", ""),
                "confidence": round(c["confidence"], 2),
            })
        return json.dumps(out, ensure_ascii=True) if out else ""

    def set_qty(rec, ln, dict_packf, unit_hint):
        cq, unit, bd, flag = compute_qty(ln, dict_packf, unit_hint)
        rec["canonical_qty"] = cq
        rec["canonical_unit"] = unit
        rec["qty_breakdown"] = bd
        rec["unit_flag"] = flag

    unmatched_for_llm = []
    line_records = []
    for i, ln in enumerate(lines):
        dm = dict_match(ln, supplier_id, dict_idx)
        rec = _base_line(doc_id, i + 1, ln, currency, fx_rate)
        if dm and dm["match_productId"]:
            pid = dm["match_productId"]
            rec["match_productId"] = pid
            rec["match_name"] = cat_by_id.get(pid, {}).get("name", "")
            rec["match_confidence"] = "%.2f" % dm["confidence"]
            rec["match_source"] = "dict"
            set_qty(rec, ln, dm["pack_factor"],
                    dm["canonical_unit"] or cat_by_id.get(pid, {}).get("unit", ""))
            rec["status"] = "matched"
        else:
            unmatched_for_llm.append((i, ln))
        line_records.append(rec)

    if unmatched_for_llm:
        matches = llm_match_batch([l for _, l in unmatched_for_llm], catalog, match_prompt)
        for pos, (i, ln) in enumerate(unmatched_for_llm):
            cands = matches.get(pos) or []
            rec = line_records[i]
            primary = cands[0] if cands else None
            conf = primary["confidence"] if primary else 0.0
            pid = primary["productId"] if primary else ""
            # store alternatives (candidates 2..3) for one-tap swap in the app
            rec["match_alternatives"] = alt_json(cands[1:3])
            # Bands: >=GREEN auto-accept; [REVIEW,GREEN) suggest + "check";
            # <REVIEW -> unmatched (drop productId, do not stretch).
            if pid and conf >= REVIEW:
                rec["match_productId"] = pid
                rec["match_name"] = cat_by_id.get(pid, {}).get("name", "")
                rec["match_confidence"] = "%.2f" % conf
                rec["match_source"] = "llm"
                set_qty(rec, ln, None, cat_by_id.get(pid, {}).get("unit", ""))
                rec["status"] = "matched"
                if conf < GREEN:
                    rec["notes"] = "do sprawdzenia (pewnosc %.2f)" % conf
            else:
                rec["match_productId"] = ""
                rec["match_name"] = ""
                rec["match_confidence"] = "%.2f" % conf
                rec["match_source"] = "llm"
                # still compute qty so the manager sees pack breakdown / flag
                set_qty(rec, ln, None, "")
                rec["status"] = "unmatched"

    # price per canonical unit (needs canonical_qty, so after the qty pass)
    for i, ln in enumerate(lines):
        set_price(line_records[i], ln, currency, fx_rate)

    # report
    rc.log("doc %s: supplier=%s nr=%s date=%s cur=%s lines=%d status=%s"
           % (doc_id, supplier_id or parsed.get("supplier_name", ""),
              parsed.get("doc_number", ""), doc_date, currency, len(lines), status))
    for r in line_records:
        qty = r.get("qty_breakdown") or ("%s %s" % (r.get("canonical_qty", ""), r.get("canonical_unit", "")))
        price = r.get("purchase_price_pln") or "-"
        rc.log("  [%s] %s -> %s (%s, conf %s) | qty: %s | cena: %s%s"
               % (r["line_no"], r["raw_name"][:40], r.get("match_name", "") or "?",
                  r.get("match_source", ""), r.get("match_confidence", ""),
                  qty.strip(), price,
                  " UNIT_FLAG" if rc.is_true(r.get("unit_flag")) else ""))

    if dry:
        rc.log("dry: not writing doc %s (%d lines)" % (doc_id, len(line_records)))
        return

    # write lines then update doc
    rc.append_rows(lines_ws, line_headers, line_records)
    patch = {
        "status": status,
        "updated_at": now_ts(),
        "supplier_id": supplier_id,
        "supplier_name_raw": parsed.get("supplier_name", ""),
        "doc_number": parsed.get("doc_number", ""),
        "doc_date": doc_date,
        "currency": currency,
        "is_foreign_wnt": bool(is_foreign),
        "dedup_key": dedup_key,
    }
    if fx_rate is not None:
        patch["fx_rate_to_pln"] = "%.4f" % fx_rate
        patch["fx_rate_date"] = fx_date or ""
    if parser_note:
        patch["notes"] = parser_note
    _set_doc(docs_ws, doc_headers, d, patch, dry)


def _base_line(doc_id, line_no, ln, currency, fx_rate):
    # Price is computed later by set_price (needs canonical_qty). Empty here.
    return {
        "line_id": str(uuid.uuid4()),
        "doc_id": doc_id,
        "line_no": line_no,
        "raw_name": ln.get("raw_name", ""),
        "raw_supplier_code": ln.get("raw_supplier_code", ""),
        "raw_qty": ln.get("raw_qty", ""),
        "raw_unit": ln.get("raw_unit", ""),
        "raw_unit_price": ln.get("raw_unit_price", ""),
        "raw_line_total": ln.get("raw_line_total", ""),
        "vat_rate": ln.get("vat_rate", ""),
        "purchase_price_pln": "",
        "status": "unmatched",
    }


def set_price(rec, ln, currency, fx_rate):
    # Net purchase price PER WAREHOUSE UNIT (canonical), consistent with the
    # quantity math. Price is OPTIONAL for a receipt (WZ often has none):
    #   1) wartosc netto (line total) / canonical_qty   -> best, packaging-safe
    #   2) (cena jedn. netto x raw_qty) / canonical_qty  -> total via unit price
    #   3) cena jedn. netto, only if there is no packaging (pack_size empty/1)
    # If none is reliable -> leave empty (NOT zero) and mark "wpisz cene".
    cq = rc.to_float(rec.get("canonical_qty"))
    lt = rc.to_float(ln.get("raw_line_total"))
    up = rc.to_float(ln.get("raw_unit_price"))
    rq = rc.to_float(ln.get("raw_qty"))
    psize = rc.to_float(ln.get("pack_size"))

    price = None
    if lt is not None and cq and cq > 0:
        price = lt / cq
    elif up is not None and rq is not None and cq and cq > 0 and rq > 0:
        price = (up * rq) / cq
    elif up is not None and (psize is None or psize == 1):
        price = up

    if price is None or price < 0:
        rec["purchase_price_pln"] = ""
        note = "wpisz cene"
        rec["notes"] = (rec.get("notes") + " | " + note) if rec.get("notes") else note
        return
    if currency != "PLN" and fx_rate:
        price = price * fx_rate
    rec["purchase_price_pln"] = "%.4f" % price


def _set_doc(docs_ws, doc_headers, d, patch, dry):
    if dry or not d.get("_row"):
        rc.log("dry/no-row: doc patch %s" % json.dumps(patch))
        return
    for k, v in patch.items():
        if k in doc_headers:
            rc.set_cell(docs_ws, doc_headers, d["_row"], k, v)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        rc.log("ingest: FATAL %s" % e)
        rc.tg("M12 ingest fatal: %s" % str(e)[:300])
        sys.exit(1)
