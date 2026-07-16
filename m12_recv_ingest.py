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


def _claim_age_seconds(stamp):
    # Age in seconds of a "YYYY-MM-DD HH:MM:SS" claim timestamp (local time, same
    # clock that wrote it). None if unparseable. Used for the doc-claim freshness
    # check (brief batch2 sec.5).
    try:
        t = time.mktime(time.strptime(stamp.strip(), "%Y-%m-%d %H:%M:%S"))
    except (ValueError, TypeError):
        return None
    return max(0, int(time.time() - t))


def norm_name(s):
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


# faza1 task1: document-kind vocabulary written to Recv_Docs.doc_kind. The
# parser (LLM / ksef) emits a raw kind; this normalizes it. Anything the parser
# could not read -> "?" (the manager fixes it in the app; process_doc never
# overwrites a manual value with "?").
DOC_KINDS = ("faktura", "wz", "paragon", "zamowienie", "inny")


def classify_doc_kind(raw):
    # Map the parser's raw doc_kind (or a printed title fragment) onto the fixed
    # vocabulary. Keyword rules per the faza1 brief: "Faktura"/"Faktura VAT" ->
    # faktura; "WZ"/"Wydanie z magazynu" -> wz; "Paragon" -> paragon; faza1.1
    # task3: "Zamowienie"/"Order" -> zamowienie (an order is NOT a delivery
    # document and is never postable); a readable OTHER type -> inny;
    # unreadable/empty -> "?".
    s = str(raw or "").strip().lower()
    if not s:
        return "?"
    if s in DOC_KINDS:
        return s
    if "paragon" in s:
        return "paragon"
    if "faktura" in s or "invoice" in s or s in ("fv", "fa"):
        return "faktura"
    if s == "wz" or s.startswith("wz ") or "wydanie" in s or "dowod wydania" in s:
        return "wz"
    # after faktura/wz so "faktura do zamowienia..." still reads as faktura
    if "zamowienie" in s or "zamowienia" in s or "order" in s:
        return "zamowienie"
    return "?"


def dup_blocks(prev_row, doc_status):
    # faza1.1 task1: an exact-dup hash entry (Recv_Files) only blocks when the
    # document that OWNS it is still alive (needs_review/approved/posted/...).
    # A rejected or duplicate document keeps its hash in the registry as a
    # trace, but no longer vetoes a re-upload of the same bytes (real case:
    # rejected sklejka 23a163bc kept blocking the legal KSeF invoice 53194).
    # A hash row without doc_id never blocks (manual unlink); an owner missing
    # from Recv_Docs blocks (conservative - we cannot prove it is dead).
    did = (prev_row.get("doc_id") or "").strip()
    if not did:
        return False
    st = (doc_status or {}).get(did, "")
    return st not in ("rejected", "duplicate")


def page_ids_of(d):
    # Files that make up one document. A multi-page upload (brief item 5/6) sets
    # page_file_ids = comma-separated Drive ids (first == primary drive_file_id).
    # Single-file docs fall back to drive_file_id.
    raw = (d.get("page_file_ids") or "").strip()
    ids = [x.strip() for x in raw.split(",") if x.strip()] if raw else []
    if not ids:
        fid = (d.get("drive_file_id") or "").strip()
        ids = [fid] if fid else []
    return ids


def _num_key(v):
    return str(v or "").strip().replace(",", ".")


def _df(ln, *names):
    # First non-empty value among the given field names. Lets the new document-
    # layer schema (qty_doc/unit_doc/price_doc_net/total_doc_net) fall back to the
    # legacy raw_* names so old parses and re-parses both work.
    for n in names:
        v = ln.get(n)
        if v is not None and str(v).strip() != "":
            return v
    return ""


def dedup_parsed_lines(lines):
    # Drop positions repeated inside ONE parse (brief item 1). The LLM sometimes
    # emits the same physical line twice (Maracana WZ came back doubled). The key
    # is the WHOLE printed identity - Lp + name + qty + unit + code/EAN + unit
    # price + line total. batch5 task1: Lp is part of the key because a collective
    # invoice legally repeats byte-identical positions on DIFFERENT Lp (INTER-MLECZ
    # 75630: 12 legit repeats were collapsed -> 535.28 net short). Same Lp, or no
    # Lp extracted (fallback = the old key), still collapses as a true model
    # repeat. First wins, order preserved. Returns (unique_lines, removed_count).
    # Known limit: a multi-page doc whose pages restart Lp at 1 could collide two
    # identical rows across pages - accepted (identical name+qty+price+same Lp).
    seen = set()
    out = []
    removed = 0
    for ln in lines:
        name = norm_name(ln.get("raw_name"))
        lp = str(ln.get("lp") or "").strip()
        key = "|".join([
            lp,
            name,
            _num_key(_df(ln, "qty_doc", "raw_qty")),
            (_df(ln, "unit_doc", "raw_unit") or "").strip().lower(),
            (ln.get("raw_supplier_code") or "").strip().lower(),
            _num_key(_df(ln, "price_doc_net", "raw_unit_price")),
            _num_key(_df(ln, "total_doc_net", "raw_line_total")),
        ])
        # A wholly empty key (no name at all) is not a meaningful dup signal.
        if name and key in seen:
            removed += 1
            # Never collapse a line silently: log the exact repeat we drop so a
            # real under-count (if the key ever proves too loose) is visible.
            rc.log("dedup: exact repeat collapsed: lp=%s %r qty=%s unit=%s cena=%s suma=%s"
                   % (lp or "?", ln.get("raw_name"),
                      _num_key(_df(ln, "qty_doc", "raw_qty")),
                      (_df(ln, "unit_doc", "raw_unit") or "").strip(),
                      _num_key(_df(ln, "price_doc_net", "raw_unit_price")),
                      _num_key(_df(ln, "total_doc_net", "raw_line_total"))))
            continue
        seen.add(key)
        out.append(ln)
    return out, removed


def delivery_key(supplier_ref, number, date, count, total):
    # Identity of a DELIVERY for cross-document dedup (brief item 3): supplier
    # (NIP/supplier_id) + document number + date. When the number is unreadable,
    # fall back to supplier + line count + net total. Deliberately does NOT hash
    # the line contents, so a re-photo / file+KSeF pair of the same invoice
    # collide and get flagged for the manager (never auto-dropped).
    supplier_ref = (supplier_ref or "").strip().lower()
    number = re.sub(r"\s+", " ", (number or "").strip().lower())
    date = (date or "").strip()
    if number:
        basis = "num|%s|%s|%s" % (supplier_ref, number, date)
    else:
        basis = "cnt|%s|%s|%s" % (supplier_ref, count, total)
    return hashlib.sha1(basis.encode("utf-8")).hexdigest()[:16]


def delivery_key_for(supplier_ref, number, date, count, total):
    # Return a delivery dedup key ONLY when the document carries enough identity
    # to trust a collision. A readable number is enough on its own; otherwise we
    # need a supplier AND at least one line. A blank/unreadable scan (no number,
    # no supplier, 0 lines) gets None -> it can never be flagged as a duplicate
    # of another blank scan.
    number_n = re.sub(r"\s+", " ", (number or "").strip())
    if number_n:
        return delivery_key(supplier_ref, number, date, count, total)
    if (supplier_ref or "").strip() and count and count > 0:
        return delivery_key(supplier_ref, number, date, count, total)
    return None


def _sniff_source(path, fallback):
    # Detect a page's real type from its magic bytes so a multi-page document
    # mixing a PDF and a photo still parses each page correctly. Falls back to
    # the document-level source when the signature is unrecognised.
    try:
        with open(path, "rb") as f:
            head = f.read(16)
    except Exception:
        return fallback
    if head[:4] == b"%PDF":
        return "pdf"
    if head[:5].lower() == b"<?xml":
        return "ksef_xml"
    if (head[:3] == b"\xff\xd8\xff"                       # JPEG
            or head[:8] == b"\x89PNG\r\n\x1a\n"           # PNG
            or head[:4] == b"RIFF"                        # WEBP container
            or head[4:12] in (b"ftypheic", b"ftypmif1", b"ftypheix")):  # HEIC
        return "image"
    return fallback


def _safe_part(s, maxlen=40):
    # ASCII-safe, filesystem-safe fragment for a Drive filename.
    s = (s or "").strip()
    try:
        import unicodedata
        s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    except Exception:
        pass
    s = re.sub(r"[^A-Za-z0-9._-]+", "_", s).strip("_")
    return s[:maxlen] or "dokument"


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
        # faza1 task1: a KSeF file IS an invoice by definition - deterministic.
        "doc_kind": "faktura",
        # faza1 task2: P_15 = kwota naleznosci ogolem (gross "Do zaplaty").
        "doc_total_gross": first_text("P_15"),
        "lines": [],
    }
    for w in idx.get("FaWiersz", []):
        row = {}
        for child in w.iter():
            row[local(child.tag)] = (child.text or "").strip()
        header["lines"].append({
            "lp": row.get("NrWierszaFa", ""),
            "raw_name": row.get("P_7", ""),
            "raw_supplier_code": row.get("P_6A", ""),
            "raw_qty": row.get("P_8B", ""),
            "raw_unit": row.get("P_8A", ""),
            "raw_unit_price": row.get("P_9A", ""),
            "raw_line_total": row.get("P_11", ""),
            "vat_rate": row.get("P_12", ""),
        })
    return header


# A native PDF is treated as text-layer when its extracted text clears this bar;
# below it we render the page to an image for vision (brief batch2 sec.2.4).
PDF_TEXT_MIN_CHARS = int(os.environ.get("M12_PDF_TEXT_MIN_CHARS", "200") or "200")


def parse_document(source, local_path, prompt, ctx=None):
    # Returns (parsed_or_None, route_used). Route selection (brief batch5 task4):
    # BASE choice by file type only - PDF with a text layer -> pdf-text, photo /
    # scanned PDF (no text layer) -> vision. The batch3/4 "thin text layer"
    # auto-detect (qty=Lp / num_ratio / seq) is REMOVED; instead the manager can
    # force a route from the app ("Rozpoznaj ponownie": Tekstowo / Wzrokowo),
    # which arrives here as ctx["force_route"] in {"pdf-text","vision"}.
    force = ""
    if isinstance(ctx, dict):
        force = (ctx.get("force_route") or "").strip()
    if source in ("image", "wz_photo"):
        if force == "pdf-text":
            rc.log("route=vision (%s; wymuszony tekstowy niemozliwy dla obrazu)"
                   % source)
        else:
            rc.log("route=vision (%s)" % source)
        return rc.infer_vision(prompt, local_path), "vision"
    if source == "ksef_xml":
        rc.log("route=ksef-xml")
        return parse_ksef_xml(local_path), "ksef-xml"
    if source == "csv":
        return parse_csv(local_path, prompt, ctx), "csv"
    if source == "pdf":
        def _pdf_vision(tag):
            png = pdf_render_png(local_path)
            if not png:
                raise RuntimeError(
                    "PDF bez warstwy tekstowej; brak PyMuPDF/poppler do renderu strony")
            try:
                rc.log("route=vision (%s)" % tag)
                return rc.infer_vision(prompt, png), "vision"
            finally:
                try:
                    os.remove(png)
                except Exception:
                    pass

        if force == "vision":
            return _pdf_vision("wymuszony recznie")
        txt = pdf_extract_text(local_path)
        ntxt = len(txt.strip())
        if force == "pdf-text" or ntxt >= PDF_TEXT_MIN_CHARS:
            rc.log("route=pdf-text (%d chars)%s"
                   % (ntxt, " [wymuszony recznie]" if force == "pdf-text" else ""))
            return (rc.infer_text(prompt + "\n\n=== DOCUMENT TEXT ===\n" + txt),
                    "pdf-text")
        return _pdf_vision("pdf: text layer too short, %d chars < %d"
                           % (ntxt, PDF_TEXT_MIN_CHARS))
    return None, ""  # manual / paragon: lines already present


def parse_csv(local_path, prompt, ctx):
    # CSV import v1 (brief batch2 sec.2.4). A confirmed per-supplier template
    # (Recv_CsvTemplates, matched by header signature) parses deterministically
    # with NO LLM. Otherwise the LLM proposes a column mapping, we parse with it,
    # save the template UNCONFIRMED, and flag the doc "szablon do potwierdzenia".
    import csv as _csv
    with open(local_path, "r", encoding="utf-8", errors="replace", newline="") as f:
        sample = f.read()
    try:
        dialect = _csv.Sniffer().sniff(sample[:4096], delimiters=",;\t|")
    except Exception:
        class dialect:  # noqa: N801 - simple fallback dialect
            delimiter = ";" if sample.count(";") > sample.count(",") else ","
    reader = _csv.reader(sample.splitlines(), dialect)
    table = [row for row in reader if any((c or "").strip() for c in row)]
    if not table:
        return {"lines": [], "notes": "csv: pusty plik"}
    header = [(c or "").strip() for c in table[0]]
    data = table[1:]
    sig = rc.csv_header_signature(header)

    tmpl = None
    for t in (ctx or {}).get("csv_templates", []):
        if (t.get("header_signature") or "").strip() == sig and rc.is_true(t.get("confirmed")):
            tmpl = t
            break

    if tmpl:
        rc.log("route=csv-template (sig %s, deterministic)" % sig)
        mapping = {}
        try:
            mapping = json.loads(tmpl.get("mapping_json") or "{}")
        except Exception:
            mapping = {}
        lines = _csv_apply_mapping(header, data, mapping)
        return {"lines": lines}

    # No confirmed template -> let the LLM map columns, then remember it.
    rc.log("route=csv-llm (sig %s, szablon do potwierdzenia)" % sig)
    preview = "\n".join(["\t".join(r) for r in table[:8]])
    map_prompt = (
        "You map columns of a supplier delivery CSV to a fixed schema. Return "
        "STRICT JSON only: {\"mapping\":{\"raw_name\":\"<col>\",\"qty_doc\":\"<col>\","
        "\"unit_doc\":\"<col>\",\"price_doc_net\":\"<col>\",\"total_doc_net\":\"<col>\","
        "\"raw_supplier_code\":\"<col>\",\"vat_rate\":\"<col>\"}} where each <col> is "
        "the EXACT header text of the matching column, or \"\" if absent. "
        "Header row and a few data rows follow:\n" + preview
    )
    mapping = {}
    try:
        obj = rc.infer_text(map_prompt)
        mapping = obj.get("mapping") or {}
    except Exception as e:
        rc.log("csv: LLM mapping failed: %s" % e)
    lines = _csv_apply_mapping(header, data, mapping)
    # Save the proposed template (unconfirmed) for the manager to confirm later.
    if ctx and ctx.get("csv_templates_ws") is not None and mapping:
        try:
            row = {"supplier_nip": (ctx.get("csv_supplier_nip") or ""),
                   "header_signature": sig,
                   "mapping_json": json.dumps(mapping, ensure_ascii=True),
                   "confirmed": "FALSE", "created_at": now_ts()}
            rc.append_rows(ctx["csv_templates_ws"], rc.CSV_TEMPLATES_HEADERS, [row])
            ctx.setdefault("csv_templates", []).append(row)
        except Exception as e:
            rc.log("csv: template save failed: %s" % e)
    return {"lines": lines, "notes": "csv: szablon do potwierdzenia"}


def _csv_apply_mapping(header, data, mapping):
    # Build document-layer lines from a CSV using a {schema_field: header} mapping.
    idx = {}
    for field, col in (mapping or {}).items():
        col = (col or "").strip()
        if col and col in header:
            idx[field] = header.index(col)
    out = []
    for row in data:
        def cell(field):
            j = idx.get(field)
            return (row[j].strip() if (j is not None and j < len(row)) else "")
        name = cell("raw_name")
        if not name:
            continue
        out.append({
            "raw_name": name,
            "raw_supplier_code": cell("raw_supplier_code"),
            "qty_doc": _num_key(cell("qty_doc")),
            "unit_doc": cell("unit_doc"),
            "price_doc_net": _num_key(cell("price_doc_net")),
            "total_doc_net": _num_key(cell("total_doc_net")),
            "vat_rate": cell("vat_rate"),
        })
    return out


# ---------------------------------------------------------------------------
# matching
# ---------------------------------------------------------------------------
def build_dict_index(dict_rows):
    # Index Recv_Dictionary by every key we may look it up by:
    #   * the row's own `key` string (legacy: supplier_id|code or norm_name)
    #   * the SKU-memory key mem_key(supplier_nip, name/code) when the row carries
    #     supplier_nip (brief batch2 sec.2.3), so memory written by the app is
    #     found here at parse time.
    # Each entry carries both the legacy match fields AND the new translator/price
    # memory fields (unit_content, unit_skl, last_price_skl, tryb).
    idx = {}
    for r in dict_rows:
        entry = {
            "match_productId": r.get("match_productId", "") or r.get("productId", ""),
            "canonical_unit": r.get("canonical_unit", ""),
            "pack_factor": rc.to_float(r.get("pack_factor")) or 1.0,
            "confidence": rc.to_float(r.get("confidence")) or 0.9,
            "domain": r.get("domain", ""),
            # SKU-memory (batch2)
            "unit_content": rc.to_float(r.get("unit_content")),
            "unit_skl": (r.get("unit_skl") or "").strip(),
            "last_price_skl": rc.to_float(r.get("last_price_skl")),
            "tryb": (r.get("tryb") or "").strip(),
            # full-decision memory (batch5 task3): the chosen ingredient name for
            # create_ingredient and the expense category for koszt. Old-format
            # rows lack these columns -> .get() returns "" and the code degrades
            # to the previous behavior (no crash).
            "match_name": (r.get("match_name") or "").strip(),
            "expense_category": (r.get("expense_category") or "").strip(),
        }
        key = (r.get("key") or "").strip().lower()
        if key:
            idx.setdefault(key, entry)
        nip = re.sub(r"\D", "", r.get("supplier_nip") or "")
        if nip:
            code = (r.get("raw_supplier_code") or "").strip()
            name = r.get("raw_name") or ""
            mk = rc.mem_key(nip, name, code)
            idx.setdefault(mk, entry)
    return idx


def dict_match(line, supplier_id, supplier_nip, dict_idx):
    # Look up the learned binding/memory for a line. Priority: supplier code /
    # EAN, then supplier_nip + name (SKU memory), then legacy keys.
    code = (line.get("raw_supplier_code") or "").strip()
    keys = []
    if supplier_nip:
        keys.append(rc.mem_key(supplier_nip, line.get("raw_name"), code))
    if supplier_id and code:
        keys.append(("%s|%s" % (supplier_id, code)).lower())
    keys.append(norm_name(line.get("raw_name")))
    for k in keys:
        if k in dict_idx:
            return dict_idx[k]
    return None


def build_carry_index(old_rows):
    # batch5 task3: doc-local carry-over of the manager's decisions across a
    # re-parse. replace-by-doc_id rewrites the line set, and the standing rule
    # says manual edits (match_source=manual, resolution_mode, expense_category,
    # translator) must NOT be lost. The dictionary covers the cross-document
    # case; this covers THE SAME document: decisions harvested from the rows
    # about to be replaced, keyed by supplier code and by normalized name.
    # The fresh DOCUMENT layer always wins for quantities/prices.
    idx = {}
    for r in old_rows:
        entry = {}
        mode = (r.get("resolution_mode") or "").strip()
        src = (r.get("match_source") or "").strip().lower()
        pid = (r.get("match_productId") or "").strip()
        if mode == "expense_direct" and (r.get("expense_category") or "").strip():
            entry["mode"] = mode
            entry["expense_category"] = (r.get("expense_category") or "").strip()
        elif mode == "create_ingredient":
            entry["mode"] = mode
            entry["match_name"] = (r.get("match_name") or "").strip()
        elif mode == "skip":
            entry["mode"] = "skip"
        elif mode in ("bind_ingredient", "bind_sku") and pid and src in ("manual", "confirmed"):
            entry["mode"] = mode
            entry["match_productId"] = pid
            entry["match_name"] = (r.get("match_name") or "").strip()
            entry["match_source"] = src
            entry["match_confidence"] = (r.get("match_confidence") or "0.95").strip()
        # A manually set translator carries regardless of the mode.
        if (r.get("unit_content_src") or "").strip().lower() == "manual":
            uc = rc.to_float(r.get("unit_content"))
            if uc and uc > 0:
                entry["unit_content"] = uc
                entry["unit_skl"] = (r.get("unit_skl") or "").strip()
        if not entry:
            continue
        code = (r.get("raw_supplier_code") or "").strip().lower()
        if code:
            idx.setdefault("code|" + code, entry)
        nm = rc.mem_norm_name(r.get("raw_name"))
        if nm:
            idx.setdefault("name|" + nm, entry)
    return idx


def carry_for(dl, carry_idx):
    if not carry_idx:
        return None
    code = (dl.get("raw_supplier_code") or "").strip().lower()
    if code and ("code|" + code) in carry_idx:
        return carry_idx["code|" + code]
    nm = rc.mem_norm_name(dl.get("raw_name"))
    return carry_idx.get("name|" + nm)


def _candidates_from_match(m):
    # Extract the sorted candidate list from one match object. New shape:
    # candidates[]; the old single-productId shape is tolerated.
    raw_cands = m.get("candidates")
    if raw_cands is None and m.get("productId") is not None:
        raw_cands = [{"productId": m.get("productId"), "confidence": m.get("confidence", 0.0)}]
    cands = []
    for c in (raw_cands or []):
        pid = str(c.get("productId", "") or "")
        try:
            conf = float(c.get("confidence", 0.0))
        except Exception:
            conf = 0.0
        if pid:
            cands.append({"productId": pid, "confidence": conf})
    cands.sort(key=lambda x: x["confidence"], reverse=True)
    return cands[:3]


def llm_match_batch(items, catalog, match_prompt):
    # One LLM call: map raw delivery lines to catalog productIds using the MATCH
    # prompt (confidence bands + type-mismatch penalty live in that prompt).
    # `items` is a list of (ref, line) where ref is the STABLE index of the line
    # in the final (already-deduped) line list. Returns {ref: [candidate, ...]}
    # keyed by that SAME ref. Empty on failure.
    #
    # The match is anchored to the LINE, never to list position: we send each
    # line's ref and require the model to ECHO the raw_name back. A returned match
    # is trusted for a ref only when the echoed name matches that line; when the
    # model's index drifts (long produce lists with near-identical names used to
    # desync by one - e.g. MORELA taking OGOREK's match), the echoed name that
    # belongs to a DIFFERENT line reveals the swap and we rebind by name so every
    # line gets the match computed FOR IT (or none - we never apply another line's
    # match to it).
    if not items or not catalog:
        return {}
    cat = catalog[:800]
    cat_lines = "\n".join(
        "%s\t%s\t%s" % (c.get("productId", ""), c.get("name", ""), c.get("domain", ""))
        for c in cat
    )
    line_txt = "\n".join("%d\t%s" % (ref, (l.get("raw_name") or "")) for ref, l in items)
    prompt = (
        match_prompt
        + "\n\nCATALOG (productId<TAB>name<TAB>domain):\n" + cat_lines
        + "\n\nLINES (index<TAB>raw_name):\n" + line_txt + "\n"
    )
    try:
        obj = rc.infer_text(prompt)
    except Exception as e:
        rc.log("llm match failed: %s" % e)
        return {}

    want_name = dict((ref, norm_name(l.get("raw_name"))) for ref, l in items)
    our_names = set(v for v in want_name.values() if v)
    by_index = {}   # ref -> (cands, echoed_norm_name)
    by_name = {}    # norm_name -> cands (first non-empty answer for that name wins)
    for m in obj.get("matches", []):
        cands = _candidates_from_match(m)
        echoed = norm_name(m.get("name")) if m.get("name") is not None else ""
        try:
            ref = int(m["index"])
        except Exception:
            ref = None
        if ref is not None and ref in want_name and ref not in by_index:
            by_index[ref] = (cands, echoed)
        if echoed and echoed not in by_name:
            by_name[echoed] = cands

    out = {}
    realigned = 0
    for ref, l in items:
        want = want_name[ref]
        entry = by_index.get(ref)
        if entry is not None:
            cands, echoed = entry
            # Only override when the echoed name is DEFINITELY another line's name
            # (a genuine index swap). A mere paraphrase (echoed name not among our
            # lines) is trusted as this line's answer - dropping it would turn a
            # good match into a needless "unmatched".
            if echoed and want and echoed != want and echoed in our_names:
                realigned += 1
                if want in by_name:
                    out[ref] = by_name[want]  # this line's real answer, by name
                # else: no name-anchored answer for this line -> leave unmatched,
                # never apply the other line's candidates to it.
                continue
            out[ref] = cands
        elif want and want in by_name:
            out[ref] = by_name[want]
    if realigned:
        rc.log("llm match: %d line(s) had index drift -> rebound by name" % realigned)
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


# Base warehouse unit per measurement dimension.
_BASE_NAME = {"mass": "kg", "vol": "l", "count": "szt"}


def _skl_from_unit(u):
    dim, _ = normalize_unit(u)
    return _BASE_NAME.get(dim, "")


def _carton_from_name(name):
    # A trailing factory-carton hint: "(8)", "/8", "x8" WITHOUT a unit next to it.
    # Reference only - NEVER a quantity or a multiplier (brief batch2 sec.2.1).
    m = re.search(r"(?:\(\s*(\d+)\s*\)|/\s*(\d+)|[xX]\s*(\d+))\s*$", name or "")
    if not m:
        return ""
    num = next((g for g in m.groups() if g), "")
    return ("(%s)" % num) if num else ""


def doc_layer(ln):
    # The DOCUMENT layer: only what is printed in the table columns. The parser
    # emits the new *_doc names; legacy raw_* names are accepted as a fallback.
    name = ln.get("raw_name", "") or ""
    return {
        "raw_name": name,
        "raw_supplier_code": ln.get("raw_supplier_code", "") or "",
        "qty_doc": _df(ln, "qty_doc", "raw_qty"),
        "unit_doc": _df(ln, "unit_doc", "raw_unit"),
        "price_doc_net": _df(ln, "price_doc_net", "raw_unit_price"),
        "total_doc_net": _df(ln, "total_doc_net", "raw_line_total"),
        "vat_rate": ln.get("vat_rate", "") or "",
        "carton_hint": (ln.get("carton_hint") or "").strip() or _carton_from_name(name),
        "parsed_content": rc.to_float(_df(ln, "unit_content")),
        "parsed_unit_skl": (ln.get("unit_skl") or "").strip().lower(),
    }


# A number immediately followed by a mass/volume unit inside the product name
# (e.g. "2,5kg", "950 g", "750ml", "1 L"). Signals a packaged good whose size
# should come from unit_content, NOT from a piece count.
_NAME_MEASURE_RE = re.compile(r"\d[\d.,]*\s*(kg|dag|g|ml|cl|dl|l)\b", re.I)


def _name_has_measure(name):
    return bool(_NAME_MEASURE_RE.search(name or ""))


def translate_layer(dl, mem):
    # The TRANSLATOR layer (the only thing allowed to come from the name/memory):
    # unit_content = size of ONE document unit in the warehouse base unit; unit_skl
    # = that base unit; src in {memory, name, column}. Priority: memory > name >
    # column (unit_doc already a convertible base unit). Undefined -> (None,"","").
    if mem and mem.get("unit_content"):
        u = mem.get("unit_skl") or _skl_from_unit(dl["unit_doc"]) or ""
        # src: "memory" for a dictionary hit; a doc-local carry-over of the
        # manager's own translator passes src="manual" (batch5 task3).
        return mem["unit_content"], u, mem.get("src", "memory")
    # batch5.2 task1: when the DOCUMENT unit is already a convertible mass/volume
    # unit (kg/g/l/ml), the printed quantity IS the warehouse quantity (1:1 after
    # base conversion). A size token in the name ("ok.8,6 kg/szt") is per-piece
    # info and must NOT multiply the doc qty (Szynka 51994: 8.458 kg * 8.6 ->
    # 72.7 kg, ~8.6x stock distortion). The name source stays ONLY for count
    # units (szt/op), where the per-piece size is exactly what is needed.
    # Memory/manual above keeps priority over both.
    ddim, dfac = normalize_unit(dl["unit_doc"])
    if ddim in ("mass", "vol") and dfac is not None:
        return dfac, _BASE_NAME[ddim], "column"
    if dl["parsed_content"] and dl["parsed_content"] > 0:
        u = dl["parsed_unit_skl"]
        if u not in ("kg", "l", "szt"):
            u = _skl_from_unit(dl["unit_doc"]) or u or ""
        return dl["parsed_content"], u, "name"
    dim, fac = normalize_unit(dl["unit_doc"])
    if dim is not None and fac is not None:
        # column source (unit_doc already a base unit) is 1:1 - BUT a COUNT unit
        # (szt/op) on a line whose name carries an unresolved weight/volume token
        # (e.g. "DEVELEY 2,5kg/1,5kg/6") is an ambiguous package: do NOT invent a
        # piece count. Leave it undefined -> "jednostki?" for the manager to decide
        # (brief batch2 sec.10 test A: DEVELEY).
        if dim == "count" and _name_has_measure(dl["raw_name"]):
            return None, "", ""
        return fac, _BASE_NAME[dim], "column"
    return None, "", ""


def warehouse_layer(dl, content, unit_skl, currency, fx_rate):
    # The WAREHOUSE layer (only computed, never recognised):
    #   qty_skl   = qty_doc * unit_content
    #   price_skl = total_doc_net / qty_skl, else price_doc_net / unit_content
    # Empty (and unit_flag=True -> "jednostki?") when unit_content is undefined.
    out = {"unit_content": "", "unit_skl": (unit_skl or ""), "qty_skl": "",
           "price_skl": "", "unit_flag": True}
    qd = rc.to_float(dl["qty_doc"])
    if content is None or content <= 0 or qd is None:
        return out
    qskl = qd * content
    out["unit_content"] = _fmt(content)
    out["qty_skl"] = _fmt(qskl)
    out["unit_flag"] = bool(qskl >= 100000)
    price = None
    td = rc.to_float(dl["total_doc_net"])
    pd = rc.to_float(dl["price_doc_net"])
    if td is not None and qskl > 0:
        price = td / qskl
    elif pd is not None:
        price = pd / content
    if price is not None and price >= 0:
        if currency != "PLN" and fx_rate:
            price = price * fx_rate
        out["price_skl"] = "%.4f" % price
    return out


def ref_price_for(rec, cat_by_id, mem):
    # Reference net price per warehouse unit for the cena? control: the bound
    # ingredient's purchase price from Recv_Catalog, else the SKU memory's
    # last_price_skl. None when there is no reference to compare against.
    pid = (rec.get("match_productId") or "").strip()
    if pid:
        ref = rc.to_float(cat_by_id.get(pid, {}).get("last_purchase_price"))
        if ref:
            return ref
    if mem and mem.get("last_price_skl"):
        return mem["last_price_skl"]
    return None


def apply_controls(dl, wh, ref_price):
    # rachunek? : qty_doc * price_doc_net does not reconcile with total_doc_net.
    # cena?     : price_skl is outside [ref/2 .. ref*2] of a known reference.
    flags = {"flag_rachunek": False, "flag_cena": False}
    qd = rc.to_float(dl["qty_doc"])
    pd = rc.to_float(dl["price_doc_net"])
    td = rc.to_float(dl["total_doc_net"])
    if qd is not None and pd is not None and td is not None:
        tol = max(rc.RACHUNEK_ABS_PLN, rc.RACHUNEK_REL * abs(td))
        if abs(qd * pd - td) > tol:
            flags["flag_rachunek"] = True
    ps = rc.to_float(wh["price_skl"])
    if ps is not None and ref_price and ref_price > 0:
        if ps < ref_price * rc.CENA_LO or ps > ref_price * rc.CENA_HI:
            flags["flag_cena"] = True
    return flags


# ---------------------------------------------------------------------------
# --if-requested trigger: claim a fresh scan request from Recv_Control
# ---------------------------------------------------------------------------
def _stamp_control(row_num, patch):
    # Write path (rare): runs only when there is a fresh/stale request to stamp.
    # Kept out of the idle path so the every-minute tick stays read-only.
    ws = rc.open_ws(rc.TAB_CONTROL, rc.CONTROL_HEADERS)
    for k, v in patch.items():
        rc.set_cell(ws, rc.CONTROL_HEADERS, row_num, k, v)


def claim_requests(dry=False):
    # ONE Recv_Control read: scan_request + post_dry_request + rescan_request
    # (batch5 task4) checked together. Returns (scan_token, dry_req, rescan_req,
    # row_num). A fresh rescan takes THIS tick (the run becomes doc-targeted);
    # a simultaneous scan_request is left UNCLAIMED so the next tick handles it.
    # post_dry is only detected here - main() stamps post_dry_done after the dry
    # preview is actually sent, so a crash re-fires it.
    row, row_num = rc.control_row()
    if not row:
        return None, None, None, None
    dry_req = _detect_post_dry(row, row_num, dry)  # (doc_id, token) or None
    rescan_req = _claim_rescan(row, row_num, dry)  # (doc_id, route) or None
    scan_token = None if rescan_req else _claim_scan(row, row_num, dry)
    return scan_token, dry_req, rescan_req, row_num


def _claim_rescan(row, row_num, dry):
    # batch5 task4: "Rozpoznaj ponownie" - re-parse ONE doc with a FORCED route.
    # Token = "<doc_id>#<pdf-text|vision>#<epoch_ms>". Claimed (rescan_done
    # stamped) BEFORE the run, scan-style, so a crash cannot re-fire it in a loop;
    # the result is visible on the doc row itself (route / lines / status).
    req = str(row.get("rescan_request") or "").strip()
    done = str(row.get("rescan_done") or "").strip()
    if not req or req == done:
        return None
    parts = req.split("#")
    doc_id = parts[0].strip() if parts else ""
    route = parts[1].strip() if len(parts) > 1 else ""
    ms_part = parts[2] if len(parts) > 2 else ""
    try:
        req_ms = int(float(ms_part))
    except (TypeError, ValueError):
        req_ms = 0
    now_ms = int(time.time() * 1000)
    stale = bool(req_ms and (now_ms - req_ms) > SCAN_FRESH_SECONDS * 1000)
    bad = (not doc_id) or route not in ("pdf-text", "vision")
    if stale or bad:
        rc.log("if-requested: rescan request %r %s, ignored"
               % (req, "stale" if stale else "invalid"))
        if not dry:
            _stamp_control(row_num, {"rescan_done": req,
                                     "rescan_note": "ignored (stale/invalid)",
                                     "updated_at": now_ts()})
        return None
    if dry:
        rc.log("if-requested (dry): would claim rescan %s route=%s" % (doc_id, route))
        return (doc_id, route)
    _stamp_control(row_num, {"rescan_done": req,
                             "rescan_note": "rescan started %s route=%s"
                                            % (now_ts(), route),
                             "updated_at": now_ts()})
    return (doc_id, route)


def _claim_scan(row, row_num, dry):
    req = str(row.get("scan_request") or "").strip()
    done = str(row.get("scan_done") or "").strip()
    if not req or req == done:
        return None
    try:
        req_ms = int(float(req))
    except (TypeError, ValueError):
        req_ms = 0
    now_ms = int(time.time() * 1000)
    if req_ms and (now_ms - req_ms) > SCAN_FRESH_SECONDS * 1000:
        rc.log("if-requested: scan request %s is stale (%ds old), ignored"
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


def _detect_post_dry(row, row_num, dry):
    # post_dry_request token = "<doc_id>#<epoch_ms>". Fresh + not yet done -> return
    # the doc_id (do NOT stamp done here; main stamps after the dry send succeeds).
    # A stale request is cleared so we stop re-checking.
    req = str(row.get("post_dry_request") or "").strip()
    done = str(row.get("post_dry_done") or "").strip()
    if not req or req == done:
        return None
    doc_id, _, ms_part = req.partition("#")
    doc_id = doc_id.strip()
    try:
        req_ms = int(float(ms_part))
    except (TypeError, ValueError):
        req_ms = 0
    now_ms = int(time.time() * 1000)
    if req_ms and (now_ms - req_ms) > SCAN_FRESH_SECONDS * 1000:
        rc.log("if-requested: post_dry request %s is stale, ignored" % req)
        if not dry:
            _stamp_control(row_num, {"post_dry_done": req,
                                     "post_dry_note": "stale request ignored",
                                     "updated_at": now_ts()})
        return None
    if not doc_id:
        return None
    return (doc_id, req)


def _run_post_dry(doc_id, req_token, row_num, dry):
    # Send the dry stock-up preview for one doc to Telegram (NEVER live), then mark
    # post_dry_done so the trigger stops re-firing (brief batch2 sec.8).
    rc.log("if-requested: post_dry_request -> dry preview for doc %s" % doc_id)
    if dry:
        rc.log("if-requested (dry): would send post-dry preview for %s" % doc_id)
        return
    try:
        import m12_recv_post as post
        res = post.run_dry_to_telegram(doc_id)
        rc.log("post_dry: doc %s -> %s" % (doc_id, res))
        _stamp_control(row_num, {"post_dry_done": req_token,
                                 "post_dry_note": "dry wyslany " + now_ts() + " | " + res,
                                 "updated_at": now_ts()})
    except Exception as e:
        rc.log("post_dry FAILED for %s: %s" % (doc_id, e))
        rc.tg("M12 dry blad doc %s: %s" % (doc_id, str(e)[:200]))


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
    ap.add_argument("--route", default="", choices=["", "pdf-text", "vision"],
                    help="force the parse route for this run (batch5 task4; "
                         "used by the app's Rozpoznaj ponownie)")
    args = ap.parse_args()

    if args.redo:
        args.doc = args.redo
        args.force = True

    # Cross-process mutex (brief batch2 sec.5, level 1): the */5 ingest and the
    # */3 --if-requested trigger must never parse concurrently. A second run that
    # cannot get the lock exits silently (0), so cron does not alert on it.
    if not rc.acquire_singleton_lock():
        rc.log("ingest: another run active (lock busy) -> exit 0")
        return 0
    run_id = uuid.uuid4().hex[:12]
    rc.log("ingest: run_id=%s holds the ingest lock" % run_id)

    # Light trigger for the "Rozpoznaj" and "Przygotuj do wysylki" buttons: ONE
    # Recv_Control read checks BOTH keys (brief batch2 sec.8). A post_dry_request
    # runs a dry preview to Telegram right here (never live) and does not require a
    # scan. Checked BEFORE loading prompts / scanning Drive, so an idle every-minute
    # run costs one tiny read.
    if args.if_requested:
        token, dry_req, rescan_req, ctrl_row = claim_requests(args.dry)
        if dry_req:
            _run_post_dry(dry_req[0], dry_req[1], ctrl_row, args.dry)
        if rescan_req:
            # batch5 task4: targeted re-parse of ONE doc with a forced route.
            # A simultaneous scan_request stays unclaimed for the next tick.
            args.doc = rescan_req[0]
            args.force = True
            args.route = rescan_req[1]
            rc.log("if-requested: rescan doc %s route=%s -> targeted ingest"
                   % rescan_req)
        elif not token:
            rc.log("if-requested: no fresh scan request; nothing more to do")
            return 0
        else:
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
    files_ws = rc.open_ws(rc.TAB_FILES, rc.FILES_HEADERS)
    doc_headers, doc_rows = rc.read_records(docs_ws)
    # batch4 sec.1: the bot opens Recv_Docs without a header contract, so it must
    # create the columns it writes if the app has not yet. supplier_nip (seller
    # NIP) is new in batch4; without this the write was silently dropped by
    # _set_doc. Append-only, at the end. Skipped in --dry (no Sheets writes).
    if not args.dry:
        doc_headers = rc.ensure_columns(docs_ws, doc_headers,
                                        ["claimed_at", "claimed_by", "supplier_nip",
                                         "doc_total_net", "flag_suma", "suma_diff",
                                         "route",
                                         # faza1: same relative order as the
                                         # console's DOCS_HEADERS (lib/schema.ts)
                                         "doc_kind", "doc_total_gross",
                                         "dup_override",
                                         # faza1.1 task2: sklejka? flag
                                         "flag_sklejka"])
    line_headers, line_rows = rc.read_records(lines_ws)
    files_headers, file_rows = rc.read_records(files_ws)
    _, dict_rows = rc.read_records(rc.open_ws(rc.TAB_DICT))
    _, sup_rows = rc.read_records(rc.open_ws(rc.TAB_SUPPLIERS))
    _, cat_rows = rc.read_records(rc.open_ws(rc.TAB_CATALOG))

    dict_idx = build_dict_index(dict_rows)
    sup_by_nip = {re.sub(r"\D", "", (s.get("nip") or "")): s for s in sup_rows if s.get("nip")}

    # Per-doc line aggregates from the ONE Recv_Lines read (no extra reads):
    # which docs already have lines (double-append guard, item 1) and their
    # count + net total (delivery-dedup fallback when the number is unreadable).
    existing_line_doc_ids = set()
    line_agg = {}  # doc_id -> [count, total_net]
    old_lines_by_doc = {}  # doc_id -> old rows (carry-over source, batch5 task3)
    for lr in line_rows:
        did = (lr.get("doc_id") or "").strip()
        if not did:
            continue
        existing_line_doc_ids.add(did)
        old_lines_by_doc.setdefault(did, []).append(lr)
        agg = line_agg.setdefault(did, [0, 0.0])
        agg[0] += 1
        lt = rc.to_float(lr.get("raw_line_total"))
        if lt is not None:
            agg[1] += lt

    # Exact-file-dup registry (item 2): content hashes of already-parsed files.
    known_md5 = {r.get("md5"): r for r in file_rows if (r.get("md5") or "").strip()}
    known_sha = {r.get("sha256"): r for r in file_rows if (r.get("sha256") or "").strip()}

    # faza1.1 task1: status of every known doc, so the exact-dup guards can tell
    # a LIVE hash owner from a rejected/duplicate one (dup_blocks above).
    doc_status = {}
    for d in doc_rows:
        did = (d.get("doc_id") or "").strip()
        if did:
            doc_status[did] = (d.get("status") or "").strip().lower()

    # Every file id already claimed by a doc - INCLUDING extra pages of a
    # multi-page doc - so the scan never re-adds a page as its own document.
    known_file_ids = set()
    for d in doc_rows:
        for pid in page_ids_of(d):
            known_file_ids.add(pid)

    def _supplier_ref(d):
        sid = (d.get("supplier_id") or "").strip()
        return sid or norm_name(d.get("supplier_name_raw"))

    # Delivery keys already present (item 3) -> flag a colliding new doc. Only
    # docs with enough identity (number, or supplier+lines) get a key.
    known_delivery = {}
    for d in doc_rows:
        did = (d.get("doc_id") or "").strip()
        agg = line_agg.get(did, [0, 0.0])
        k = delivery_key_for(_supplier_ref(d), d.get("doc_number"),
                             d.get("doc_date"), agg[0], round(agg[1], 2))
        if k:
            known_delivery[k] = did

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
    batch_md5 = {}  # md5 seen earlier in THIS scan -> first fid (same-run dup)
    dup_skipped = 0
    for f in files:
        fid = f.get("id", "")
        if not fid or fid in known_file_ids:
            continue
        mime = f.get("mimeType", "")
        name_l = (f.get("name") or "").lower()
        if mime == "application/pdf":
            source = "pdf"
        elif "xml" in mime:
            source = "ksef_xml"
        elif "csv" in mime or name_l.endswith(".csv"):
            source = "csv"
        elif mime in IMAGE_MIMES:
            source = "wz_photo"
        else:
            rc.log("skip unsupported file %s (%s)" % (f.get("name"), mime))
            continue
        # Exact-duplicate file (item 2): a byte-identical file already parsed in
        # an earlier run (known_md5) or earlier in this scan (batch_md5). Do NOT
        # create a second document - move the copy out of the inbox, silently.
        # faza1.1 task1: a hash owned by a rejected/duplicate document does NOT
        # block - the re-upload becomes a normal new document (dup_blocks).
        md5 = (f.get("md5Checksum") or "").strip()
        prev_file = known_md5.get(md5) if md5 else None
        if md5 and (md5 in batch_md5
                    or (prev_file is not None and dup_blocks(prev_file, doc_status))):
            dup_skipped += 1
            rc.log("scan: exact-dup file %s (md5 %s) -> skip, no new doc"
                   % (f.get("name"), md5[:10]))
            if not args.dry and not args.doc:
                _move_processed(fid, "DUP_" + _safe_part(f.get("name") or fid, 60))
            known_file_ids.add(fid)
            continue
        if prev_file is not None:
            rc.log("scan: file %s (md5 %s) matches doc %s (status %s) -> "
                   "NOT a dup, parsing anew"
                   % (f.get("name"), md5[:10],
                      (prev_file.get("doc_id") or "-"),
                      doc_status.get((prev_file.get("doc_id") or "").strip(), "?")))
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
        if md5:
            batch_md5[md5] = fid
        new_doc_rows.append(rec)
        known_file_ids.add(fid)
    rc.log("scan: %d new file(s), %d exact-dup skipped" % (len(new_doc_rows), dup_skipped))
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

    # CSV templates (brief batch2 sec.2.4) - read once; parse_csv reads/appends.
    csv_templates_ws = rc.open_ws(rc.TAB_CSV_TEMPLATES, rc.CSV_TEMPLATES_HEADERS)
    _, csv_templates = rc.read_records(csv_templates_ws)

    ctx = {
        "known_delivery": known_delivery,
        "existing_line_doc_ids": existing_line_doc_ids,
        "files_ws": files_ws,
        "files_headers": files_headers,
        "known_sha": known_sha,
        "known_md5": known_md5,
        "doc_status": doc_status,
        "force": args.force,
        "csv_templates_ws": csv_templates_ws,
        "csv_templates": csv_templates,
        "csv_supplier_nip": "",
        "run_id": run_id,
        # batch5: forced parse route (rescan / --route) + carry-over source rows.
        "force_route": (args.route or "").strip(),
        "old_lines_by_doc": old_lines_by_doc,
    }
    for d in todo:
        try:
            process_doc(d, parse_prompt, match_prompt, dict_idx, sup_by_nip, cat_rows,
                        docs_ws, doc_headers, lines_ws, line_headers,
                        ctx, args.dry)
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
                ctx, dry):
    doc_id = d.get("doc_id")
    source = (d.get("source") or "").strip()
    rc.log("--- doc %s source=%s ---" % (doc_id, source))

    # Level-2 claim (brief batch2 sec.5): if another run stamped a FRESH claim on
    # this doc, skip it (defense-in-depth beyond the process lock). Otherwise stamp
    # our own claim so any later run sees this doc is being processed. --redo/--force
    # ignores a stale claim by design (manual re-test). Skipped in --dry.
    if not dry:
        claimed_at = (d.get("claimed_at") or "").strip()
        claimed_by = (d.get("claimed_by") or "").strip()
        if claimed_at and claimed_by and claimed_by != ctx.get("run_id"):
            age = _claim_age_seconds(claimed_at)
            if age is not None and age < rc.CLAIM_FRESH_SECONDS and not ctx.get("force"):
                rc.log("doc %s: fresh claim by run %s (%ss ago) -> skip (another run active)"
                       % (doc_id, claimed_by, age))
                return
        _set_doc(docs_ws, doc_headers, d,
                 {"claimed_at": now_ts(), "claimed_by": ctx.get("run_id", "")}, dry)

    file_meta = []  # per parsed page: {"sha256","md5","fid","filename"}
    parsed = None
    route_used = ""
    sklejka = False       # faza1.1 task2: pages look like DIFFERENT documents
    sklejka_msg = ""
    if source in ("manual", "paragon"):
        rc.log("manual/paragon: lines already present, skip parse")
    else:
        # A document can be ONE file or several pages (item 5). Parse each page
        # and merge the positions into a single line list.
        page_ids = page_ids_of(d)
        if not page_ids:
            raise RuntimeError("no drive_file_id to parse")
        ext = {"pdf": ".pdf", "ksef_xml": ".xml", "csv": ".csv"}.get(source, ".bin")
        merged = []
        header = None
        first_parsed = None
        routes = []           # per-page route -> Recv_Docs.route (batch5 task4)
        total_from_pages = ""  # printed Razem netto may sit on a later page
        gross_from_pages = ""  # faza1 task2: Razem brutto / Do zaplaty too
        kind_from_pages = ""   # faza1 task1: the title page may not be page 1
        page_heads = []        # faza1.1 task2: printed header of each page
        multipage = len(page_ids) > 1
        for pno, fid in enumerate(page_ids, start=1):
            tmp = "/tmp/m12_recv_%s_p%d%s" % (doc_id, pno, ext)
            rc.drive_download(fid, tmp)
            sha = rc.sha256_file(tmp)
            md5 = rc.md5_file(tmp)
            # Fallback exact-dup guard for a single bot file whose md5 was not in
            # the Drive metadata at scan time (item 2): a byte-identical file
            # already parsed under another doc -> drop this one silently.
            # faza1.1 task1: only a LIVE owner blocks; a hash held by a
            # rejected/duplicate document is a trace, not a veto (dup_blocks).
            prev = ctx["known_sha"].get(sha)
            prev_owner = (prev.get("doc_id") or "").strip() if prev else ""
            if not multipage and prev_owner and prev_owner != doc_id:
                if dup_blocks(prev, ctx.get("doc_status")):
                    try:
                        os.remove(tmp)
                    except Exception:
                        pass
                    rc.log("doc %s: exact-dup of %s (sha %s) -> drop, no lines"
                           % (doc_id, prev_owner, sha[:10]))
                    _set_doc(docs_ws, doc_headers, d,
                             {"status": "duplicate", "updated_at": now_ts(),
                              "notes": "dokladny duplikat pliku"}, dry)
                    if not dry:
                        _move_processed(fid, "DUP_" + _safe_part(prev.get("filename") or fid, 60))
                    return
                rc.log("doc %s: sha %s owned by %s (status %s) -> dead doc, "
                       "NOT a dup, parsing"
                       % (doc_id, sha[:10], prev_owner,
                          (ctx.get("doc_status") or {}).get(prev_owner, "?")))
            file_meta.append({"sha256": sha, "md5": md5, "fid": fid,
                              "filename": d.get("_name") or ""})
            # For a single-file doc keep the known source; for a multi-page doc
            # sniff each page so mixed PDF/photo pages parse correctly.
            psource = _sniff_source(tmp, source) if multipage else source
            try:
                page_parsed, page_route = parse_document(psource, tmp, parse_prompt, ctx)
            finally:
                try:
                    os.remove(tmp)
                except Exception:
                    pass
            if page_route:
                routes.append(page_route)
            if page_parsed is None:
                continue
            if first_parsed is None:
                first_parsed = dict(page_parsed)
            if header is None and any(page_parsed.get(k) for k in
                                      ("supplier_name", "supplier_nip", "doc_number", "doc_date")):
                header = dict(page_parsed)
            if not total_from_pages:
                tv = str(page_parsed.get("doc_total_net") or "").strip()
                if tv:
                    total_from_pages = tv
            if not gross_from_pages:
                gv = str(page_parsed.get("doc_total_gross") or "").strip()
                if gv:
                    gross_from_pages = gv
            if not kind_from_pages:
                kv = str(page_parsed.get("doc_kind") or "").strip()
                if kv:
                    kind_from_pages = kv
            if multipage:
                # faza1.1 task2: remember what THIS page prints about itself.
                # Continuation pages legally print nothing - they do not vote.
                page_heads.append({
                    "page": pno,
                    "doc_number": str(page_parsed.get("doc_number") or "").strip(),
                    "doc_date": str(page_parsed.get("doc_date") or "").strip(),
                    "supplier_nip": re.sub(
                        r"\D", "", str(page_parsed.get("supplier_nip") or "")),
                })
            pls = page_parsed.get("lines", []) or []
            merged.extend(pls)
            if multipage:
                rc.log("doc %s: page %d/%d -> %d line(s)"
                       % (doc_id, pno, len(page_ids), len(pls)))
        # faza1.1 task2: pages of ONE document must agree on their printed
        # header. Two DIFFERENT invoices uploaded together otherwise merge
        # into one doc silently (real case 23a163bc: 51994 + 53194 -> 16
        # lines under the first number, stock would book two deliveries as
        # one). A field with two+ distinct non-empty values across pages =
        # sklejka -> needs_review + flag, never a silent merge.
        if multipage and len(page_heads) > 1:
            diff_parts = []
            for fld, label in (("doc_number", "numery"),
                               ("supplier_nip", "NIP"),
                               ("doc_date", "daty")):
                vals = []   # distinct values, first-seen spelling kept
                seen = set()
                for ph in page_heads:
                    v = ph[fld]
                    k = re.sub(r"\s+", " ", v.strip().lower())
                    if k and k not in seen:
                        seen.add(k)
                        vals.append(v)
                if len(vals) > 1:
                    diff_parts.append("%s: %s" % (label, ", ".join(vals)))
            if diff_parts:
                sklejka = True
                sklejka_msg = ("sklejka? strony wygladaja na ROZNE dokumenty "
                               "(%s) - rozdziel i wgraj kazdy osobno"
                               % "; ".join(diff_parts))
                rc.log("doc %s: %s" % (doc_id, sklejka_msg))
        # Header from the page that carries supplier/number/date; fall back to
        # the first page (keeps currency etc. for an otherwise headerless page).
        parsed = header or first_parsed or {}
        parsed["lines"] = merged
        # The printed document total can live on a non-header page (batch5 task2).
        if not str(parsed.get("doc_total_net") or "").strip() and total_from_pages:
            parsed["doc_total_net"] = total_from_pages
        if not str(parsed.get("doc_total_gross") or "").strip() and gross_from_pages:
            parsed["doc_total_gross"] = gross_from_pages
        if not str(parsed.get("doc_kind") or "").strip() and kind_from_pages:
            parsed["doc_kind"] = kind_from_pages
        route_used = "+".join(dict.fromkeys(routes))

    if parsed is None:
        # manual/paragon: just move to needs_review. faza1 task1: the kind is
        # known from the channel itself (no parse to classify from).
        _set_doc(docs_ws, doc_headers, d,
                 {"status": "needs_review",
                  "doc_kind": "paragon" if source == "paragon" else "inny",
                  "updated_at": now_ts()}, dry)
        return

    lines = parsed.get("lines", []) or []
    # Remove positions repeated within THIS parse (item 1) before anything else.
    lines, dup_removed = dedup_parsed_lines(lines)
    if dup_removed:
        rc.log("doc %s: dropped %d duplicated position(s) inside the parse"
               % (doc_id, dup_removed))
    parsed["lines"] = lines
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
    # Visible in the log so the owner can confirm the LLM actually returned it
    # (batch4 sec.1): empty here means the model did not read a seller NIP.
    rc.log("doc %s: parsed supplier_nip=%r (name=%r)"
           % (doc_id, supplier_nip, parsed.get("supplier_name", "")))
    # Parties safety net (brief batch2 sec.2.5): the buyer NIP must never be read
    # as the supplier. If the parser swapped them, drop the buyer NIP/name so the
    # line is treated as "supplier unknown" rather than "supplier = us".
    if supplier_nip == rc.BUYER_NIP:
        rc.log("doc %s: parser put BUYER nip as supplier -> clearing (parties swap)"
               % doc_id)
        supplier_nip = ""
        sn = (parsed.get("supplier_name") or "").lower()
        if any(k in sn for k in ("moss", "fortbolt", "kawiarnia")):
            parsed["supplier_name"] = ""
    # faza1 task4: a value already on the row (typed in by the manager, or read
    # by an earlier parse) survives a re-parse that reads nothing. The parser
    # only ever IMPROVES the field, never blanks it.
    if not supplier_nip:
        prev_nip = re.sub(r"\D", "", d.get("supplier_nip") or "")
        if prev_nip and prev_nip != rc.BUYER_NIP:
            supplier_nip = prev_nip
            rc.log("doc %s: parser read no supplier_nip -> keeping stored %s"
                   % (doc_id, prev_nip))
    sup = sup_by_nip.get(supplier_nip)
    supplier_id = (sup.get("supplier_id") if sup else "") or (d.get("supplier_id") or "")
    currency = (parsed.get("currency") or d.get("currency") or "PLN").strip().upper()
    doc_date = (parsed.get("doc_date") or "").strip()
    is_foreign = bool(parsed.get("is_foreign")) or (sup and rc.is_true(sup.get("is_foreign_wnt")))

    # Delivery-level dedup (item 3): supplier(NIP)/id + number + date, fallback
    # supplier + line count + net total when the number is unreadable. A hit is a
    # POSSIBLE duplicate (re-photo, or file+KSeF of one invoice) -> flag for the
    # manager, never auto-drop. Multi-page docs are ONE doc, so their merged
    # pages cannot collide with each other.
    supplier_ref = supplier_id or norm_name(parsed.get("supplier_name", ""))
    net_total = 0.0
    for ln in lines:
        lt = rc.to_float(_df(ln, "total_doc_net", "raw_line_total"))
        if lt is not None:
            net_total += lt
    dedup_key = delivery_key_for(supplier_ref, parsed.get("doc_number", ""),
                                 doc_date, len(lines), round(net_total, 2)) or ""
    is_dup = False
    if dedup_key:
        prev_doc = ctx["known_delivery"].get(dedup_key)
        is_dup = bool(prev_doc and prev_doc != doc_id)
        ctx["known_delivery"][dedup_key] = doc_id
        if is_dup:
            rc.log("doc %s: possible duplicate delivery of %s (key %s)"
                   % (doc_id, prev_doc, dedup_key))
    status = "duplicate" if is_dup else "needs_review"
    # faza1.1 task2: a sklejka is a header-identity problem, not a duplicate -
    # the merged header mimics the FIRST page's invoice (23a163bc pretended to
    # be 51994 and lit "nowsza wersja"). The human decides; needs_review wins.
    if sklejka:
        status = "needs_review"

    # fx
    fx_rate, fx_date = (None, None)
    if currency != "PLN" and doc_date:
        fx_rate, fx_date = rc.nbp_rate(currency, doc_date)

    # match + normalize (three-layer model, brief batch2 sec.2.1/2.2/2.3)
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

    # Document layer for every line (istina - only printed columns).
    dls = [doc_layer(ln) for ln in lines]

    # --- match pass (batch5 task3): the manager's own doc-local decision
    # (carry-over) wins, then the SKU memory FULL decision (bind / koszt /
    # create_ingredient), then the LLM for what is left.
    carry_idx = build_carry_index(ctx.get("old_lines_by_doc", {}).get(doc_id, []))
    unmatched_for_llm = []
    line_records = []
    mem_for = {}  # i -> memory entry (translator + price memory) when hit
    for i, dl in enumerate(dls):
        rec = _base_line(doc_id, i + 1, dl)
        cr = carry_for(dl, carry_idx)
        dm = dict_match(dl, supplier_id, supplier_nip, dict_idx)
        if dm:
            mem_for[i] = dm
        # The manager's own translator from the previous session overrides the
        # dictionary for the translator layer (shows as "recznie").
        if cr and cr.get("unit_content"):
            mem_for[i] = {"unit_content": cr["unit_content"],
                          "unit_skl": cr.get("unit_skl", ""), "src": "manual"}
        cmode = (cr or {}).get("mode", "")
        tryb = (dm.get("tryb") or "").strip() if dm else ""
        if cmode == "skip":
            rec["resolution_mode"] = "skip"
            rec["match_source"] = "manual"
        elif cmode == "expense_direct":
            rec["resolution_mode"] = "expense_direct"
            rec["expense_category"] = cr.get("expense_category", "")
            rec["match_source"] = "manual"
            rec["match_confidence"] = "0.95"
            rec["status"] = "matched"
        elif cmode == "create_ingredient":
            rec["resolution_mode"] = "create_ingredient"
            rec["match_name"] = cr.get("match_name", "")
            rec["needs_recipe"] = True
            rec["match_source"] = "manual"
            rec["match_confidence"] = "0.95"
            rec["status"] = "matched"
        elif cmode in ("bind_ingredient", "bind_sku"):
            pid = cr["match_productId"]
            rec["resolution_mode"] = cmode
            rec["match_productId"] = pid
            rec["match_name"] = cr.get("match_name") or cat_by_id.get(pid, {}).get("name", "")
            rec["match_source"] = cr.get("match_source", "manual")
            rec["match_confidence"] = cr.get("match_confidence", "0.95")
            rec["status"] = "matched"
        elif dm and tryb == "expense_direct" and dm.get("expense_category"):
            # memory koszt: full decision, terminal per batch3 rules
            rec["resolution_mode"] = "expense_direct"
            rec["expense_category"] = dm["expense_category"]
            rec["match_source"] = "memory"
            rec["match_confidence"] = "0.95"
            rec["status"] = "matched"
        elif dm and tryb == "create_ingredient" and not (dm.get("match_productId") or "").strip():
            # memory create_ingredient: name + translator restored; post creates
            # the product at prowadzenie (batch3 pkg2 approach)
            rec["resolution_mode"] = "create_ingredient"
            rec["match_name"] = dm.get("match_name", "")
            rec["needs_recipe"] = True
            rec["match_source"] = "memory"
            rec["match_confidence"] = "0.95"
            rec["status"] = "matched"
        elif dm and dm.get("match_productId"):
            pid = dm["match_productId"]
            rec["match_productId"] = pid
            rec["match_name"] = cat_by_id.get(pid, {}).get("name", "")
            rec["match_confidence"] = "%.2f" % dm["confidence"]
            # SKU memory (has supplier_nip-derived translator) shows as "pamiec";
            # a legacy dictionary hit stays "dict".
            rec["match_source"] = "memory" if dm.get("unit_content") else "dict"
            if dm.get("tryb"):
                rec["resolution_mode"] = dm["tryb"]
            rec["status"] = "matched"
        else:
            unmatched_for_llm.append((i, {"raw_name": dl["raw_name"]}))
        line_records.append(rec)

    if unmatched_for_llm:
        # Key the matcher on the STABLE line index i (not list position), so the
        # returned match binds to the exact line it was computed for even if some
        # lines were dict-matched or the model's index drifts (see llm_match_batch).
        matches = llm_match_batch(unmatched_for_llm, catalog, match_prompt)
        for i, _ in unmatched_for_llm:
            cands = matches.get(i) or []
            rec = line_records[i]
            primary = cands[0] if cands else None
            conf = primary["confidence"] if primary else 0.0
            pid = primary["productId"] if primary else ""
            rec["match_alternatives"] = alt_json(cands[1:3])
            if pid and conf >= REVIEW:
                rec["match_productId"] = pid
                rec["match_name"] = cat_by_id.get(pid, {}).get("name", "")
                rec["match_confidence"] = "%.2f" % conf
                rec["match_source"] = "llm"
                rec["status"] = "matched"
                if conf < GREEN:
                    rec["notes"] = "do sprawdzenia (pewnosc %.2f)" % conf
            else:
                rec["match_productId"] = ""
                rec["match_name"] = ""
                rec["match_confidence"] = "%.2f" % conf
                rec["match_source"] = "llm"
                rec["status"] = "unmatched"

    # --- translator + warehouse layers + self-checks, per line ---
    for i, dl in enumerate(dls):
        rec = line_records[i]
        mem = mem_for.get(i)
        content, unit_skl, src = translate_layer(dl, mem)
        wh = warehouse_layer(dl, content, unit_skl, currency, fx_rate)
        rec["unit_content"] = wh["unit_content"]
        rec["unit_skl"] = wh["unit_skl"]
        rec["unit_content_src"] = src
        rec["qty_skl"] = wh["qty_skl"]
        rec["price_skl"] = wh["price_skl"]
        rec["carton_hint"] = dl["carton_hint"]
        # Legacy projection so post.py / older UI keep working unchanged.
        rec["canonical_qty"] = wh["qty_skl"]
        rec["canonical_unit"] = wh["unit_skl"]
        rec["purchase_price_pln"] = wh["price_skl"]
        rec["unit_flag"] = wh["unit_flag"]
        flags = apply_controls(dl, wh, ref_price_for(rec, cat_by_id, mem))
        rec["flag_rachunek"] = flags["flag_rachunek"]
        rec["flag_cena"] = flags["flag_cena"]

    # faza1 task1: classify the document kind. A manual fix on the row (or an
    # earlier good read) is never downgraded to "?" by a re-parse that could
    # not read the type.
    doc_kind = classify_doc_kind(parsed.get("doc_kind"))
    prev_kind = (d.get("doc_kind") or "").strip().lower()
    if doc_kind == "?" and prev_kind in DOC_KINDS:
        rc.log("doc %s: parser read no doc_kind -> keeping stored %r"
               % (doc_id, prev_kind))
        doc_kind = prev_kind

    # faza1 task2: printed gross total ("Razem brutto" / "Do zaplaty").
    # Unreadable -> empty, never blocks; an existing row value is kept.
    doc_gross = rc.to_float(parsed.get("doc_total_gross"))
    gross_out = ("%.2f" % doc_gross) if doc_gross is not None else ""
    if not gross_out:
        gross_out = (d.get("doc_total_gross") or "").strip()

    # report
    rc.log("doc %s: supplier=%s nr=%s date=%s cur=%s lines=%d status=%s"
           % (doc_id, supplier_id or parsed.get("supplier_name", ""),
              parsed.get("doc_number", ""), doc_date, currency, len(lines), status))
    rc.log("doc %s: doc_kind=%s (raw=%r) | doc_total_gross=%s"
           % (doc_id, doc_kind, parsed.get("doc_kind", ""), gross_out or "-"))
    for r in line_records:
        qskl = ("%s %s" % (r.get("qty_skl", ""), r.get("unit_skl", ""))).strip()
        price = r.get("price_skl") or "-"
        marks = "".join([
            " JEDN?" if rc.is_true(r.get("unit_flag")) else "",
            " RACHUNEK?" if rc.is_true(r.get("flag_rachunek")) else "",
            " CENA?" if rc.is_true(r.get("flag_cena")) else "",
        ])
        rc.log("  [%s] %s -> %s (%s, conf %s) | qty_doc: %s %s -> skl: %s | cena: %s%s"
               % (r["line_no"], r["raw_name"][:40], r.get("match_name", "") or "?",
                  r.get("match_source", ""), r.get("match_confidence", ""),
                  r.get("qty_doc", ""), r.get("unit_doc", ""), qskl or "-", price, marks))

    if dry:
        rc.log("dry: not writing doc %s (%d lines)" % (doc_id, len(line_records)))
        return

    # Line write - REPLACE by doc_id is the ONLY path (brief batch2 sec.5, level 3).
    # Always delete this doc's existing rows FIRST, then write the fresh set, so a
    # crash-recovery re-parse OR a --redo/--force OR two runs that both reached here
    # converge to ONE current line set (never 26+26=52). Only this doc_id's rows are
    # touched. Combined with the process lock (level 1) and the doc claim (level 2),
    # a plain append can no longer double a document.
    already = doc_id in ctx["existing_line_doc_ids"]
    if already:
        removed = rc.delete_rows_where(lines_ws, line_headers, "doc_id", doc_id)
        rc.log("doc %s: replace-by-doc_id -> deleted %d old line(s) before writing %d new"
               % (doc_id, removed, len(line_records)))
    rc.append_rows(lines_ws, line_headers, line_records)
    ctx["existing_line_doc_ids"].add(doc_id)

    # Document-sum control (batch5 task2): sum of parsed line totals vs the
    # PRINTED "Razem netto". Advisory only - never blocks Zatwierdz. Skipped when
    # the document prints no total. net_total is the line-total sum computed
    # above (all lines incl. koszt; a fresh parse has no deleted lines).
    doc_total = rc.to_float(parsed.get("doc_total_net"))
    flag_suma = False
    suma_diff = ""
    if doc_total is not None:
        sdiff = round(net_total - doc_total, 2)
        if abs(sdiff) > rc.SUMA_TOL_PLN:
            flag_suma = True
            suma_diff = "%.2f" % sdiff
            rc.log("doc %s: suma? lines=%.2f vs Razem netto=%.2f -> diff %.2f"
                   % (doc_id, net_total, doc_total, sdiff))
        else:
            rc.log("doc %s: suma ok (lines=%.2f vs Razem netto=%.2f)"
                   % (doc_id, net_total, doc_total))

    patch = {
        "status": status,
        "updated_at": now_ts(),
        "supplier_id": supplier_id,
        "supplier_name_raw": parsed.get("supplier_name", ""),
        # batch4 sec.2: persist the SELLER NIP so the SKU-memory key is complete
        # (supplier_nip + normalize(name)). Empty when the doc has no seller NIP.
        "supplier_nip": supplier_nip,
        "doc_number": parsed.get("doc_number", ""),
        "doc_date": doc_date,
        "currency": currency,
        "is_foreign_wnt": bool(is_foreign),
        "dedup_key": dedup_key,
        # batch5: printed doc total + suma? control + the route actually used.
        "doc_total_net": ("%.2f" % doc_total) if doc_total is not None else "",
        "flag_suma": flag_suma,
        "suma_diff": suma_diff,
        "route": route_used,
        # faza1 tasks 1+2: document kind + printed gross ("Do zaplaty"). Both
        # written on EVERY parse; an unreadable value never wipes what is
        # already on the row (a manual fix or an earlier good read).
        "doc_kind": doc_kind,
        "doc_total_gross": gross_out,
        # faza1.1 task2: written on EVERY parse, so a re-parse of corrected
        # pages clears a stale flag. error_msg carries the detected numbers
        # for the UI banner only when the flag is up.
        "flag_sklejka": sklejka,
    }
    if sklejka:
        patch["error_msg"] = sklejka_msg
    if fx_rate is not None:
        patch["fx_rate_to_pln"] = "%.4f" % fx_rate
        patch["fx_rate_date"] = fx_date or ""
    if parser_note:
        patch["notes"] = parser_note
    _set_doc(docs_ws, doc_headers, d, patch, dry)

    # Record processed file(s) in Recv_Files and move them out of the inbox into
    # Przetworzone/ with a readable name (items 2 + 4). Best-effort: a Drive
    # hiccup here must not fail a doc whose lines are already written.
    if file_meta:
        try:
            _register_and_move(ctx, d, parsed, doc_date, file_meta, net_total)
        except Exception as e:
            rc.log("doc %s: register/move failed: %s" % (doc_id, e))


def _base_line(doc_id, line_no, dl):
    # One Recv_Lines record seeded with the DOCUMENT layer (brief batch2 sec.2.1).
    # New *_doc columns are the truth; legacy raw_* columns are mirrored from them
    # so older code paths and the delivery-dedup net_total keep working.
    return {
        "line_id": str(uuid.uuid4()),
        "doc_id": doc_id,
        "line_no": line_no,
        "raw_name": dl["raw_name"],
        "raw_supplier_code": dl["raw_supplier_code"],
        # document layer (new)
        "qty_doc": dl["qty_doc"],
        "unit_doc": dl["unit_doc"],
        "price_doc_net": dl["price_doc_net"],
        "total_doc_net": dl["total_doc_net"],
        # legacy mirror (kept in sync with the document layer)
        "raw_qty": dl["qty_doc"],
        "raw_unit": dl["unit_doc"],
        "raw_unit_price": dl["price_doc_net"],
        "raw_line_total": dl["total_doc_net"],
        "vat_rate": dl["vat_rate"],
        "purchase_price_pln": "",
        "status": "unmatched",
    }


def _set_doc(docs_ws, doc_headers, d, patch, dry):
    if dry or not d.get("_row"):
        rc.log("dry/no-row: doc patch %s" % json.dumps(patch))
        return
    for k, v in patch.items():
        if k in doc_headers:
            rc.set_cell(docs_ws, doc_headers, d["_row"], k, v)


_PROCESSED_FOLDER_ID = None


def _processed_folder():
    # Resolve (and cache for this process) the Przetworzone/ subfolder id.
    global _PROCESSED_FOLDER_ID
    if _PROCESSED_FOLDER_ID is None:
        _PROCESSED_FOLDER_ID = rc.drive_ensure_folder(
            rc.DRIVE_PROCESSED_SUBFOLDER, rc.DRIVE_WZ_INBOX_FOLDER_ID) or ""
    return _PROCESSED_FOLDER_ID


def _move_processed(fid, new_name):
    # Move one file from the WZ inbox into the Przetworzone/ subfolder, renamed.
    # Files are only MOVED (audit), never deleted. Best-effort + logged.
    try:
        processed = _processed_folder()
        if not processed:
            rc.log("move: no Przetworzone folder; leaving %s in inbox" % fid)
            return
        rc.drive_move_rename(fid, new_name, processed, rc.DRIVE_WZ_INBOX_FOLDER_ID)
        rc.log("move: %s -> Przetworzone/%s" % (fid, new_name))
    except Exception as e:
        rc.log("move failed for %s: %s" % (fid, e))


def _register_and_move(ctx, d, parsed, doc_date, file_meta, net_total):
    # Append a Recv_Files row per page (audit + exact-dup registry) and move each
    # page out of the inbox, renamed YYYY-MM-DD_Supplier_Suma.ext (+_strN pages).
    doc_id = d.get("doc_id")
    supplier = parsed.get("supplier_name", "") or d.get("supplier_name_raw", "")
    number = parsed.get("doc_number", "") or d.get("doc_number", "")
    date = (doc_date or rc.today_str()).strip()
    suma = ("%.2f" % net_total) if net_total else ""
    base = "_".join(p for p in [
        date, _safe_part(supplier, 30), (suma + "PLN") if suma else ""] if p)
    multi = len(file_meta) > 1

    reg_rows = []
    for m in file_meta:
        row = {"sha256": m["sha256"], "md5": m["md5"], "doc_id": doc_id,
               "drive_file_id": m["fid"], "filename": m.get("filename", ""),
               "doc_number": number, "supplier": _safe_part(supplier, 40),
               "ts": now_ts(),
               "claimed_at": now_ts(), "claimed_by": ctx.get("run_id", "")}
        reg_rows.append(row)
        ctx["known_sha"][m["sha256"]] = row
        if m["md5"]:
            ctx["known_md5"][m["md5"]] = row
    try:
        rc.append_rows(ctx["files_ws"], ctx["files_headers"], reg_rows)
    except Exception as e:
        rc.log("Recv_Files append failed: %s" % e)

    for i, m in enumerate(file_meta, start=1):
        try:
            meta = rc.drive_get_meta(m["fid"])
            name = meta.get("name", "") or m.get("filename", "")
        except Exception:
            name = m.get("filename", "")
        ext = ("." + name.rsplit(".", 1)[-1]) if ("." in (name or "")) else ""
        suffix = ("_str%d" % i) if multi else ""
        _move_processed(m["fid"], base + suffix + ext)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        rc.log("ingest: FATAL %s" % e)
        rc.tg("M12 ingest fatal: %s" % str(e)[:300])
        sys.exit(1)
