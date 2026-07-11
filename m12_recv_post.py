#!/usr/bin/env python3
# -*- coding: ascii -*-
# MOSS M12 recv - post: the ONLY module that writes to live Dotypos.
# It creates a stockup (receiving) on the single warehouse and, for new items,
# creates products. Also updates purchase prices. See brief sec.0 SAFETY.
#
# SAFETY GATE (do not weaken):
#   * default behaviour is DRY. Nothing is sent to Dotypos unless you pass
#     BOTH --live AND the parsing does not set --dry.
#   * even with --live, process small batches (--limit).
#
# Schema is confirmed against docs.api.dotypos.com (Warehouse entity):
#   POST /v2/clouds/{cloudId}/warehouses/{warehouseId}/stockups   (create-only;
#   GET on that path returns 405 - there is no list endpoint). Body:
#   {invoiceNumber (required), currency="PLN" (default is CZK), updatePurchasePrice,
#    items:[{_productId, quantity, purchasePrice}], optional _supplierId/note}.
#   No ETag/If-Match required. items[] capped at 100 -> we POST in chunks.
#
# Usage:
#   python3 m12_recv_post.py --probe          # read existing stockups (safe)
#   python3 m12_recv_post.py --dry            # show what WOULD be sent
#   python3 m12_recv_post.py --live --limit 1 # LIVE: one document (after OK)
#
# Only documents with Recv_Docs.status == approved are considered. The bot
# never posts a document the manager did not approve.

import argparse
import json
import sys
import time

import recv_common as rc

WH = rc.DOTY_WAREHOUSE_ID
STOCKUPS_PATH = "/warehouses/%s/stockups" % WH


def now_ts():
    return time.strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# schema probe (read-only). NOTE: there is NO GET to list stock-ups - a GET on
# /warehouses/{id}/stockups returns HTTP 405 (create-only via POST). So we
# verify connectivity and show the warehouse-product fields that a stock-up
# updates (purchasePriceWithoutVat, stockQuantityStatus).
# ---------------------------------------------------------------------------
def probe(doty):
    rc.log("probe: reading warehouse %s (read-only)" % WH)
    try:
        wh, _ = doty.get("/warehouses/%s" % WH)
        rc.log("probe: warehouse: %s" % json.dumps(wh)[:400])
    except Exception as e:
        rc.log("probe: warehouse read failed: %s" % e)
    try:
        data, _ = doty.get("/warehouses/%s/products?page=1&limit=2" % WH)
        prods = data.get("data") if isinstance(data, dict) else data
    except Exception as e:
        rc.log("probe: products read failed: %s" % e)
        prods = []
    rc.log("probe: sample warehouse products (the fields a stock-up updates):")
    for p in (prods or [])[:2]:
        rc.log("  keys: %s" % ", ".join(sorted(p.keys())))
        rc.log("  sample: %s" % json.dumps(p)[:500])
    rc.log("probe: CONFIRMED stock-up = POST %s (create-only, no GET list)" % STOCKUPS_PATH)
    rc.log("probe: body = {invoiceNumber, currency=PLN, updatePurchasePrice, "
           "items:[{_productId, quantity, purchasePrice}], optional _supplierId/note}. "
           "No ETag required.")
    return 0


# ---------------------------------------------------------------------------
# id extraction from a Dotypos POST response (robust to shape)
# ---------------------------------------------------------------------------
def _extract_id(data, hdrs=None):
    # Pull a created-resource id from a Dotypos v2 POST response whatever the
    # shape: a bare object {id/_id}, an ARRAY (bulk endpoints return arrays), or
    # a {"data":[...]} envelope. Falls back to the Location header's trailing id.
    # Returns "" when nothing usable is present (caller logs the raw body).
    obj = None
    if isinstance(data, list):
        obj = data[0] if data else None
    elif isinstance(data, dict):
        inner = data.get("data")
        if isinstance(inner, list) and inner:
            obj = inner[0]
        elif isinstance(inner, dict):
            obj = inner
        else:
            obj = data
    if isinstance(obj, dict):
        for k in ("id", "_id", "stockupId", "_stockupId"):
            v = obj.get(k)
            if v not in (None, ""):
                return str(v)
    if hdrs:
        loc = hdrs.get("Location") or hdrs.get("location") or ""
        if loc:
            tail = loc.rstrip("/").rsplit("/", 1)[-1]
            if tail:
                return str(tail)
    return ""


# ---------------------------------------------------------------------------
# product creation (schema VERIFY before live). Returns new productId or None.
# ---------------------------------------------------------------------------
def create_product(doty, name, category_id, unit, vat, sale_price, ean, dry):
    body = {
        "name": name,
        "_categoryId": category_id,
        "unit": unit or "szt",
    }
    # sellable SKU fields (only when provided)
    if vat not in (None, ""):
        body["vatRate"] = rc.to_float(vat)
    if sale_price not in (None, ""):
        body["priceWithVat"] = rc.to_float(sale_price)
    if ean:
        body["ean"] = ean
    if dry:
        rc.log("dry: would POST /products [%s]" % json.dumps(body))
        return "DRY_NEW_%d" % (abs(hash(name)) % 100000)
    # Dotypos v2 POST /products expects an ARRAY of product objects (bulk),
    # and returns an array. Wrap the single object and read data[0].
    status, data, _ = doty.post("/products", [body])
    obj = {}
    if isinstance(data, list) and data:
        obj = data[0]
    elif isinstance(data, dict):
        inner = data.get("data")
        obj = inner[0] if isinstance(inner, list) and inner else data
    pid = str(obj.get("id") or obj.get("_id") or "")
    rc.log("created product %s -> id %s (HTTP %s)" % (name, pid, status))
    return pid


# ---------------------------------------------------------------------------
# build stockup items from a document's confirmed lines
# ---------------------------------------------------------------------------
def build_items(doty, doc_id, lines, dry):
    # QUANTITY comes first: a line is postable with productId + quantity ALONE.
    # Price is optional. We split postable lines into two groups so we never
    # touch purchase price when it is unknown:
    #   priced   -> stock-up with updatePurchasePrice=true, items carry price
    #   qtyonly  -> stock-up with updatePurchasePrice=false, quantity only
    priced = []
    qtyonly = []
    koszt_rows = []
    tasks = []
    skipped = 0
    notpostable = []
    for ln in lines:
        mode = (ln.get("resolution_mode") or "").strip()
        line_no = ln.get("line_no", "")
        if mode == "skip" or mode == "":
            skipped += 1
            continue
        if mode == "expense_direct":
            koszt_rows.append({
                "doc_id": doc_id,
                "line_id": ln.get("line_id", ""),
                "supplier": ln.get("_supplier", ""),
                "category": ln.get("expense_category", ""),
                "net_pln": ln.get("purchase_price_pln", ""),
                "vat_rate": ln.get("vat_rate", ""),
                "date": ln.get("_doc_date", ""),
                "faktura_ref": ln.get("_ksef_ref", ""),
                "_name": ln.get("raw_name", ""),  # local only, for --dry report
            })
            continue

        product_id = (ln.get("match_productId") or "").strip()
        if mode == "create_ingredient":
            product_id = create_product(
                doty, ln.get("raw_name", "New ingredient"),
                rc.CAT_SKLADNIKI, ln.get("canonical_unit", "szt"),
                None, None, None, dry)
            tasks.append({
                "type": "recipe",
                "doc_id": doc_id,
                "line_id": ln.get("line_id", ""),
                "note": "Nowy skladnik: dodaj do receptury: %s" % ln.get("raw_name", ""),
                "productId": product_id,
                "created_at": now_ts(),
            })
        elif mode == "create_sku":
            product_id = create_product(
                doty, ln.get("new_sku_name") or ln.get("raw_name", "New SKU"),
                ln.get("new_sku_categoryId", ""), ln.get("canonical_unit", "szt"),
                ln.get("new_sku_vat"), ln.get("new_sku_sale_price_pln"),
                ln.get("new_sku_ean"), dry)

        qty = rc.to_float(ln.get("canonical_qty"))
        price = rc.to_float(ln.get("purchase_price_pln"))
        # Postable by QUANTITY: need product + qty. Price is NOT required.
        if not product_id or qty is None:
            if not product_id:
                reason = "brak productId (niedopasowane)"
            elif rc.is_true(ln.get("unit_flag")):
                reason = "brak ilosci (unit_flag / przelicz recznie)"
            else:
                reason = "brak ilosci"
            notpostable.append((line_no, ln.get("raw_name", ""), reason))
            rc.log("  line %s not postable (%s) - skipping" % (line_no, reason))
            continue
        item = {
            "_productId": product_id,
            "quantity": qty,
            "_line_id": ln.get("line_id", ""),  # local only, stripped before send
            "_name": ln.get("raw_name", ""),    # local only, for --dry report
            "_unit": ln.get("canonical_unit", ""),
        }
        if price is not None and price > 0:
            item["purchasePrice"] = price
            priced.append(item)
        else:
            qtyonly.append(item)  # WZ without price: move stock, keep old price
    return priced, qtyonly, koszt_rows, tasks, skipped, notpostable


def _chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def stockup_body(items, invoice_number, supplier_id=None, note="", with_price=True):
    # Confirmed StockUp schema (docs.api.dotypos.com, Warehouse entity):
    #   invoiceNumber (required, non-empty), currency (default CZK -> we force
    #   PLN), updatePurchasePrice, items[{_productId, quantity, purchasePrice}].
    #   Optional _supplierId (long), _closeDeliveryNoteIds, note. No ETag.
    # with_price=False -> quantity-only receipt: updatePurchasePrice=false and
    # items carry NO purchasePrice, so stock moves but the cost is not touched.
    clean = []
    for it in items:
        row = {"_productId": it["_productId"], "quantity": it["quantity"]}
        if with_price and it.get("purchasePrice") is not None:
            row["purchasePrice"] = it["purchasePrice"]
        clean.append(row)
    body = {
        "invoiceNumber": invoice_number,
        "currency": "PLN",
        "updatePurchasePrice": bool(with_price),
        "items": clean,
    }
    # _supplierId expects a numeric Dotypos supplier id; include only if numeric.
    if supplier_id and str(supplier_id).isdigit():
        body["_supplierId"] = int(supplier_id)
    if note:
        body["note"] = note
    return body


def _qfmt(x):
    try:
        return ("%g" % float(x))
    except Exception:
        return str(x)


def dry_report(doc_id, invoice_number, supplier_id, note,
               priced, qtyonly, koszt_rows, tasks, skipped, notpostable):
    # Human-readable, UNTRUNCATED preview + full JSON body written to a file,
    # so the whole receipt can be reviewed before --live.
    path = "/tmp/m12_dry_body_%s.json" % str(doc_id)[:8]
    payload = {
        "doc_id": doc_id,
        "invoiceNumber": invoice_number,
        "priced": stockup_body(priced, invoice_number, supplier_id, note, True) if priced else None,
        "qtyonly": stockup_body(qtyonly, invoice_number, supplier_id, note, False) if qtyonly else None,
    }
    try:
        with open(path, "w") as f:
            json.dump(payload, f, indent=2)
        rc.log("dry: FULL stockup body written to %s" % path)
    except Exception as e:
        rc.log("dry: could not write body file: %s" % e)

    def show_group(label, items, with_price):
        rc.log("  == %s: %d poz. ==" % (label, len(items)))
        for i, it in enumerate(items, 1):
            price = (" | %s PLN" % it["purchasePrice"]) if (with_price and it.get("purchasePrice") is not None) else " | bez ceny"
            rc.log("    %2d. %-40s | pid=%s | %s %s%s"
                   % (i, (it.get("_name") or "")[:40], it.get("_productId", ""),
                      _qfmt(it.get("quantity")), it.get("_unit") or "", price))

    if priced:
        show_group("PRICED (updatePurchasePrice=true)", priced, True)
    if qtyonly:
        show_group("QTY-ONLY (updatePurchasePrice=false, cena bez zmian)", qtyonly, False)
    if notpostable:
        rc.log("  == NIE do stockup: %d ==" % len(notpostable))
        for (lno, nm, reason) in notpostable:
            rc.log("    - linia %s: %-40s -> %s" % (lno, (nm or "")[:40], reason))
    if koszt_rows:
        rc.log("  == KOSZT (expense_direct, nie na magazyn): %d ==" % len(koszt_rows))
        for k in koszt_rows:
            rc.log("    - %-40s | %s | %s PLN"
                   % ((k.get("_name") or "")[:40], k.get("category", ""), k.get("net_pln", "")))
    if tasks:
        rc.log("  == ZADANIA (do receptury): %d ==" % len(tasks))
        for t in tasks:
            rc.log("    - %s" % t.get("note", ""))
    if skipped:
        rc.log("  == pominietych (skip): %d ==" % skipped)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry", action="store_true", help="force dry even with --live")
    ap.add_argument("--live", action="store_true", help="actually write to Dotypos")
    ap.add_argument("--probe", action="store_true", help="read existing stockups and exit")
    ap.add_argument("--limit", type=int, default=1, help="max approved docs this run")
    ap.add_argument("--doc", default="", help="process only this doc_id")
    args = ap.parse_args()

    doty = rc.Doty()

    if args.probe:
        return probe(doty)

    dry = args.dry or not args.live
    rc.log("post: mode = %s (limit=%d)" % ("DRY" if dry else "LIVE", args.limit))
    if not dry:
        rc.log("post: LIVE writes enabled - proceeding in small batches")

    docs_ws = rc.open_ws(rc.TAB_DOCS)
    lines_ws = rc.open_ws(rc.TAB_LINES)
    doc_headers, doc_rows = rc.read_records(docs_ws)
    line_headers, line_rows = rc.read_records(lines_ws)

    lines_by_doc = {}
    for l in line_rows:
        lines_by_doc.setdefault(l.get("doc_id"), []).append(l)

    approved = [d for d in doc_rows
                if (d.get("status") or "").strip() == "approved"
                and (not args.doc or d.get("doc_id") == args.doc)]
    approved = approved[:args.limit]
    rc.log("post: %d approved document(s) to process" % len(approved))

    for d in approved:
        doc_id = d.get("doc_id")
        dlines = lines_by_doc.get(doc_id, [])
        # enrich lines with a few doc-level fields for Recv_Koszt
        for l in dlines:
            l["_supplier"] = d.get("supplier_id") or d.get("supplier_name_raw", "")
            l["_doc_date"] = d.get("doc_date", "")
            l["_ksef_ref"] = d.get("ksef_faktura_ref", "")

        priced, qtyonly, koszt_rows, tasks, skipped, notpostable = build_items(
            doty, doc_id, dlines, dry)
        # invoiceNumber is required and must not be empty; fall back to a ref.
        invoice_number = (d.get("doc_number") or "").strip() or ("WZ-" + str(doc_id)[:8])
        note = "M12 recv %s" % doc_id
        rc.log("doc %s: priced=%d, qtyonly=%d, koszt=%d, task=%d, skip=%d, notpostable=%d"
               % (doc_id, len(priced), len(qtyonly), len(koszt_rows), len(tasks),
                  skipped, len(notpostable)))
        dry_report(doc_id, invoice_number, d.get("supplier_id"), note,
                   priced, qtyonly, koszt_rows, tasks, skipped, notpostable)

        if dry:
            rc.log("dry: NOT posting doc %s" % doc_id)
            continue

        # mark posting
        _set_doc(docs_ws, doc_headers, d, {"status": "posting", "updated_at": now_ts()})
        stockup_id = ""
        try:
            sids = []
            # items array is capped at 100 per stock-up; POST in chunks.
            # Priced lines: update purchase price. Qty-only: leave price alone.
            for (group, with_price) in ((priced, True), (qtyonly, False)):
                for chunk in _chunks(group, 100):
                    cbody = stockup_body(chunk, invoice_number, d.get("supplier_id"),
                                         note, with_price)
                    status, data, hdrs = doty.post(STOCKUPS_PATH, cbody)
                    sid = _extract_id(data, hdrs)
                    if not sid:
                        # Never lose the stock-up id silently: log what came back
                        # so the response shape is visible for the next run.
                        rc.log("doc %s: stock-up id NOT found in response (HTTP %s) raw=%s"
                               % (doc_id, status, json.dumps(data)[:300]))
                    sids.append(sid)
                    rc.log("doc %s: stock-up %s chunk (%d it.) id=%s HTTP %s"
                           % (doc_id, "priced" if with_price else "qtyonly",
                              len(chunk), sid, status))
            stockup_id = ",".join(s for s in sids if s)
        except Exception as e:
            rc.log("doc %s: stockup FAILED: %s" % (doc_id, e))
            _set_doc(docs_ws, doc_headers, d,
                     {"status": "error", "error_msg": str(e)[:300], "updated_at": now_ts()})
            rc.tg("M12 post blad doc %s: %s" % (doc_id, str(e)[:200]))
            continue

        # side sheets
        if koszt_rows:
            kws = rc.open_ws(rc.TAB_KOSZT,
                             ["doc_id", "line_id", "supplier", "category",
                              "net_pln", "vat_rate", "date", "faktura_ref"])
            kh, _ = rc.read_records(kws)
            rc.append_rows(kws, kh, koszt_rows)
        if tasks:
            tws = rc.open_ws(rc.TAB_TASKS,
                             ["type", "doc_id", "line_id", "note", "productId", "created_at"])
            th, _ = rc.read_records(tws)
            rc.append_rows(tws, th, tasks)

        # mark posted lines + doc (both priced and qty-only lines are posted)
        posted_ids = set(it["_line_id"] for it in priced) | set(it["_line_id"] for it in qtyonly)
        for l in dlines:
            if l.get("line_id") in posted_ids and l.get("_row"):
                rc.set_cell(lines_ws, line_headers, l["_row"], "status", "posted")
        _set_doc(docs_ws, doc_headers, d,
                 {"status": "posted", "stockup_id": stockup_id, "updated_at": now_ts()})
        rc.tg("M12 post: dokument %s zaksiegowany (stockup %s; %d z cena, %d bez ceny)"
              % (doc_id, stockup_id, len(priced), len(qtyonly)))

    return 0


def _set_doc(docs_ws, doc_headers, d, patch):
    if not d.get("_row"):
        return
    for k, v in patch.items():
        if k in doc_headers:
            rc.set_cell(docs_ws, doc_headers, d["_row"], k, v)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        rc.log("post: FATAL %s" % e)
        rc.tg("M12 post fatal: %s" % str(e)[:300])
        sys.exit(1)
