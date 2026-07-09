#!/usr/bin/env python3
# -*- coding: ascii -*-
# MOSS M12 recv - reconcile: match collective KSeF faktura vs posted WZ.
# Reads Google Sheets only. Writes a Recv_Reconcile summary tab and flags
# mismatches to Telegram. Never writes to Dotypos. Run daily.
#
#   python3 m12_recv_reconcile.py --dry
#   python3 m12_recv_reconcile.py

import argparse
import sys
import time

import recv_common as rc

RECON_HEADERS = ["faktura_ref", "supplier", "wz_count", "sum_net_pln",
                 "faktura_net_pln", "diff_pln", "flag", "checked_at"]
TOLERANCE_PLN = 1.0  # rounding tolerance


def now_ts():
    return time.strftime("%Y-%m-%d %H:%M:%S")


def line_net(l):
    price = rc.to_float(l.get("purchase_price_pln"))
    qty = rc.to_float(l.get("canonical_qty")) or rc.to_float(l.get("raw_qty"))
    if price is not None and qty is not None:
        return price * qty
    total = rc.to_float(l.get("raw_line_total"))
    return total or 0.0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry", action="store_true", help="print, do not write sheet")
    args = ap.parse_args()

    docs_ws = rc.open_ws(rc.TAB_DOCS)
    lines_ws = rc.open_ws(rc.TAB_LINES)
    _, doc_rows = rc.read_records(docs_ws)
    _, line_rows = rc.read_records(lines_ws)
    _, koszt_rows = rc.read_records(rc.open_ws(rc.TAB_KOSZT,
                    ["doc_id", "line_id", "supplier", "category",
                     "net_pln", "vat_rate", "date", "faktura_ref"]))

    lines_by_doc = {}
    for l in line_rows:
        lines_by_doc.setdefault(l.get("doc_id"), []).append(l)

    # group by ksef_faktura_ref
    groups = {}
    faktura_net = {}
    for d in doc_rows:
        ref = (d.get("ksef_faktura_ref") or "").strip()
        if not ref:
            continue
        g = groups.setdefault(ref, {"supplier": "", "wz": [], "net": 0.0})
        g["supplier"] = d.get("supplier_id") or d.get("supplier_name_raw", "")
        src = (d.get("source") or "").strip()
        net = sum(line_net(l) for l in lines_by_doc.get(d.get("doc_id"), []))
        if src == "ksef_xml":
            # this doc IS the collective faktura -> its total is the target
            faktura_net[ref] = faktura_net.get(ref, 0.0) + net
        else:
            g["wz"].append(d.get("doc_id"))
            g["net"] += net

    # add koszt lines into the WZ-side sums (they belong to the faktura too)
    for k in koszt_rows:
        ref = (k.get("faktura_ref") or "").strip()
        if ref in groups:
            groups[ref]["net"] += rc.to_float(k.get("net_pln")) or 0.0

    rows = []
    flags = 0
    for ref, g in sorted(groups.items()):
        fnet = faktura_net.get(ref)
        diff = (g["net"] - fnet) if fnet is not None else None
        flag = ""
        if fnet is not None and abs(diff) > TOLERANCE_PLN:
            flag = "MISMATCH"
            flags += 1
        rows.append({
            "faktura_ref": ref,
            "supplier": g["supplier"],
            "wz_count": len(g["wz"]),
            "sum_net_pln": "%.2f" % g["net"],
            "faktura_net_pln": ("%.2f" % fnet) if fnet is not None else "",
            "diff_pln": ("%.2f" % diff) if diff is not None else "",
            "flag": flag,
            "checked_at": now_ts(),
        })
        rc.log("recon %s supplier=%s wz=%d sumWZ=%.2f faktura=%s %s"
               % (ref, g["supplier"], len(g["wz"]), g["net"],
                  ("%.2f" % fnet) if fnet is not None else "?", flag))

    if args.dry:
        rc.log("reconcile: --dry, %d group(s), %d mismatch(es), not writing" % (len(rows), flags))
        return 0

    ws = rc.open_ws("Recv_Reconcile", RECON_HEADERS)
    try:
        ws.batch_clear(["A3:H"])
    except Exception:
        pass
    rc.append_rows(ws, RECON_HEADERS, rows)
    if flags:
        rc.tg("M12 reconcile: %d rozbieznosc(i) faktura vs WZ - sprawdz Recv_Reconcile" % flags)
    rc.log("reconcile: wrote %d row(s), %d mismatch(es)" % (len(rows), flags))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        rc.log("reconcile: FATAL %s" % e)
        rc.tg("M12 reconcile fatal: %s" % str(e)[:300])
        sys.exit(1)
