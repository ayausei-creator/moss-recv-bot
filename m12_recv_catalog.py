#!/usr/bin/env python3
# -*- coding: ascii -*-
# MOSS M12 recv - catalog refresh (SAFE, read-only against Dotypos).
# Pulls products of the target categories from Dotypos and writes the
# Recv_Catalog tab that feeds the manager app dropdowns (brief sec.5).
# Writes ONLY to Google Sheets. Never writes to Dotypos.
#
# Run daily via cron. Use --dry to print what would be written without touching
# the sheet.
#
#   python3 m12_recv_catalog.py --dry
#   python3 m12_recv_catalog.py

import argparse
import re
import sys

import recv_common as rc

# Domain classification by category name (keywords, case-insensitive). We pull a
# BROAD set so purchased goods (kitchen ingredients AND retail drinks/water/alko)
# reach the app. Categories that match nothing are EXCLUDED (likely made-to-order
# menu items we never receive). The full category list is logged every run so
# Alex can refine these keywords without guessing.
KITCHEN_KW = ["skladnik", "mleko", "nabial", "opakowanie", "warzyw", "owoc",
              "mies", "ryb", "przypraw", "maka", "cukier", "kawa ziarn", "mrozon"]
SHELF_KW = ["sklep", "woda", "napoj", "sok", "piwo", "wino", "alko",
            "syrop", "herbat", "kawa-herb"]

CATALOG_HEADERS = ["productId", "name", "category", "unit", "domain",
                   "packaging_hint", "last_purchase_price"]


# Archive/inactive is decided by CATEGORY NAME + the product `deleted` flag only.
# NOT by `display`: ingredients (stockDeduct=0) are legitimately display=false
# ("hidden on the register") yet fully active in the warehouse.
ARCHIVE_NAME_KW = ["nie aktywny", "nieaktywny", "archiw", "stara", "stary",
                   "stare", "old", "zzz"]


def classify_domain(name):
    n = (name or "").strip().lower()
    if not n:
        return ""
    if n.startswith("sklep") or any(k in n for k in SHELF_KW):
        return "shelf"
    if any(k in n for k in KITCHEN_KW):
        return "kitchen"
    return ""  # excluded from the catalog


def is_archive_category(name):
    # Old duplicate/archive categories: "nie aktywny produkty", "Mleko L",
    # "Mleko M", "Herbata stara", etc. Rule: an archive keyword, OR a name that
    # ends with a lone single letter after a space (e.g. "... L" / "... M").
    n = (name or "").strip().lower()
    if not n:
        return True
    if any(k in n for k in ARCHIVE_NAME_KW):
        return True
    if re.search(r"\s[a-z]$", n):
        return True
    return False


def is_active_product(p):
    # Only `deleted` marks a product inactive. Do NOT use `display` (false is
    # normal for non-register ingredients).
    return str(p.get("deleted", "")).strip().lower() not in ("true", "1")


def _unit_of(prod):
    # Dotypos product unit field name is not 100% certain across clouds; try a
    # few known spellings, fall back to empty. (Verify against live docs.)
    for k in ("unit", "measurementUnit", "unitName", "quantityUnit"):
        v = prod.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def _purchase_price(wp):
    # Net purchase price of a warehouse product across known field spellings.
    for k in ("purchasePriceWithoutVat", "purchasePrice", "purchase_price",
              "lastPurchasePrice", "priceWithoutVat"):
        v = wp.get(k)
        f = rc.to_float(v)
        if f is not None and f > 0:
            return f
    return None


def _packaging_hint(prod):
    # Best-effort hint from a note/description field; empty if none.
    for k in ("note", "description", "packaging"):
        v = prod.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()[:80]
    return ""


def build_category_map(doty):
    # Return {category_id: (name, domain)} for every classified category.
    cats = doty.categories()
    out = {}
    names = {}
    for c in cats:
        cid = str(c.get("id") or c.get("_id") or "")
        name = (c.get("name") or "").strip()
        if not cid or not name:
            continue
        names[cid] = name
        if is_archive_category(name):
            continue  # skip archive/duplicate categories (Mleko L/M, nie aktywny)
        domain = classify_domain(name)
        if domain:
            out[cid] = (name, domain)
    # Ensure the two known ids are present even if names differ slightly.
    out.setdefault(rc.CAT_SKLADNIKI, ("Skladniki", "kitchen"))
    out.setdefault(rc.CAT_SKLEP_KAWA_HERB, ("Sklep Kawa-Herb", "shelf"))
    return out, names


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry", action="store_true", help="print, do not write sheet")
    args = ap.parse_args()

    doty = rc.Doty()
    rc.log("catalog: authorizing and reading categories/products from Dotypos")
    cat_map, all_names = build_category_map(doty)
    rc.log("catalog: classified target categories: %d" % len(cat_map))

    products = doty.products()
    rc.log("catalog: total products fetched: %d" % len(products))

    # Warehouse purchase price per product -> Recv_Catalog.last_purchase_price, the
    # reference the ingest cena? control compares against (brief batch2 sec.2.2).
    price_by_pid = {}
    try:
        for wp in doty.warehouse_products():
            pid = str(wp.get("_productId") or wp.get("productId")
                      or wp.get("id") or wp.get("_id") or "")
            pp = _purchase_price(wp)
            if pid and pp is not None:
                price_by_pid[pid] = pp
        rc.log("catalog: purchase prices for %d warehouse products" % len(price_by_pid))
    except Exception as e:
        rc.log("catalog: warehouse price read failed (%s); last_purchase_price empty" % e)

    # Count products per category (all + active) to expose real coverage.
    total_by_cat = {}
    active_by_cat = {}
    for p in products:
        cid = str(p.get("_categoryId") or "")
        total_by_cat[cid] = total_by_cat.get(cid, 0) + 1
        if is_active_product(p):
            active_by_cat[cid] = active_by_cat.get(cid, 0) + 1

    # Diagnostic: full category list with counts and include/exclude decision.
    # active = non-deleted (display is intentionally NOT used).
    rc.log("catalog: --- all categories (name | active/total | domain) ---")
    for cid in sorted(all_names, key=lambda x: all_names[x].lower()):
        name = all_names[cid]
        if cid in cat_map:
            dom = cat_map[cid][1]
        elif is_archive_category(name):
            dom = "ARCHIVE"
        else:
            dom = "EXCLUDED"
        rc.log("  %-28s %3d/%-3d  %s"
               % (name[:28], active_by_cat.get(cid, 0), total_by_cat.get(cid, 0), dom))

    rows = []
    per_cat = {}
    skipped_inactive = 0
    for p in products:
        cid = str(p.get("_categoryId") or "")
        if cid not in cat_map:
            continue
        if not is_active_product(p):
            skipped_inactive += 1
            continue
        cat_name, domain = cat_map[cid]
        pid = str(p.get("id") or p.get("_id") or "")
        name = (p.get("name") or "").strip()
        if not pid or not name:
            continue
        pp = price_by_pid.get(pid)
        rows.append({
            "productId": pid,
            "name": name,
            "category": cat_name,
            "unit": _unit_of(p),
            "domain": domain,
            "packaging_hint": _packaging_hint(p),
            "last_purchase_price": ("%.4f" % pp) if pp is not None else "",
        })
        per_cat[cat_name] = per_cat.get(cat_name, 0) + 1

    rc.log("catalog: rows to write: %d (skipped inactive/hidden: %d)"
           % (len(rows), skipped_inactive))
    for cat_name in sorted(per_cat):
        rc.log("  %-24s %d" % (cat_name, per_cat[cat_name]))

    # Skladniki slice so Alex can judge growth vs duplicates.
    sklad = [r for r in rows if r["category"] == "Skladniki"]
    rc.log("catalog: Skladniki (active) = %d" % len(sklad))
    for r in sorted(sklad, key=lambda x: x["name"].lower())[:25]:
        rc.log("    - %s [%s]" % (r["name"], r["unit"] or "?"))

    if args.dry:
        rc.log("catalog: --dry, not writing Recv_Catalog")
        return 0

    ws = rc.open_ws(rc.TAB_CATALOG, CATALOG_HEADERS)
    # Replace data rows (row 3 down) with the fresh snapshot.
    try:
        ws.batch_clear(["A3:%s" % _end_col(len(CATALOG_HEADERS))])
    except Exception as e:
        rc.log("catalog: batch_clear failed (%s); appending anyway" % e)
    rc.append_rows(ws, CATALOG_HEADERS, rows)
    rc.log("catalog: wrote %d rows to Recv_Catalog" % len(rows))
    return 0


def _end_col(n):
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        rc.log("catalog: FATAL %s" % e)
        rc.tg("M12 catalog blad: %s" % str(e)[:300])
        sys.exit(1)
