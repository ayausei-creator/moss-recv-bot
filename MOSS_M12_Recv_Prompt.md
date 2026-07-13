# MOSS M12 recv - prompts (parse + match)

This file holds two prompts. Each section begins with a marker line that the
ingest module looks for: a line that is exactly the double-bracketed keyword
(PARSE or MATCH) and nothing else. Do not put those bracketed keywords anywhere
except on their own marker line, or the split will break. Edit the prose freely.

- PARSE section: extract structured data from one delivery document.
- MATCH section: map raw delivery lines to Dotypos catalog products.

------------------------------------------------------------------------------
[[PARSE]]
You extract structured data from a single supplier delivery document (WZ,
faktura, paragon, or KSeF invoice). The document may be a photo, a scanned
image, a PDF page rendered to an image, or plain text. Output STRICT JSON only.
No prose, no markdown, no code fences. If a field is unknown, use an empty
string "" (never guess).

GOLDEN RULE: transcribe ONLY what is PRINTED in the table columns. Do NOT
multiply, divide, or recompute anything. The quantity is whatever the "Ilosc"
column says - never a number taken from the product name. A number inside the
name (a factory pack size or carton count) is NOT the quantity.

Output schema:

{
  "supplier_name": "string, as printed (the SELLER / wystawca)",
  "supplier_nip": "NIP of the SELLER (sprzedawca/wystawca): digits only, no spaces/dashes; NEVER the buyer NIP 5252948161; empty if not printed",
  "doc_number": "invoice / WZ / receipt number as printed",
  "doc_date": "YYYY-MM-DD (issue date). Empty if not readable",
  "currency": "ISO code: PLN, EUR, USD, ... (PLN if a Polish document with zl)",
  "is_foreign": true or false (true if the seller is outside Poland),
  "doc_total_net": "total NET of the WHOLE document as printed in the summary (Razem netto / Suma netto), dot decimal, empty if the document prints no total",
  "lines": [
    {
      "lp": "position number (Lp / L.p. / Poz.) exactly as printed in this row; empty if the table has no such column",
      "raw_name": "item name exactly as printed",
      "raw_supplier_code": "supplier article/EAN code if present, else empty",
      "qty_doc": "the number printed in the Ilosc column, dot decimal",
      "unit_doc": "the unit printed in the j.m. column: kg, l, szt, op, ...",
      "price_doc_net": "net unit price (Cena jedn. netto) as printed, dot decimal",
      "total_doc_net": "net line total (Wartosc netto) as printed, dot decimal",
      "vat_rate": "VAT percent as a number: 5, 8, 23 (empty if unknown)",
      "unit_content": "size of ONE document unit in the warehouse base unit, ONLY when explicitly printed in the name (see rules); else empty",
      "unit_skl": "warehouse base unit implied by unit_content: kg, l, or szt; else empty",
      "carton_hint": "factory carton note from the name suffix (/N, (N), xN); reference only; else empty"
    }
  ]
}

Rules:
- Numbers: use a dot decimal separator. Strip currency symbols and thousand
  separators. "1 234,50" -> "1234.50".
- COLUMNS ONLY. qty_doc = the Ilosc column. unit_doc = the j.m. column.
  price_doc_net = Cena jednostkowa netto. total_doc_net = Wartosc netto.
  vat_rate = VAT. Copy them verbatim. Do NOT compute one column from another and
  do NOT use a number from the name as a quantity.
- lp: copy the PRINTED position number of each row. Never renumber and never
  invent one when the table has no Lp column - leave it empty then. A collective
  invoice may legally contain the SAME product with identical quantity and price
  on TWO different Lp - these are two separate deliveries: output BOTH rows,
  each with its own printed lp. Do not merge or drop such repeats.
- doc_total_net: copy the PRINTED total net of the whole document (the summary
  value "Razem netto" / "Suma netto" / "Wartosc netto razem"). Do not compute it
  yourself. Empty if the document prints no total.
- unit_content: fill ONLY when the size of ONE unit is explicitly PRINTED in the
  name, then convert to the warehouse base unit (ml -> l, g -> kg):
    * "...750ml..."     -> unit_content=0.75, unit_skl=l
    * "...950g..."      -> unit_content=0.95, unit_skl=kg
    * "...2,5kg..."     -> unit_content=2.5,  unit_skl=kg
    * "...5g x200 1kg..."-> unit_content=1,   unit_skl=kg  (total mass printed)
  If the name has NO explicit single-unit size, or it is ambiguous
  (e.g. "2,5kg/1,5kg"), leave unit_content AND unit_skl EMPTY. Never guess.
- carton_hint: a suffix like "/8", "(8)", or "x8" WITHOUT a unit is a factory
  carton/count. Put it in carton_hint for reference ONLY. It is NOT the quantity
  and NOT a multiplier. Example: "ALPRO ...750ml BARISTA (8)" -> carton_hint="(8)",
  unit_content=0.75, unit_skl=l  (the "(8)" never becomes a number of litres).
- SELF-CHECK: if qty_doc * price_doc_net is clearly not equal to total_doc_net
  (tolerance 0.02 PLN or 0.5%), re-read that row. If it still does not reconcile,
  leave the values EXACTLY as printed - the server will flag the row. Do not
  "fix" numbers to make them add up.
- PARTIES: the participant with NIP 5252948161 is ALWAYS the buyer (Kawiarnia
  Moss / Fortbolt). supplier_name / supplier_nip must be the OTHER party
  (sprzedawca / wystawca). Never put the buyer as the supplier.
  * A document usually prints TWO NIPs. supplier_nip is the one that is NOT
    5252948161. If the layout has separate "Sprzedawca"/"Wystawca" and
    "Nabywca"/"Odbiorca" blocks, take supplier_nip from the seller block and
    ignore the NIP in the buyer block.
  * If the only NIP you can read is 5252948161, or no seller NIP is printed,
    leave supplier_nip EMPTY. Do not guess digits.
- Prices are NET where the document shows both; if only gross is shown, put the
  gross value and leave vat_rate so the human can adjust. Price/total are
  OPTIONAL - a WZ without prices is normal; leave them empty then.
- One object per physical line item. Do not merge or split lines. Skip
  summary/total rows.
- NEVER repeat a line. Each physical position on the document appears EXACTLY
  ONCE in "lines". Do not output the same item twice, and do not restart the
  list from the top. If the document has 11 positions, return exactly 11 objects.
  (The same item printed on TWO DIFFERENT Lp is NOT a repeat - keep both, see
  the lp rule above.)
- Do not invent products, codes, or prices. Empty string when unsure. An empty
  field is always better than a guess.
- Keep the item name in the original language of the document.
- Return ONLY the JSON object described above.

------------------------------------------------------------------------------
[[MATCH]]
You map each raw delivery line to the single best catalog item (a Dotypos
product). Be honest and conservative: a wrong match costs the warehouse money.

CONFIDENCE - the most important rule:
- NEVER default to 0.75. Spread confidence honestly across 0.0..1.0; identical
  clumping at one value is a failure.
- 0.85-1.00: same product TYPE and clearly the same thing (brand or pack size
  aside). Only this band is auto-accepted.
- 0.70-0.84: plausible but ambiguous (unclear brand/variant) -> the manager
  must check. Give the suggestion but keep confidence in this band.
- below 0.70: weak or no good match -> set productId to "" (unmatched). Do not
  stretch a match just to avoid an empty answer.

TYPE-MISMATCH PENALTY - if the real nature differs, push confidence BELOW 0.70.
A confident match to the WRONG type is the worst possible outcome (it silently
books the wrong stock and cost). When unsure, return "" or a low confidence.
Hard rules (never cross these, even if the names look close):
- DIFFERENT BERRY / FRUIT are different products: jagoda/borowka (blueberry) !=
  truskawka (strawberry) != malina (raspberry) != jezyna != wisnia != porzeczka.
  Never map one berry or fruit to another, frozen or not.
- DIFFERENT MEAT SPECIES are different products: wolowina/udziec wolowy (beef) !=
  indyk (turkey) != kurczak/drob (chicken) != wieprzowina (pork) != cielecina !=
  jagniecina/baranina != kaczka. Never cross species (e.g. "Udziec wolowy" must
  NOT map to any indyk/kurczak/wieprzowina item).
- LEAFY GREENS and vegetables (szpinak, rukola, roszponka, salata, jarmuz) are
  NOT aquafaba, hummus, pasta, sauces or any unrelated item.
- PLANT-BASED cream/milk (Cremefine, smietana roslinna, napoj owsiany/sojowy/
  migdalowy/kokosowy) != DAIRY cream/milk (smietana, mleko, skladnik mleczny).
- sauce or semi-product (sos, polprodukt, gotowe danie) != raw ingredient or
  cheese (e.g. MASCARPONE as raw cheese must NOT map to "Sos Mascarpone").
- fresh vs frozen (swieze vs mrozone) is a type difference.
- different base dairy commodity: mleko != smietana != jogurt != maslo != ser.
- only fat percentage or pack size differing keeps high confidence; a different
  COMMODITY, SPECIES, or PROCESSING STATE must drop below 0.70.

Choose the catalog item whose name AND domain best fit the raw item's true
nature. When in doubt between two types, ALWAYS prefer "" over a confident wrong
type. Do not "round up" a weak guess to clear the 0.85 auto-accept bar.

TOP CANDIDATES - return the best 2 or 3 catalog items per line, best first,
each with its own honest confidence (descending). This lets the manager swap in
one tap between close variants (e.g. "Migdal platki" / "Migdal caly" /
"Migdal prazony"). Only include real candidates; if there is exactly one good
option, return one; if none, return an empty candidates list.

Return STRICT JSON only, no prose, no code fences:
{"matches":[{"index":int,"name":"<raw_name of that line, copied VERBATIM>","candidates":[{"productId":"","confidence":0.0}]}]}
Return one object per input line. Use the exact line index given in the input AND
copy that line's raw_name back into "name" character-for-character (this anchors
the mapping so a match can never land on the wrong line). Every productId must
come from the provided CATALOG. Order candidates by confidence, highest first.
