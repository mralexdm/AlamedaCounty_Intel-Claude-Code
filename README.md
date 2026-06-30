# MADM - Alameda County Motivated Seller Lead Scraper

Automated daily pull of newly recorded **distress documents** (lis pendens,
foreclosure notices, judgments, tax / mechanic / HOA liens, probate, etc.) from
the **Alameda County Clerk-Recorder** public portal, enriched with parcel
addresses from the **County ArcGIS parcel layer**, scored as motivated-seller
leads, and published to a static dashboard + a Go High Level (GHL) CSV.

```
scraper/fetch.py            ← the scraper (Playwright + requests/BeautifulSoup)
scraper/requirements.txt    ← Python deps
dashboard/index.html        ← static lead dashboard (GitHub Pages)
dashboard/records.json      ← machine-readable output (also data/records.json)
dashboard/leads_ghl.csv     ← GHL import (also data/leads_ghl.csv)
.github/workflows/scrape.yml← daily cron + Pages deploy
```

## Quick start (local)

```bash
cd AlamedaCounty_Intel-Claude-Code
python -m venv .venv && . .venv/Scripts/activate      # Windows
# source .venv/bin/activate                            # macOS/Linux

pip install -r scraper/requirements.txt
python -m playwright install --with-deps chromium

python scraper/fetch.py --headed           # watch it work the first time
```

Open `dashboard/index.html` in a browser (or serve the folder) to view leads.

### Useful flags

| Flag | Purpose |
|------|---------|
| `--lookback-days N` | search window (default 7, or `LOOKBACK_DAYS` env) |
| `--cats LP,NOFC`    | only these category codes (default: all) |
| `--grouped`         | one combined search instead of one-per-category |
| `--headed`          | show the browser (default headless) |
| `--no-enrich`       | skip ArcGIS parcel enrichment |
| `--max-pages N`     | result-page pagination cap per search (default 25) |
| `--dry-run`         | skip the live scrape, just rewrite empty output |
| `--verbose`         | debug logging |

Category codes: `LP, NOFC, TAXDEED, JUD, CCJ, DRJUD, LNCORPTX, LNIRS, LNFED,
LN, LNMECH, LNHOA, MEDLN, PRO, NOC, RELLP`.

## How it works

1. **Disclaimer gate** — the portal (Aumentum Recorder Public Access, Harris)
   shows a disclaimer on each new session. The scraper auto-detects and clicks
   *"Click here to acknowledge the disclaimer and enter the site"* every time.
2. **Search form** — fills **Date Filed From/To** for the lookback window,
   leaves Party Name / Instrument / Book / Page blank (countywide), and ticks the
   **Document-Type** checklist items that match each category's alias keywords
   (resilient to scrolling & partially-visible items).
3. **One search per category** (or `--grouped`), logging exactly which doc types
   were searched and how many rows each returned.
4. **Results parsing** — extracts doc number, filed date, doc type, grantor,
   grantee, legal, amount, and the record link from the results grid.
5. **Enrichment** — joins to the ArcGIS parcel layer to add **property (situs)**
   and **mailing** addresses (see the note below).
6. **Scoring** — assigns a 0-100 motivated-seller score + flags.
7. **Output** — writes `records.json` (×2) and `leads_ghl.csv` (×2).

## Scoring

Base **30**, then:

- **+10** per flag
- **+20** lis-pendens **+** foreclosure combo (same owner)
- **+15** amount > $100k  *(tiered — takes precedence over the $50k tier)*
- **+10** amount > $50k
- **+5** new this week
- **+5** has a usable address

Flags: `Lis pendens`, `Pre-foreclosure`, `Judgment lien`, `Tax lien`,
`Mechanic lien`, `Probate / estate`, `LLC / corp owner`, `New this week`
(+ `Lis pendens + foreclosure combo` when both apply to one owner).

## Portal specifics handled automatically

- **Anti-bot challenge.** The portal sits behind an F5/Shape "TSPD" JavaScript
  challenge that blocks vanilla automation. The scraper runs a stealth Chromium
  context (masks `navigator.webdriver`, realistic UA/locale/timezone) and waits
  out / reloads the interstitial until the disclaimer renders.
- **Disclaimer gate.** Clicked automatically every session ("acknowledge the
  disclaimer and enter the site").
- **~300-row cap.** A single Aumentum search returns at most ~300 rows,
  **oldest-first** — so a busy category over a 7-day window would silently drop
  the *newest* (most valuable) filings. When a search hits the cap, the scraper
  automatically **re-runs that category day-by-day** and unions the results
  (global dedup handles overlap), guaranteeing completeness. For daily cron runs
  with a small lookback this rarely triggers.

## ⚠️ Important enrichment note (read this)

The Alameda County ArcGIS parcel layer exposes situs + mailing addresses, APN,
book/page/parcel, and assessed value — **but no owner-name field.** There is
therefore **no owner→parcel join key** in this dataset.

Automatic enrichment fills addresses only when a record yields:

1. an **APN** parsed from the legal description, or
2. a **situs address** already present on the record, or
3. (future) an owner match, *if* you point `ArcGISEnricher.find_parcels_by_owner()`
   at an owner-indexed dataset (e.g. the Assessor secured roll). The owner
   name-variant logic (`FIRST LAST` / `LAST FIRST` / `LAST, FIRST`) is built and
   ready for that; it just has nothing to match against in the public parcel
   layer today. A `dbfread`-based bulk loader (`load_owner_index_from_dbf`) is
   included for the same purpose if a bulk `.dbf` becomes available.

Recorder result rows frequently lack both APN and address, so expect a portion
of leads to have no enriched address until an owner index is wired in. The
`with_address` count in the output tells you the coverage for each run.

## GitHub Actions

`.github/workflows/scrape.yml`:

- Runs daily at **07:00 UTC** (`schedule`) and on demand (`workflow_dispatch`,
  with optional `lookback_days` / `cats` inputs).
- Installs deps + `playwright install --with-deps chromium`.
- Runs `python scraper/fetch.py`.
- Commits the refreshed `records.json` / `leads_ghl.csv` files.
- Deploys `dashboard/` to **GitHub Pages**.

Enable Pages: **Settings → Pages → Source: GitHub Actions**.

## Failure attribution

Every stage logs a tag so you can see exactly where a run struggled:
`DISCLAIMER`, `SEARCH_FORM`, `RESULTS`, `PARSING`, `ENRICHMENT`, `EXPORT`.
The pipeline never hard-crashes — partial/empty output is always written.

## Legal / ethical

This tool reads **public records** at a polite, once-daily cadence (one search
per document type). It does not bypass authentication, evade rate limits, or
access non-public data. Use leads in compliance with the FDCPA, TCPA, DNC, and
local solicitation rules.
