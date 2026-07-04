#!/usr/bin/env python3
"""
edgar_13f.py — SEC EDGAR 13F aggregator for the 13F Tracker dashboard.

Pipeline (per ticker, per quarter):
  1. EDGAR full-text search (efts.sec.gov) for the CUSIP within the filing
     window -> list of 13F-HR filings (accession + infotable filename + CIK).
  2. Download & cache each infotable XML, stream-parse it, keep only rows
     whose CUSIP starts with the issuer's 6-char prefix (COM shares only).
  3. Save a per-quarter snapshot {cik: {shares, value}}.
  4. Diff vs. the prior quarter's snapshot -> new / increased / decreased /
     sold-out counts, plus per-fund signals for your watchlist CIKs.
  5. Emit data.json in the schema the dashboard reads.

Usage:
  python edgar_13f.py --quarter 2026-03-31            # one quarter
  python edgar_13f.py --quarter 2026-03-31 --tickers SNDK
  python edgar_13f.py --backfill 4                    # last 4 quarters
  python edgar_13f.py --add AVGO                      # add ticker, CUSIP auto

Notes:
  * SEC fair-access policy: <=10 req/s and a User-Agent identifying you.
    Set EDGAR_UA env var or edit USER_AGENT below. Do NOT leave the default.
  * 13F values are reported in full USD (post-2023 rule change).
  * Filings are cached under .cache/ so re-runs during filing season
    (the ~45 days after quarter end) only fetch new filings.
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from xml.etree import ElementTree as ET

import requests

# ──────────────────────────── configuration ────────────────────────────

USER_AGENT = os.environ.get("EDGAR_UA", "13FTracker your-email@example.com")

# Ticker config lives in data/tickers.json (committed to the repo) so new
# tickers can be added without touching code: python edgar_13f.py --add AVGO
TICKERS_FILE = Path("data/tickers.json")
DEFAULT_TICKERS = {
    "SNDK": {"name": "SANDISK CORP", "issuer6": "80004C", "com_cusip": "80004C200"},
    "MU":   {"name": "MICRON TECHNOLOGY", "issuer6": "595112", "com_cusip": "595112103"},
}
TICKERS = dict(DEFAULT_TICKERS)  # replaced by load_tickers() in main()


def load_tickers() -> dict:
    if TICKERS_FILE.exists():
        return json.loads(TICKERS_FILE.read_text())
    return dict(DEFAULT_TICKERS)


def save_tickers(t: dict):
    TICKERS_FILE.parent.mkdir(parents=True, exist_ok=True)
    TICKERS_FILE.write_text(json.dumps(t, indent=1, ensure_ascii=False))

# Watchlist funds (CIK -> display name) that trigger "Watchlist Signal" cards.
WATCHLIST_FUNDS = {
    "1656456": "Appaloosa LP",
    "1350694": "Bridgewater Associates, LP",
    "1423053": "Citadel Advisors LLC",
    "1273087": "Millennium Management LLC",
    "1067983": "Berkshire Hathaway Inc",
    "1037389": "Renaissance Technologies LLC",
    "1009207": "D. E. Shaw & Co., Inc.",
}

CACHE_DIR = Path(".cache")
SNAP_DIR = Path("data/snapshots")
OUT_FILE = Path("data/data.json")

FTS_URL = "https://efts.sec.gov/LATEST/search-index"
ARCHIVE_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{acc_nodash}/{fname}"

REQ_INTERVAL = 0.13  # ~7.5 req/s, under SEC's 10 req/s limit
_last_req = [0.0]

session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT, "Accept-Encoding": "gzip"})


def _get(url, **kw):
    """Rate-limited GET with basic retry."""
    for attempt in range(4):
        wait = REQ_INTERVAL - (time.monotonic() - _last_req[0])
        if wait > 0:
            time.sleep(wait)
        _last_req[0] = time.monotonic()
        r = session.get(url, timeout=60, **kw)
        if r.status_code == 429 or r.status_code >= 500:
            time.sleep(2 ** attempt)
            continue
        r.raise_for_status()
        return r
    r.raise_for_status()


# ──────────────────────────── step 1: full-text search ────────────────────────────

def quarter_window(qend: str):
    """Filing window: quarter end -> ~100 days after (45-day deadline + amendments)."""
    d = datetime.strptime(qend, "%Y-%m-%d").date()
    return (d + timedelta(days=1)).isoformat(), (d + timedelta(days=100)).isoformat()


def fts_filings(cusip_or_prefix: str, qend: str):
    """Yield {cik, adsh, fname, file_date} for 13F-HR filings hitting the CUSIP."""
    startdt, enddt = quarter_window(qend)
    frm, total = 0, None
    while True:
        params = {
            "q": f'"{cusip_or_prefix}"',
            "forms": "13F-HR",
            "startdt": startdt,
            "enddt": enddt,
            "from": frm,
        }
        d = _get(FTS_URL, params=params).json()
        hits = d["hits"]["hits"]
        if total is None:
            total = d["hits"]["total"]["value"]
            print(f"    FTS: {total} filings match {cusip_or_prefix} in {startdt}..{enddt}")
        if not hits:
            break
        for h in hits:
            src = h["_source"]
            if src.get("period_ending") and src["period_ending"] != qend:
                continue  # a filer's other-period filing caught in the window
            adsh, fname = h["_id"].split(":", 1)
            yield {
                "cik": str(int(src["ciks"][0])),
                "adsh": adsh,
                "fname": fname,
                "file_date": src["file_date"],
                "name": src["display_names"][0].split("  (CIK")[0],
            }
        frm += len(hits)
        if frm >= min(total, 10000):
            break


# ──────────────────────────── step 2: parse infotables ────────────────────────────

def fetch_infotable(cik: str, adsh: str, fname: str) -> Path:
    """Download infotable XML to cache (skip if present). Returns local path."""
    acc_nodash = adsh.replace("-", "")
    p = CACHE_DIR / adsh / fname
    if p.exists() and p.stat().st_size > 0:
        return p
    p.parent.mkdir(parents=True, exist_ok=True)
    url = ARCHIVE_URL.format(cik=cik, acc_nodash=acc_nodash, fname=fname)
    r = _get(url)
    p.write_bytes(r.content)
    return p


_TAG = re.compile(r"\{.*\}")  # strip xml namespace


def parse_positions(xml_path: Path, issuer6: str):
    """Stream-parse an infotable; return aggregated COM position {shares, value}.

    Sums across multiple rows (some filers split by discretion/manager).
    Ignores PUT/CALL rows and non-SH principal amounts.
    """
    shares, value = 0, 0
    try:
        for _, el in ET.iterparse(str(xml_path), events=("end",)):
            if _TAG.sub("", el.tag) != "infoTable":
                continue
            row = {}
            for c in el.iter():
                row[_TAG.sub("", c.tag)] = (c.text or "").strip()
            el.clear()
            cusip = row.get("cusip", "").upper()
            if not cusip.startswith(issuer6.upper()):
                continue
            if row.get("putCall"):
                continue
            if row.get("sshPrnamtType", "SH") != "SH":
                continue
            shares += int(float(row.get("sshPrnamt", 0) or 0))
            value += int(float(row.get("value", 0) or 0))
    except ET.ParseError as e:
        print(f"    ! parse error {xml_path}: {e}")
        return None
    if shares == 0 and value == 0:
        return None  # e.g. only option rows matched
    return {"shares": shares, "value": value}


# ──────────────────────────── ticker -> CUSIP auto-resolve ────────────────────────────

REF_FILER_CIK = "102909"  # Vanguard Group – holds nearly every US-listed name

_SUFFIX = re.compile(r"\b(INC|INCORPORATED|CORP|CORPORATION|CO|COM|COMPANY|LTD|PLC|SA|NV|NEW|DEL|THE|HOLDINGS|HOLDING|HLDGS|HLDG|GROUP|GRP|CL|CLASS|ADR|ADS|SHS|SPON|SPONS|SPONSORED|REPSTG|COMMON|STOCK|ORD)\b")


def _norm_name(s: str) -> str:
    s = re.sub(r"[^A-Z0-9 ]", " ", s.upper())
    s = _SUFFIX.sub(" ", s)
    return re.sub(r"\s+", " ", s).strip()


def _latest_ref_infotable() -> Path:
    """Path to the newest 13F-HR infotable of the reference filer (cached)."""
    subs = _get(f"https://data.sec.gov/submissions/CIK{int(REF_FILER_CIK):010d}.json").json()
    recent = subs["filings"]["recent"]
    adsh = next(a for f, a in zip(recent["form"], recent["accessionNumber"]) if f == "13F-HR")
    acc_nodash = adsh.replace("-", "")
    idx = _get(f"https://www.sec.gov/Archives/edgar/data/{REF_FILER_CIK}/{acc_nodash}/index.json").json()
    xmls = [i for i in idx["directory"]["item"] if i["name"].lower().endswith(".xml")
            and "primary_doc" not in i["name"].lower()]
    fname = max(xmls, key=lambda i: int(i.get("size") or 0))["name"]
    return fetch_infotable(REF_FILER_CIK, adsh, fname)


def _iter_rows(xml_path: Path):
    """Yield (norm_name, value, cusip, issuer_name) for non-option rows."""
    for _, el in ET.iterparse(str(xml_path), events=("end",)):
        if _TAG.sub("", el.tag) != "infoTable":
            continue
        row = {}
        for c in el.iter():
            row[_TAG.sub("", c.tag)] = (c.text or "").strip()
        el.clear()
        got = _norm_name(row.get("nameOfIssuer", ""))
        if not got or row.get("putCall") or not row.get("cusip"):
            continue
        yield (got, int(float(row.get("value", 0) or 0)),
               row["cusip"].upper(), row.get("nameOfIssuer", ""))


def _pick_match(rows, want: str):
    want_t = want.split()

    def pick(pred):
        c = [r for r in rows if pred(r[0])]
        return max(c, key=lambda r: r[1]) if c else None

    best = pick(lambda g: g == want or g.startswith(want) or want.startswith(g))
    if not best and len(want_t) >= 2:
        best = pick(lambda g: g.split()[:2] == want_t[:2])
    return best


def resolve_cusip(ticker: str) -> dict:
    """Ticker -> {name, issuer6, com_cusip} via SEC mapping + reference filings."""
    mapping = _get("https://www.sec.gov/files/company_tickers.json").json()
    title = next((v["title"] for v in mapping.values()
                  if v["ticker"].upper() == ticker.upper()), None)
    if not title:
        sys.exit(f"ticker {ticker!r} not found in SEC company_tickers.json")
    want = _norm_name(title)
    print(f"  resolving {ticker} ({title}) via reference 13F…")

    # source 1: Vanguard's latest 13F (covers nearly all US-listed common)
    best = _pick_match(list(_iter_rows(_latest_ref_infotable())), want)

    # source 2: FTS phrase search — any filer holding the name (covers ADRs etc.)
    if not best:
        phrase = " ".join(want.split()[:2]) if len(want.split()) >= 2 else want
        startdt, enddt = quarter_window(latest_completed_quarter())
        d = _get(FTS_URL, params={"q": f'"{phrase}"', "forms": "13F-HR",
                                  "startdt": startdt, "enddt": enddt}).json()
        for h in d["hits"]["hits"][:5]:
            adsh, fname = h["_id"].split(":", 1)
            cik = str(int(h["_source"]["ciks"][0]))
            try:
                best = _pick_match(list(_iter_rows(fetch_infotable(cik, adsh, fname))), want)
            except Exception:
                continue
            if best:
                break
    if not best:
        sys.exit(f"could not find {title!r} in reference filings — pass CUSIP manually "
                 f"by editing data/tickers.json")
    _, _, cusip, issuer = best
    print(f"  ✓ {ticker}: {issuer} -> CUSIP {cusip}")
    return {"name": issuer.upper(), "issuer6": cusip[:6], "com_cusip": cusip}


# ──────────────────────────── step 3/4: snapshot & diff ────────────────────────────




def build_snapshot(ticker: str, qend: str) -> dict:
    cfg = TICKERS[ticker]
    snap_path = SNAP_DIR / ticker / f"{qend}.json"
    snap_path.parent.mkdir(parents=True, exist_ok=True)

    # latest filing per CIK wins (amendments / restatements)
    # NOTE: FTS matches whole tokens, so search the full 9-char COM CUSIP;
    # the 6-char issuer prefix is only used when parsing rows.
    filings = {}
    for f in fts_filings(cfg["com_cusip"], qend):
        cur = filings.get(f["cik"])
        if cur is None or f["file_date"] > cur["file_date"]:
            filings[f["cik"]] = f

    print(f"    {len(filings)} unique filers; parsing infotables…")
    snapshot = {}
    for i, (cik, f) in enumerate(sorted(filings.items()), 1):
        try:
            path = fetch_infotable(cik, f["adsh"], f["fname"])
            pos = parse_positions(path, cfg["issuer6"])
        except Exception as e:
            print(f"    ! {f['adsh']} ({f['name']}): {e}")
            continue
        if pos:
            snapshot[cik] = {**pos, "name": f["name"]}
        if i % 100 == 0:
            print(f"      …{i}/{len(filings)}")

    snap_path.write_text(json.dumps(snapshot, indent=1))
    print(f"    snapshot saved: {snap_path} ({len(snapshot)} holders)")
    return snapshot


def load_snapshot(ticker: str, qend: str):
    p = SNAP_DIR / ticker / f"{qend}.json"
    return json.loads(p.read_text()) if p.exists() else None


def prev_quarter(qend: str) -> str:
    d = datetime.strptime(qend, "%Y-%m-%d").date()
    m = d.month - 3
    y = d.year + (m <= 0) * -1 + (0 if m > 0 else 0)
    if m <= 0:
        m += 12
        y = d.year - 1
    else:
        y = d.year
    last = {3: 31, 6: 30, 9: 30, 12: 31}[m]
    return date(y, m, last).isoformat()


def fmt_shares(n):
    return f"{n/1e9:.2f}B" if n >= 1e9 else f"{n/1e6:.1f}M" if n >= 1e6 else f"{n/1e3:.1f}K" if n >= 1e3 else str(n)


def fmt_usd(v):
    return f"${v/1e9:.1f}B" if v >= 1e9 else f"${v/1e6:.1f}M" if v >= 1e6 else f"${v/1e3:.0f}K"


def diff_quarter(cur: dict, prev: dict | None):
    neu = inc = dec = sold = 0
    signals, changes = [], {}
    prev = prev or {}
    for cik, p in cur.items():
        q = prev.get(cik)
        if q is None:
            neu += 1
            act, chg = "NEW", None
        elif p["shares"] > q["shares"]:
            inc += 1
            act, chg = "INCREASED", (p["shares"] - q["shares"]) / q["shares"] * 100 if q["shares"] else None
        elif p["shares"] < q["shares"]:
            dec += 1
            act, chg = "DECREASED", (p["shares"] - q["shares"]) / q["shares"] * 100
        else:
            act, chg = "UNCHANGED", 0.0
        changes[cik] = (act, chg)
        if cik in WATCHLIST_FUNDS and act != "UNCHANGED":
            signals.append({
                "fund": WATCHLIST_FUNDS[cik], "action": act,
                "shares": fmt_shares(p["shares"]), "value": fmt_usd(p["value"]),
                **({"chg": f"{chg:+.0f}%"} if chg is not None else {}),
            })
    for cik, q in prev.items():
        if cik not in cur:
            sold += 1
            if cik in WATCHLIST_FUNDS:
                signals.append({"fund": WATCHLIST_FUNDS[cik], "action": "SOLD OUT",
                                "shares": "—", "value": "—"})
    order = {"NEW": 0, "INCREASED": 1, "DECREASED": 2, "SOLD OUT": 3}
    signals.sort(key=lambda s: order[s["action"]])
    return {"new": neu, "inc": inc, "dec": dec, "sold": sold,
            "signals": signals, "changes": changes}


# ──────────────────────────── step 5: emit dashboard JSON ────────────────────────────

def emit(quarters_done: list[str]):
    out = {"generated": datetime.utcnow().isoformat() + "Z", "quarters": quarters_done, "stocks": {}}
    for tk, cfg in TICKERS.items():
        tl, prev_snap = [], None
        for q in quarters_done:
            snap = load_snapshot(tk, q)
            if snap is None:
                tl.append(None)
                prev_snap = None
                continue
            d = diff_quarter(snap, prev_snap)
            tl.append({
                "q": q,
                "holders": len(snap),
                "valueB": round(sum(p["value"] for p in snap.values()) / 1e9, 2),
                "neu": d["new"], "inc": d["inc"], "dec": d["dec"], "sold": d["sold"],
            })
            last_diff, last_snap = d, snap
            prev_snap = snap
        if prev_snap is None:
            continue
        top = sorted(last_snap.items(), key=lambda kv: -kv[1]["value"])[:15]
        out["stocks"][tk] = {
            "name": cfg["name"],
            "sharesM": round(sum(p["shares"] for p in last_snap.values()) / 1e6, 1),
            "tl": tl,
            "signals": last_diff["signals"],
            "holders": [
                {"fund": p["name"], "shares": fmt_shares(p["shares"]), "value": fmt_usd(p["value"]),
                 "chg": ("NEW" if last_diff["changes"][cik][0] == "NEW"
                         else f"{last_diff['changes'][cik][1]:+.1f}%" if last_diff["changes"][cik][1] is not None
                         else "—")}
                for cik, p in top
            ],
        }
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUT_FILE.write_text(json.dumps(out, indent=1, ensure_ascii=False))
    print(f"\n✓ wrote {OUT_FILE}")


# ──────────────────────────── main ────────────────────────────

def latest_completed_quarter() -> str:
    """Latest quarter whose 45-day 13F filing deadline has already passed,
    so most institutions have actually filed (not just the earliest ones)."""
    t = date.today()
    q = [(3, 31), (6, 30), (9, 30), (12, 31)]
    ends = [date(t.year - 1, 12, 31)] + [date(t.year, m, d) for m, d in q]
    return max(e for e in ends if e + timedelta(days=45) < t).isoformat()


def main():
    global TICKERS
    ap = argparse.ArgumentParser()
    ap.add_argument("--quarter", help="quarter end YYYY-MM-DD (default: latest completed)")
    ap.add_argument("--backfill", type=int, default=1, help="number of quarters ending at --quarter")
    ap.add_argument("--tickers", help="comma-separated subset of configured tickers")
    ap.add_argument("--add", help="add new ticker(s) by symbol, e.g. AVGO or AVGO,TSM (CUSIP auto-resolved)")
    args = ap.parse_args()

    if "your-email" in USER_AGENT:
        sys.exit("Set EDGAR_UA env var to 'AppName your-real-email' first (SEC requirement).")

    TICKERS = load_tickers()
    if args.add:
        for tk in args.add.split(","):
            tk = tk.strip().upper()
            if not tk:
                continue
            if tk in TICKERS:
                print(f"  {tk} already tracked, skipping add")
                continue
            TICKERS[tk] = resolve_cusip(tk)
    save_tickers(TICKERS)  # persist config (incl. first-run defaults)

    tickers = args.tickers.split(",") if args.tickers else list(TICKERS)
    qend = args.quarter or latest_completed_quarter()

    quarters = [qend]
    for _ in range(args.backfill - 1):
        quarters.insert(0, prev_quarter(quarters[0]))
    # need one extra quarter before the first for the diff
    if load_snapshot(tickers[0], prev_quarter(quarters[0])) is None and args.backfill == 1:
        pass  # first-run: first quarter's changes will all show as NEW

    for tk in tickers:
        print(f"\n=== {tk} ===")
        for q in quarters:
            print(f"  quarter {q}")
            if load_snapshot(tk, q) is not None and q != quarters[-1]:
                print("    snapshot exists, skipping (latest quarter always refreshed)")
                continue
            build_snapshot(tk, q)

    emit(quarters)


if __name__ == "__main__":
    main()
