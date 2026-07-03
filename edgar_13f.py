#!/usr/bin/env python3
"""
edgar_13f.py â€” SEC EDGAR 13F aggregator for the 13F Tracker dashboard.

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

# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ configuration â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

USER_AGENT = os.environ.get("EDGAR_UA", "13FTracker your-email@example.com")

# Ticker -> 6-char CUSIP issuer prefix (first 6 of the 9-char CUSIP).
# Full CUSIP works too; the prefix also catches filers who list option
# CUSIPs under the same issuer number. Verify each prefix in any 13F filing
# via EDGAR full-text search before trusting the numbers.
TICKERS = {
    "SNDK": {"name": "SANDISK CORP", "issuer6": "80004C", "com_cusip": "80004C200"},
    # "MU":   {"name": "MICRON TECHNOLOGY", "issuer6": "595112", "com_cusip": "595112103"},
    # "NVDA": {"name": "NVIDIA CORP",       "issuer6": "67066G", "com_cusip": "67066G104"},
}

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


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ step 1: full-text search â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

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


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ step 2: parse infotables â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

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


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ step 3/4: snapshot & diff â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

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

    print(f"    {len(filings)} unique filers; parsing infotablesâ€¦")
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
            print(f"      â€¦{i}/{len(filings)}")

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
                                "shares": "â€”", "value": "â€”"})
    order = {"NEW": 0, "INCREASED": 1, "DECREASED": 2, "SOLD OUT": 3}
    signals.sort(key=lambda s: order[s["action"]])
    return {"new": neu, "inc": inc, "dec": dec, "sold": sold,
            "signals": signals, "changes": changes}


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ step 5: emit dashboard JSON â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

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
                         else "â€”")}
                for cik, p in top
            ],
        }
    OUT_FILE.parent.mkdir(parents=True, exist_okÕG'VR¢õUEôd”ÄRçw&—FU÷FW‡B†§6öâæGV×2†÷WBÂ–æFVçCÓÂVç7W&Uö66–“ÔfÇ6R’¢&–çB†b%Æî)É2w&÷FR´õUEôd”ÄWÒ"  ¢2)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)HÖ–â)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H)H  ¦FVbÆFW7Eö6ö×ÆWFVE÷V'FW"‚’Óâ7G# ¢""$ÆFW7BV'FW"v†÷6RCRÖF’4bf–Æ–ærFVFÆ–æR†2Ç&VG’76VBÀ¢6òÖ÷7B–ç7F—GWF–öç2†fR7GVÆÇ’f–ÆVB†æ÷B§W7BF†RV&Æ–W7BöæW2’â"" ¢BÒFFRçFöF’‚¢Ò²ƒ2Â3’ÂƒbÂ3’Âƒ’Â3’Âƒ"Â3•Ð¢VæG2Ò¶FFR‡Bç–V"ÒÂ"Â3•Ò²¶FFR‡Bç–V"ÂÒÂB’f÷"ÒÂB–âÐ¢&WGW&âÖ‚†Rf÷"R–âVæG2–bR²F–ÖVFVÇF†F—3ÓCR’ÂB’æ—6öf÷&ÖB‚  ¦FVbÖ–â‚“ ¢Ò&w'6Rä&wVÖVçE'6W"‚¢æFEö&wVÖVçB‚"Ò×V'FW""Â†VÇÒ'V'FW"VæB•••’ÔÔÒÔDB†FVfVÇC¢ÆFW7B6ö×ÆWFVB’"¢æFEö&wVÖVçB‚"ÒÖ&6¶f–ÆÂ"ÂG—SÖ–çBÂFVfVÇCÓÂ†VÇÒ&çVÖ&W"öbV'FW'2VæF–ærBÒ×V'FW""¢æFEö&wVÖVçB‚"Ò×F–6¶W'2"Â†VÇÒ&6öÖÖ×6W&FVB7V'6WBöb6öæf–wW&VBF–6¶W'2"¢&w2Òç'6Uö&w2‚ ¢–b'–÷W"ÖVÖ–Â"–âU4U%ôtTåC ¢7—2æW†—B‚%6WBTDt%õTVçbf"FòtæÖR–÷W"×&VÂÖVÖ–Ârf—'7B…4T2&WV—&VÖVçB’â" ¢F–6¶W'2Ò&w2çF–6¶W'2ç7Æ—B‚"Â"’–b&w2çF–6¶W'2VÇ6RÆ—7B…D”4´U%2¢VæBÒ&w2çV'FW"÷"ÆFW7Eö6ö×ÆWFVE÷V'FW"‚ ¢V'FW'2Ò·VæEÐ¢f÷"ò–â&ævR†&w2æ&6¶f–ÆÂÒ“ ¢V'FW'2æ–ç6W'BƒÂ&We÷V'FW"‡V'FW'5³Ò’¢2æVVBöæRW‡G&V'FW"&Vf÷&RF†Rf—'7Bf÷"F†RF–f`¢–bÆöE÷6æ6†÷B‡F–6¶W'5³ÒÂ&We÷V'FW"‡V'FW'5³Ò’’—2æöæRæB&w2æ&6¶f–ÆÂÓÒ ¢722f—'7B×'Vã¢f—'7BV'FW"w26†ævW2v–ÆÂÆÂ6†÷r2äUp ¢f÷"F²–âF–6¶W'3 ¢&–çB†b%ÆãÓÓÒ·F·ÒÓÓÒ"¢f÷"–âV'FW'3 ¢&–çB†b"V'FW"·Ò"¢–bÆöE÷6æ6†÷B‡F²Â’—2æ÷BæöæRæBÒV'FW'5²ÓÓ ¢&–çB‚"6æ6†÷BW†—7G2Â6¶—–ær†ÆFW7BV'FW"Çv—2&Vg&W6†VB’"¢6öçF–çVP¢'V–ÆE÷6æ6†÷B‡F²Â ¢VÖ—B‡V'FW'2  ¦–bõöæÖUõòÓÒ%õöÖ–åõò# ¢Ö–â‚