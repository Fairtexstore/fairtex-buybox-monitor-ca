import csv
import gzip
import io
import json
import os
import time
import requests
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

LWA_TOKEN_URL  = "https://api.amazon.com/auth/o2/token"
MARKETPLACE_ID = os.environ.get("AMAZON_MARKETPLACE_ID", "A2EUQ1WTGCTBG2")
US_MARKETPLACE = "ATVPDKIKX0DER"
SP_API_BASE    = "https://sellingpartnerapi-na.amazon.com"
SLACK_CHANNEL  = "C0AMDJ91151"
MY_SELLER_ID   = "A1LC1HJLF7IAWT"
NARF_IMPORT_FEE_RATE = 0.14  # 14% import/customs fee for NARF cross-border orders
# Amazon's internal USD→CAD rate for FBA cross-border sellers.
# SP-API returns USD for FBA products. This rate converts to the actual CAD price
# shown on Seller Central (e.g. $227.47 USD × 1.1447 = $260.39 CAD).
# NARF products already return CAD from the API — no conversion needed.
AMAZON_USD_CAD_RATE = 1.1447

# MSRP violations tracking (competitors priced below Fairtex MSRP)
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
VIOLATIONS_HISTORY_PATH = os.path.join(_REPO_ROOT, "violations_history.json")
VIOLATIONS_SUMMARY_PATH = os.path.join(_REPO_ROOT, "dashboard", "data", "violations_summary.json")
VIOLATIONS_RETENTION_DAYS = 65
MARKET_CODE = "CA"



def get_lwa_access_token():
    resp = requests.post(LWA_TOKEN_URL, data={
        "grant_type":    "refresh_token",
        "refresh_token": os.environ["AMAZON_REFRESH_TOKEN"],
        "client_id":     os.environ["AMAZON_CLIENT_ID"],
        "client_secret": os.environ["AMAZON_CLIENT_SECRET"],
    })
    resp.raise_for_status()
    return resp.json()["access_token"]


def sp_api_headers(access_token):
    return {
        "x-amz-access-token": access_token,
        "Content-Type":       "application/json",
        "Accept":             "application/json",
    }


_seller_names = {}
_SELLER_NAMES_PATH = None  # set lazily


def _seller_names_path():
    global _SELLER_NAMES_PATH
    if _SELLER_NAMES_PATH is None:
        _SELLER_NAMES_PATH = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "seller_names.json"
        )
    return _SELLER_NAMES_PATH


def _load_seller_names():
    global _seller_names
    try:
        with open(_seller_names_path(), "r", encoding="utf-8") as f:
            _seller_names = json.load(f)
    except Exception:
        _seller_names = {}


def _save_seller_names():
    with open(_seller_names_path(), "w", encoding="utf-8") as f:
        json.dump(_seller_names, f, indent=2, sort_keys=True)


import re as _re
_SELLER_PROFILE_URL_TMPL = "https://www.amazon.ca/sp?seller={}"
_SELLER_NAME_TITLE_RE = _re.compile(
    r"<title>\s*Amazon\.ca Seller Profile:\s*(.+?)\s*</title>", _re.IGNORECASE | _re.DOTALL
)


def enrich_seller_names(seller_ids):
    """For every previously-unseen seller ID, fetch the amazon.ca profile page
    and pull the store name out of the <title>. Cached to seller_names.json so
    subsequent runs don't re-scrape."""
    if not _seller_names:
        _load_seller_names()
    to_fetch = [
        sid for sid in seller_ids
        if sid and sid != MY_SELLER_ID and sid not in _seller_names
        and sid not in ("Unknown", "No winner")
    ]
    if not to_fetch:
        return
    print(f"  Fetching store names for {len(to_fetch)} new seller IDs...")
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-CA,en;q=0.9",
    }
    added = 0
    for i, sid in enumerate(to_fetch):
        try:
            resp = requests.get(_SELLER_PROFILE_URL_TMPL.format(sid), headers=headers, timeout=15)
            if resp.status_code == 200:
                m = _SELLER_NAME_TITLE_RE.search(resp.text)
                if m:
                    name = m.group(1).strip()
                    if name and len(name) < 200:
                        _seller_names[sid] = name
                        added += 1
        except Exception:
            pass
        time.sleep(0.4)
        if (i + 1) % 25 == 0:
            print(f"  Seller name fetch progress: {i + 1}/{len(to_fetch)}")
    if added:
        _save_seller_names()
    print(f"  Resolved {added} new store names (cache now: {len(_seller_names)})")


def seller_profile_url(seller_id):
    if not seller_id or seller_id in ("Unknown", "No winner"):
        return ""
    return _SELLER_PROFILE_URL_TMPL.format(seller_id)


def get_seller_name(seller_id):
    """Look up seller display name from seller_names.json. Falls back to seller ID."""
    if not _seller_names:
        _load_seller_names()
    return _seller_names.get(seller_id, seller_id)


def load_product_costs():
    """Load product cost data from product_costs.csv. Returns dict keyed by ASIN.
    Each entry has fba_cost and narf_cost; caller selects the right one based on fulfillment type.
    """
    path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "product_costs.csv")
    costs = {}
    try:
        with open(path, "r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                asin = row.get("ASIN", "").strip()
                if not asin:
                    continue
                def _parse(val):
                    v = (val or "").strip().replace("$", "").replace(",", "")
                    try:
                        return float(v) if v else None
                    except ValueError:
                        return None
                costs[asin] = {
                    "fba_cost":  _parse(row.get("FBA_Cost")),
                    "narf_cost": _parse(row.get("NARF_Cost")),
                }
        print(f"  Loaded cost data for {len(costs)} ASINs")
    except FileNotFoundError:
        print("  product_costs.csv not found - skipping cost data")
    except Exception as e:
        print(f"  Error loading product_costs.csv: {e}")
    return costs


def load_fairtex_msrp():
    """Load Fairtex MSRP (CAD) from 'Fairtex Price in USD.csv'. Returns dict ASIN -> float CAD.

    The CAD column is refreshed monthly by convert_costs.py at the current Bank of Canada rate.
    """
    path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Fairtex Price in USD.csv")
    msrp = {}
    try:
        with open(path, "r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                asin = (row.get("ASIN") or "").strip()
                if not asin:
                    continue
                cad_str = (row.get("MSRP per Fairtex in CAD") or "").strip().replace("$", "").replace(",", "")
                if not cad_str:
                    continue
                try:
                    msrp[asin] = float(cad_str)
                except ValueError:
                    pass
        print(f"  Loaded Fairtex MSRP for {len(msrp)} ASINs")
    except FileNotFoundError:
        print("  'Fairtex Price in USD.csv' not found - skipping Fairtex MSRP")
    except Exception as e:
        print(f"  Error loading Fairtex MSRP: {e}")
    return msrp


def _parse_money(val):
    if val is None or val == "":
        return None
    try:
        return float(str(val).replace("$", "").replace(",", ""))
    except (ValueError, TypeError):
        return None


# ----------------------------------------------------------------------
# MSRP violations tracking — daily capture, rolling history, monthly rollup
# ----------------------------------------------------------------------

def _load_violations_history():
    try:
        with open(VIOLATIONS_HISTORY_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _save_violations_history(history):
    os.makedirs(os.path.dirname(VIOLATIONS_HISTORY_PATH), exist_ok=True)
    with open(VIOLATIONS_HISTORY_PATH, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=2, default=str)


def _prune_violations_history(history, retention_days):
    """Drop entries older than retention_days."""
    cutoff = (datetime.now(timezone.utc).date() - timedelta(days=retention_days)).isoformat()
    for date_key in list(history.keys()):
        if date_key < cutoff:
            del history[date_key]


def _record_todays_violations(inventory, buy_box_map, fairtex_msrp_map):
    """Return (today_iso, list of violations) using listing_price vs Fairtex MSRP."""
    today = datetime.now(timezone.utc).date().isoformat()
    seen = set()  # dedup within one day per (market, asin, seller_id)
    violations = []
    for item in inventory:
        asin = item["asin"]
        fairtex_cad = fairtex_msrp_map.get(asin)
        if fairtex_cad is None:
            continue
        info = buy_box_map.get(item["sku"], {})
        for offer in info.get("competitor_offers", []) or []:
            listing_price = offer.get("listing_price")
            if listing_price is None:
                continue
            if listing_price >= fairtex_cad - 0.01:
                continue
            key = (MARKET_CODE, asin, offer["seller_id"])
            if key in seen:
                continue
            seen.add(key)
            violations.append({
                "market":        MARKET_CODE,
                "asin":          asin,
                "sku":           item["sku"],
                "product_name":  item.get("name", ""),
                "seller_id":     offer["seller_id"],
                "seller_name":   get_seller_name(offer["seller_id"]),
                "seller_url":    seller_profile_url(offer["seller_id"]),
                "seller_price":  listing_price,
                "shipping":      offer.get("shipping", 0),
                "landed_price":  offer.get("landed_price", listing_price),
                "fairtex_msrp":  fairtex_cad,
                "currency":      offer.get("currency", "CAD"),
            })
    return today, violations


def _build_violations_summary(history):
    """Roll up current + previous month by (market, asin, seller_id)."""
    today = datetime.now(timezone.utc).date()
    current_month = today.strftime("%Y-%m")
    prev_year, prev_month_num = (today.year - 1, 12) if today.month == 1 else (today.year, today.month - 1)
    prev_month = f"{prev_year:04d}-{prev_month_num:02d}"

    # Current ISO Mon-Sun window
    iso_weekday = today.isocalendar()[2]
    monday_this_week = today - timedelta(days=iso_weekday - 1)
    sunday_this_week = monday_this_week + timedelta(days=6)

    agg = {}
    for date_str, violations in history.items():
        try:
            d = datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            continue
        month_str = d.strftime("%Y-%m")
        if month_str not in (current_month, prev_month):
            continue
        wk = 1 if d.day <= 7 else 2 if d.day <= 14 else 3 if d.day <= 21 else 4
        for v in violations:
            key = (month_str, v.get("market"), v.get("asin"), v.get("seller_id"))
            rec = agg.get(key)
            if rec is None:
                rec = {
                    "month":         month_str,
                    "market":        v.get("market"),
                    "asin":          v.get("asin"),
                    "sku":           v.get("sku", ""),
                    "product_name":  v.get("product_name", ""),
                    "seller_id":     v.get("seller_id"),
                    "seller_name":   v.get("seller_name", ""),
                    "seller_url":    v.get("seller_url", seller_profile_url(v.get("seller_id", ""))),
                    "fairtex_msrp":  v.get("fairtex_msrp"),
                    "currency":      v.get("currency", "CAD"),
                    "_week_days":    {1: set(), 2: set(), 3: set(), 4: set()},
                    "_this_week":   set(),
                    "_prices":       [],
                    "last_seen":     None,
                    "last_price":    None,
                }
                agg[key] = rec
            rec["_week_days"][wk].add(date_str)
            if monday_this_week <= d <= sunday_this_week:
                rec["_this_week"].add(date_str)
            price = v.get("seller_price")
            if price is not None:
                rec["_prices"].append(float(price))
                if rec["last_seen"] is None or date_str > rec["last_seen"]:
                    rec["last_seen"] = date_str
                    rec["last_price"] = float(price)
            # keep latest metadata
            rec["sku"] = v.get("sku", rec["sku"])
            rec["product_name"] = v.get("product_name", rec["product_name"])
            # Prefer a resolved (non-ID) name if we have one now.
            new_name = v.get("seller_name") or rec["seller_name"]
            if new_name and new_name != rec["seller_id"]:
                rec["seller_name"] = new_name
            elif get_seller_name(rec["seller_id"]) != rec["seller_id"]:
                rec["seller_name"] = get_seller_name(rec["seller_id"])
            rec["fairtex_msrp"] = v.get("fairtex_msrp", rec["fairtex_msrp"])
            if not rec.get("seller_url"):
                rec["seller_url"] = seller_profile_url(rec["seller_id"])

    entries = []
    for rec in agg.values():
        w1 = len(rec["_week_days"][1])
        w2 = len(rec["_week_days"][2])
        w3 = len(rec["_week_days"][3])
        w4 = len(rec["_week_days"][4])
        avg = round(sum(rec["_prices"]) / len(rec["_prices"]), 2) if rec["_prices"] else None
        entries.append({
            "month":          rec["month"],
            "market":         rec["market"],
            "asin":           rec["asin"],
            "sku":            rec["sku"],
            "product_name":   rec["product_name"],
            "seller_id":      rec["seller_id"],
            "seller_name":    rec["seller_name"],
            "seller_url":     rec.get("seller_url", seller_profile_url(rec["seller_id"])),
            "fairtex_msrp":   rec["fairtex_msrp"],
            "currency":       rec["currency"],
            "last_price":     rec["last_price"],
            "avg_price":      avg,
            "last_seen":      rec["last_seen"],
            "week_1":         w1,
            "week_2":         w2,
            "week_3":         w3,
            "week_4":         w4,
            "month_total":    w1 + w2 + w3 + w4,
            "days_this_week": len(rec["_this_week"]),
        })
    return {
        "generated_at":   datetime.now(timezone.utc).isoformat(),
        "current_month":  current_month,
        "previous_month": prev_month,
        "entries":        entries,
    }


def _save_violations_summary(summary):
    os.makedirs(os.path.dirname(VIOLATIONS_SUMMARY_PATH), exist_ok=True)
    with open(VIOLATIONS_SUMMARY_PATH, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, default=str)


def compute_msrp_check(our_msrp_str, fairtex_cad):
    """Simple two-value check, with $0.01 tolerance.

      - "MSRP compliant"        when our MSRP >= Fairtex - $0.01
      - "MSRP Lower than Fairtex" otherwise
      - ""                       when either input is missing
    """
    our_msrp = _parse_money(our_msrp_str)
    if our_msrp is None or fairtex_cad is None:
        return ""
    if our_msrp >= fairtex_cad - 0.01:
        return "MSRP compliant"
    return "MSRP Lower than Fairtex"


def compute_msrp_diff_reason(our_msrp_str, fairtex_cad, has_buy_box,
                              winner_price_str, winner_seller, has_discount):
    """Reason our MSRP sits below Fairtex's. Kept in lockstep with Action
    Items: both blank when we're within $0.10 of Fairtex's MSRP, so the
    two columns never contradict each other.

      a. winner exists AND our price within $0.10 of winner →
         "MSRP lowered to match competitor: {winner_seller}"
      b. otherwise + active discount → "Discounting overstock"
      c. otherwise + no discount     → "No reason, Kindly Adjust MSRP"
    """
    our_msrp = _parse_money(our_msrp_str)
    if our_msrp is None or fairtex_cad is None:
        return ""
    # No-op zone: within $0.10 of Fairtex — nothing to attribute.
    if our_msrp >= fairtex_cad - 0.10:
        return ""

    winner = _parse_money(winner_price_str)
    if winner is not None and abs(our_msrp - winner) <= 0.10:
        seller = winner_seller or "competitor"
        return f"MSRP lowered to match competitor: {seller}"

    if has_discount:
        return "Discounting overstock"
    return "No reason, Kindly Adjust MSRP"


def compute_action_items(our_msrp_str, fairtex_cad, has_buy_box,
                          winner_price_str, has_discount):
    """Recommended action with no-op suppression.

    "Match price to X" is blanked whenever we're already within $0.10 of X
    — applied to both override and the MSRP < Fairtex branches. This keeps
    Action Items in lockstep with MSRP Difference Reason (the two columns
    never contradict each other) and avoids no-op recommendations for
    sub-cent gaps.

    Override: no buy box AND winner is more than $0.10 below us
              → "Match price to winner at $X.XX" (fires regardless of MSRP vs Fairtex)

    Then, only when MSRP < Fairtex - $0.10:
      1. own buy box, no discount             → "Match price to Fairtex"
      2. own buy box, active discount         → "Currently on discount - No action"
      3. no buy box, winner exists, not matched → "Match price to winner at $X.XX"
      4. no buy box, no winner, no discount   → "Match price to Fairtex"
      5. no buy box, no winner, has discount  → "Currently on discount - No action"
    Otherwise (essentially compliant or already matched) → ""
    """
    our_msrp = _parse_money(our_msrp_str)
    winner = _parse_money(winner_price_str)

    # Override: lost buy box and winner is materially below us (> $0.10).
    if (not has_buy_box and our_msrp is not None and winner is not None
            and (our_msrp - winner) > 0.10):
        return f"Match price to winner at ${winner:.2f}"

    if our_msrp is None or fairtex_cad is None:
        return ""
    # MSRP compliant per MSRP Check tolerance.
    if our_msrp >= fairtex_cad - 0.01:
        return ""
    # No-op zone: within $0.10 of Fairtex — suppress "Match price to Fairtex".
    if (fairtex_cad - our_msrp) <= 0.10:
        return ""

    if has_buy_box:
        return "Currently on discount - No action" if has_discount else "Match price to Fairtex"

    if winner is not None:
        # Already within $0.10 of winner — suppress (mirrors MSRP Diff Reason).
        if abs(our_msrp - winner) <= 0.10:
            return ""
        return f"Match price to winner at ${winner:.2f}"

    return "Currently on discount - No action" if has_discount else "Match price to Fairtex"


def compute_recommendation(winner_price_str, lowest_msrp, our_msrp_str=""):
    """Pricing recommendation for products where we don't own the buy box.

    Case 1 — a competing winner exists: match at the 15% margin floor if the
    winner sits above it; otherwise flag that we cannot profitably match.

    Case 2 — no winner offer at all: suggest dropping price 10% off our MSRP,
    but only if the new price stays at or above the 15% margin floor.
    """
    if lowest_msrp is None:
        return ""

    if winner_price_str:
        try:
            winner_price = float(winner_price_str.replace("$", "").replace(",", ""))
        except ValueError:
            return ""
        if winner_price > lowest_msrp:
            return f"Yes, reduce to ${lowest_msrp:.2f}"
        return "No, winner below minimum"

    if our_msrp_str:
        try:
            our_msrp = float(our_msrp_str.replace("$", "").replace(",", ""))
        except ValueError:
            return ""
        new_price = round(our_msrp * 0.9, 2)
        if new_price >= lowest_msrp:
            return f"No winner — drop price 10% to ${new_price:.2f}"
    return ""


def _request_report(headers, report_type, marketplace_id):
    """Request an SP-API report and wait for completion. Returns content string or None."""
    resp = requests.post(
        f"{SP_API_BASE}/reports/2021-06-30/reports",
        headers=headers,
        json={"reportType": report_type, "marketplaceIds": [marketplace_id]},
        timeout=30,
    )
    if resp.status_code == 429:
        time.sleep(30)
        resp = requests.post(
            f"{SP_API_BASE}/reports/2021-06-30/reports",
            headers=headers,
            json={"reportType": report_type, "marketplaceIds": [marketplace_id]},
            timeout=30,
        )
    if resp.status_code not in (200, 202):
        print(f"  Failed to create report: {resp.status_code}: {resp.text[:300]}")
        return None

    report_id = resp.json().get("reportId")
    if not report_id:
        return None
    print(f"  Report ID: {report_id}")

    # Poll until done
    doc_id = None
    for attempt in range(30):
        time.sleep(10)
        resp = requests.get(
            f"{SP_API_BASE}/reports/2021-06-30/reports/{report_id}",
            headers=headers, timeout=30,
        )
        if resp.status_code != 200:
            continue
        status = resp.json().get("processingStatus", "")
        print(f"  Report status: {status} (attempt {attempt + 1})")
        if status == "DONE":
            doc_id = resp.json().get("reportDocumentId")
            break
        if status in ("CANCELLED", "FATAL"):
            return None
    if not doc_id:
        return None

    # Get document URL and download
    resp = requests.get(
        f"{SP_API_BASE}/reports/2021-06-30/documents/{doc_id}",
        headers=headers, timeout=30,
    )
    if resp.status_code != 200:
        return None

    doc_info = resp.json()
    doc_url = doc_info.get("url")
    compression = doc_info.get("compressionAlgorithm")
    if not doc_url:
        return None

    resp = requests.get(doc_url, timeout=60)
    if resp.status_code != 200:
        return None

    raw = resp.content
    if compression == "GZIP":
        raw = gzip.decompress(raw)

    print(f"  Downloaded {len(raw)} bytes")
    return raw.decode("utf-8", errors="replace")


def get_fulfillment_types(access_token):
    """Classify Canada inventory as FBA or NARF using the planning report's
    age bucket data — the same 'FBA inventory age by days' visible on each
    product's Seller Central page.

    Uses GET_FBA_INVENTORY_PLANNING_DATA for Canada marketplace.
    Matches by ASIN (not SKU) because SKU formatting differs between the
    report and the inventory API.

    - Any age bucket > 0 → FBA (inventory in Canadian FCs)
    - All age buckets = 0 or not in report → NARF

    Returns a set of FBA ASINs, or None if report failed.
    """
    headers = sp_api_headers(access_token)

    print("  Fetching FBA Inventory Planning Data for Canada...")
    content = _request_report(headers, "GET_FBA_INVENTORY_PLANNING_DATA", MARKETPLACE_ID)
    if content is None:
        print("  WARNING: Report failed — cannot classify FBA vs NARF")
        return None

    reader = csv.DictReader(io.StringIO(content), delimiter="\t")
    fieldnames = reader.fieldnames or []

    age_columns = [
        "inv-age-0-to-90-days", "inv-age-91-to-180-days",
        "inv-age-181-to-270-days", "inv-age-271-to-365-days",
        "inv-age-365-plus-days",
    ]

    # Known FBA ASINs to verify report accuracy
    verify_asins = {"B00O1S1HUE", "B00O1S1OFW", "B00PM9XRZ4", "B07B2Z8P7S"}

    fba_asins = set()
    all_report_asins = set()
    total_rows = 0

    for row in reader:
        asin = row.get("asin", "").strip()
        sku = row.get("sku", row.get("seller-sku", "")).strip()
        if not asin:
            continue
        total_rows += 1
        all_report_asins.add(asin)

        age_vals = {col: row.get(col, "0").strip() for col in age_columns}
        has_aged = any(int(v or "0") > 0 for v in age_vals.values())

        if asin in verify_asins:
            print(f"  VERIFY {asin} (SKU={sku}): age={age_vals}, FBA={has_aged}")

        if has_aged:
            fba_asins.add(asin)

    # Check if known FBA ASINs are even in the report
    for va in verify_asins:
        if va not in all_report_asins:
            print(f"  MISSING: {va} is NOT in the planning report at all")

    print(f"  Report had {total_rows} rows, {len(all_report_asins)} unique ASINs")
    print(f"  {len(fba_asins)} ASINs with age > 0 (FBA)")
    return fba_asins


def get_fba_inventory(access_token):
    headers    = sp_api_headers(access_token)
    url        = f"{SP_API_BASE}/fba/inventory/v1/summaries"
    all_items  = []
    seen_skus  = set()
    next_token = None
    page       = 0
    start_date = (datetime.now(timezone.utc) - timedelta(days=540)).strftime("%Y-%m-%dT%H:%M:%SZ")

    while True:
        page  += 1
        params = {
            "details":         "true",
            "granularityType": "Marketplace",
            "granularityId":   MARKETPLACE_ID,
            "marketplaceIds":  MARKETPLACE_ID,
            "startDateTime":   start_date,
        }
        if next_token:
            params["nextToken"] = next_token

        resp = requests.get(url, headers=headers, params=params, timeout=30)
        if resp.status_code == 429:
            print(f"  Page {page} rate limited, waiting 5s...")
            time.sleep(5)
            resp = requests.get(url, headers=headers, params=params, timeout=30)
        if resp.status_code != 200:
            # Next token may have expired — restart pagination without token
            if next_token and "invalid" in resp.text.lower():
                print(f"  Page {page} next token expired, restarting pagination...")
                next_token = None
                # Use a later startDateTime to skip already-fetched items
                params.pop("nextToken", None)
                resp = requests.get(url, headers=headers, params=params, timeout=30)
            else:
                print(f"  Page {page} error {resp.status_code}, retrying in 5s...")
                time.sleep(5)
                resp = requests.get(url, headers=headers, params=params, timeout=30)
        if resp.status_code != 200:
            print(f"  Page {page} failed: {resp.status_code}: {resp.text[:200]}")
            break

        data  = resp.json()
        items = data.get("payload", {}).get("inventorySummaries", [])

        for item in items:
            sku = item.get("sellerSku", "")
            if sku and sku not in seen_skus:
                seen_skus.add(sku)
                all_items.append(item)

        print(f"  Page {page}: {len(items)} records (unique so far: {len(all_items)})")

        next_token = (data.get("pagination") or {}).get("nextToken")
        if not next_token:
            next_token = (data.get("payload") or {}).get("nextToken")
        if not next_token:
            break
        time.sleep(1)

    print(f"  Total unique records: {len(all_items)}")

    result = []
    for item in all_items:
        sku   = item.get("sellerSku", "")
        asin  = item.get("asin", "")
        fnsku = item.get("fnsku", "")
        name  = item.get("productName", asin)[:70]
        details = item.get("inventoryDetails") or {}
        qty     = details.get("fulfillableQuantity") or 0
        inbound = (
            (details.get("inboundWorkingQuantity")   or 0)
            + (details.get("inboundShippedQuantity")  or 0)
            + (details.get("inboundReceivingQuantity") or 0)
        )

        sku_lower = sku.lower()
        if (sku_lower.startswith("amzn.gr") or
            sku_lower.startswith("dnu") or
            sku_lower.endswith("_ln") or
            fnsku.startswith("X")):
            continue
        # Include inbound: a SKU with active replenishment should still be monitored
        # so we catch buy-box losses while stock is in transit.
        if (qty > 0 or inbound > 0) and asin and sku:
            result.append({
                "sku": sku, "asin": asin, "name": name,
                "stock": qty, "inbound": inbound,
            })

    print(f"  SKUs sellable or inbound > 0: {len(result)}")
    return result


def check_buy_box(access_token, items):
    headers    = sp_api_headers(access_token)
    result     = {}
    asin_msrp  = {}
    total      = len(items)
    batch_size = 20

    print(f"  Checking {total} SKUs via batch listing offers...")

    for i in range(0, total, batch_size):
        batch = items[i:i + batch_size]

        batch_requests = []
        for item in batch:
            batch_requests.append({
                "method": "GET",
                "uri": f"/products/pricing/v0/listings/{requests.utils.quote(item['sku'], safe='')}/offers",
                "MarketplaceId": MARKETPLACE_ID,
                "ItemCondition": "New",
                "CustomerType":  "Consumer",
            })

        resp = requests.post(
            f"{SP_API_BASE}/batches/products/pricing/v0/listingOffers",
            headers=headers,
            json={"requests": batch_requests},
            timeout=60,
        )

        if resp.status_code == 429:
            print(f"  Rate limited at batch {i // batch_size + 1}, waiting 30s...")
            time.sleep(30)
            resp = requests.post(
                f"{SP_API_BASE}/batches/products/pricing/v0/listingOffers",
                headers=headers,
                json={"requests": batch_requests},
                timeout=60,
            )

        if resp.status_code != 200:
            print(f"  Batch error {resp.status_code}: {resp.text[:200]}")
            for item in batch:
                result[item["sku"]] = {"has_buy_box": True}
            time.sleep(3)
            continue

        responses = resp.json().get("responses", [])

        for j, item in enumerate(batch):
            sku = item["sku"]
            if j >= len(responses):
                result[sku] = {"has_buy_box": True}
                continue

            res   = responses[j]
            body  = res.get("body", {})
            error = body.get("errors", [None])[0] if body.get("errors") else None

            if error:
                code = error.get("code", "")
                msg  = error.get("message", "")
                if code == "InvalidInput" and "is an invalid SKU for marketplace" in msg:
                    result[sku] = {"has_buy_box": True}
                else:
                    result[sku] = {"has_buy_box": True}
                continue

            offers = body.get("payload", {}).get("Offers", [])
            our_offer = next((o for o in offers if o.get("SellerId") == MY_SELLER_ID), None)

            # Extract our listing price and landed price
            price_source = our_offer
            winner = next((o for o in offers if o.get("IsBuyBoxWinner")), None)
            if not price_source and winner and winner.get("SellerId") == MY_SELLER_ID:
                price_source = winner
            our_msrp = ""
            our_landed = ""
            if price_source:
                lp = price_source.get("ListingPrice", {}).get("Amount")
                sp = price_source.get("Shipping", {}).get("Amount")
                if lp is not None:
                    our_msrp = f"${float(lp):.2f}"
                    asin_msrp[item["asin"]] = our_msrp
                    # Landed price = listing price + shipping/import fees
                    landed = float(lp) + (float(sp) if sp else 0)
                    our_landed = f"${landed:.2f}"

            # Every non-us offer: keep both listing and landed for reporting.
            # Winner selection stays landed-based (Amazon ranks that way) but
            # everything user-facing (winner_price, Action Items, Price Gap,
            # MSRP Difference Reason) uses LISTING price only.
            competitor_offers = []
            for o in offers:
                sid = o.get("SellerId")
                if not sid or sid == MY_SELLER_ID:
                    continue
                lp_raw = o.get("ListingPrice", {}).get("Amount")
                if lp_raw is None:
                    continue
                try:
                    lp_val = float(lp_raw)
                except (TypeError, ValueError):
                    continue
                sp_raw = o.get("Shipping", {}).get("Amount")
                try:
                    sh_val = float(sp_raw) if sp_raw is not None else 0.0
                except (TypeError, ValueError):
                    sh_val = 0.0
                competitor_offers.append({
                    "seller_id":     sid,
                    "listing_price": round(lp_val, 2),
                    "shipping":      round(sh_val, 2),
                    "landed_price":  round(lp_val + sh_val, 2),
                    "currency":      o.get("ListingPrice", {}).get("CurrencyCode", "CAD"),
                })

            we_have_it = (
                (our_offer and our_offer.get("IsBuyBoxWinner") is True) or
                (winner and winner.get("SellerId") == MY_SELLER_ID)
            )

            if we_have_it:
                result[sku] = {
                    "has_buy_box":       True,
                    "our_msrp":          our_msrp,
                    "our_landed":        our_landed,
                    "competitor_offers": competitor_offers,
                }
            else:
                winner_id     = winner.get("SellerId", "Unknown") if winner else None
                winner_seller = get_seller_name(winner_id) if winner_id else "No winner"
                winner_url    = f"https://www.amazon.ca/sp?seller={winner_id}" if winner_id else ""
                winner_price  = ""
                if winner:
                    lp = winner.get("ListingPrice", {}).get("Amount")
                    # Listing price only (per spec) — no shipping added.
                    if lp is not None:
                        winner_price = f"${float(lp):.2f}"
                result[sku] = {
                    "has_buy_box":       False,
                    "our_msrp":          our_msrp,
                    "our_landed":        our_landed,
                    "winner_seller_id":  winner_id,
                    "winner_seller":     winner_seller,
                    "winner_url":        winner_url,
                    "winner_price":      winner_price,
                    "competitor_offers": competitor_offers,
                }

        if (i + batch_size) % 100 < batch_size:
            print(f"  Progress: {min(i + batch_size, total)}/{total}")

        time.sleep(1)

    # Backfill missing MSRPs using ASIN mapping
    backfilled = 0
    for item in items:
        sku = item["sku"]
        if sku in result and not result[sku].get("our_msrp") and item["asin"] in asin_msrp:
            result[sku]["our_msrp"] = asin_msrp[item["asin"]]
            backfilled += 1
    if backfilled:
        print(f"  Backfilled MSRP for {backfilled} SKUs via ASIN mapping")

    # Fetch remaining missing MSRPs by ASIN via getPricing API
    missing_asins = set()
    for item in items:
        if item["sku"] in result and not result[item["sku"]].get("our_msrp"):
            missing_asins.add(item["asin"])
    missing_asins = list(missing_asins)

    if missing_asins:
        print(f"  Fetching {len(missing_asins)} remaining MSRPs by ASIN...")
        fetched = 0
        for asin in missing_asins:
            url = f"{SP_API_BASE}/products/pricing/v0/price?MarketplaceId={MARKETPLACE_ID}&ItemType=Asin&Asins={asin}"
            try:
                resp = requests.get(url, headers=headers, timeout=30)
                if resp.status_code == 429:
                    time.sleep(30)
                    resp = requests.get(url, headers=headers, timeout=30)
                if resp.status_code == 200:
                    for pd in resp.json().get("payload", []):
                        if pd.get("status") != "Success":
                            continue
                        ol = pd.get("Product", {}).get("Offers", [])
                        if ol:
                            lp = ol[0].get("BuyingPrice", {}).get("ListingPrice", {}).get("Amount")
                            if lp is not None:
                                asin_msrp[asin] = f"${float(lp):.2f}"
                                fetched += 1
            except Exception:
                pass
            time.sleep(0.3)
        print(f"  Fetched {fetched}/{len(missing_asins)} via getPricing")

        # Apply to all SKUs with those ASINs
        for item in items:
            sku = item["sku"]
            if sku in result and not result[sku].get("our_msrp") and item["asin"] in asin_msrp:
                result[sku]["our_msrp"] = asin_msrp[item["asin"]]

    owned     = sum(1 for v in result.values() if v.get("has_buy_box"))
    not_owned = len(result) - owned
    print(f"  Buy box owned: {owned} | NOT owned: {not_owned}")
    return result


def post_slack(headers, text):
    resp = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers=headers,
        json={"channel": SLACK_CHANNEL, "text": text},
    )
    if not resp.json().get("ok"):
        raise RuntimeError(f"Slack error: {resp.json().get('error')}")
    time.sleep(0.3)


def send_slack_alert(flagged, total_checked, non_compliant_count=0, dashboard_url=""):
    headers = {
        "Authorization": f"Bearer {os.environ['SLACK_BOT_TOKEN']}",
        "Content-Type":  "application/json",
    }
    now_cst = datetime.now(ZoneInfo("America/Chicago")).strftime("%b %d, %Y %I:%M %p CST")
    n_missing = len(flagged)

    # All good: no missing buy boxes AND no MSRP compliance issues.
    if n_missing == 0 and non_compliant_count == 0:
        post_slack(headers,
            f":white_check_mark: *Amazon FBA Buy Box Check for CA - {now_cst}*\n"
            f"Checked *{total_checked} SKUs*. All currently have the featured offer.\n"
            f"<@U04DSUU9KGT> Nothing to action. {dashboard_url}"
        )
        return

    post_slack(headers,
        f":warning: *Amazon FBA Buy Box Check for CA - {now_cst}*\n"
        f"Checked *{total_checked} SKUs*. *{n_missing}* missing buy box. "
        f"*{non_compliant_count}* not MSRP compliant.\n"
        f"<@U04DSUU9KGT> please review: {dashboard_url}"
    )




_listings_report_cache = None


def _fetch_listings_report(headers):
    """Fetch and parse GET_MERCHANT_LISTINGS_ALL_DATA for CA. Cached per run."""
    global _listings_report_cache
    if _listings_report_cache is not None:
        return _listings_report_cache

    content = _request_report(headers, "GET_MERCHANT_LISTINGS_ALL_DATA", MARKETPLACE_ID)
    if content is None:
        print("  WARNING: Listings report failed")
        _listings_report_cache = []
        return _listings_report_cache

    reader = csv.DictReader(io.StringIO(content), delimiter="\t")
    print(f"  Listings report columns: {reader.fieldnames}")

    rows = []
    for row in reader:
        sku = (row.get("seller-sku") or "").strip()
        asin = (row.get("asin1") or "").strip()
        if not sku or not asin:
            continue
        price_str = (row.get("price") or "").strip()
        try:
            price = round(float(price_str), 2) if price_str else None
        except ValueError:
            price = None
        rows.append({
            "sku": sku,
            "asin": asin,
            "name": (row.get("item-name") or "").strip(),
            "price": price,
        })

    print(f"  Parsed {len(rows)} active rows from listings report")
    _listings_report_cache = rows
    return rows


def get_ca_listings(access_token):
    """Every active CA listing, keyed by SKU: dict SKU -> {asin, name}.

    Used to expand the monitor universe beyond what /fba/inventory returns —
    catches NARF-enrolled SKUs with zero fulfillable AND zero inbound today,
    so we still see buy-box loss while we're completely out of stock.
    """
    rows = _fetch_listings_report(sp_api_headers(access_token))
    listings = {}
    for r in rows:
        sku = r["sku"]
        if sku not in listings:
            listings[sku] = {"asin": r["asin"], "name": r["name"]}
    return listings


def _get_cad_prices_from_report(headers):
    """Get CAD listing prices from the listings report, keyed by ASIN.

    Returns dict: asin -> {sku, price} (first non-empty price per ASIN wins).
    """
    report_data = {}
    for r in _fetch_listings_report(headers):
        asin = r["asin"]
        if r["price"] is None or asin in report_data:
            continue
        report_data[asin] = {"sku": r["sku"], "price": r["price"]}

    print(f"  Got data for {len(report_data)} ASINs from report")
    for i, (asin, d) in enumerate(list(report_data.items())[:3]):
        print(f"  Sample: {asin} SKU={d['sku']} CAD_price=${d['price']}")
    return report_data


def _get_fees_per_sku(headers, items, buy_box_map, report_data, fba_asins=None):
    """For each ASIN:
    1. Look up the report's SKU and CAD price by ASIN (consistent matching)
    2. Send report SKU + CAD price to SKU fee endpoint → exact fees
    3. Fall back to ASIN endpoint if SKU endpoint fails
    """
    fee_map = {}

    sku_list = []
    seen_asins = set()
    for item in items:
        asin = item["asin"]
        if asin in seen_asins:
            continue
        seen_asins.add(asin)

        # Use report's SKU and price (matched by ASIN)
        rd = report_data.get(asin)
        if rd:
            report_sku = rd["sku"]
            cad_price = rd["price"]
        else:
            # Fallback: use inventory SKU and SP-API price
            report_sku = item["sku"]
            info = buy_box_map.get(item["sku"], {})
            msrp_str = info.get("our_msrp", "")
            try:
                cad_price = float(msrp_str.replace("$", "").replace(",", "")) if msrp_str else 50.0
            except (ValueError, AttributeError):
                cad_price = 50.0

        is_narf = fba_asins is not None and asin not in fba_asins
        sku_list.append((report_sku, asin, cad_price, is_narf))

    total = len(sku_list)
    print(f"  Fetching total fees for {total} SKUs via listings endpoint...")

    from urllib.parse import quote
    for idx, (sku, asin, cad_price, is_narf) in enumerate(sku_list):
        if cad_price <= 0:
            continue

        body = {
            "FeesEstimateRequest": {
                "MarketplaceId": MARKETPLACE_ID,
                "IsAmazonFulfilled": True,
                "PriceToEstimateFees": {
                    "ListingPrice": {"CurrencyCode": "CAD", "Amount": cad_price},
                    "Shipping":     {"CurrencyCode": "CAD", "Amount": 0},
                },
                "Identifier": sku,
            }
        }
        sku_encoded = quote(sku, safe="")
        url = f"{SP_API_BASE}/products/fees/v0/listings/{sku_encoded}/feesEstimate"

        try:
            resp = requests.post(url, headers=headers, json=body, timeout=30)
            # Retry up to 2 times on rate limit — don't fall back to inaccurate ASIN
            for retry in range(2):
                if resp.status_code != 429:
                    break
                wait = 10 * (retry + 1)
                print(f"  Rate limited on {asin}, waiting {wait}s (retry {retry+1})...")
                time.sleep(wait)
                resp = requests.post(url, headers=headers, json=body, timeout=30)

            success = False
            if resp.status_code == 200:
                payload = resp.json().get("payload", {})
                res = payload.get("FeesEstimateResult", {})
                if res.get("Status") == "Success":
                    total_fees_obj = res.get("FeesEstimate", {}).get("TotalFeesEstimate", {})
                    total_fee = float(total_fees_obj.get("Amount", 0) or 0)
                    currency = total_fees_obj.get("CurrencyCode", "?")
                    fee_map[asin] = {"total_fee": round(total_fee, 2), "cad_price": cad_price}
                    ft_label = "NARF" if is_narf else "FBA"
                    if idx < 5:
                        print(f"  Sample: {sku} ({ft_label}) price=${cad_price} fee={currency}${total_fee}")
                    success = True

            # Fallback to ASIN endpoint if SKU endpoint failed
            if not success and asin not in fee_map:
                try:
                    resp2 = requests.post(
                        f"{SP_API_BASE}/products/fees/v0/items/{asin}/feesEstimate",
                        headers=headers, json=body, timeout=30)
                    if resp2.status_code == 200:
                        res2 = resp2.json().get("payload", {}).get("FeesEstimateResult", {})
                        if res2.get("Status") == "Success":
                            tf2 = res2.get("FeesEstimate", {}).get("TotalFeesEstimate", {})
                            total_fee = float(tf2.get("Amount", 0) or 0)
                            fee_map[asin] = {"total_fee": round(total_fee, 2), "cad_price": cad_price}
                            if idx < 5:
                                print(f"  Fallback OK: {asin} fee=${total_fee}")
                except Exception:
                    pass
        except Exception as e:
            if idx < 3:
                print(f"  Exception for {sku}: {e}")

        if (idx + 1) % 100 == 0:
            print(f"  Fees progress: {idx + 1}/{total}")
        time.sleep(1)

    print(f"  Fees retrieved for {len(fee_map)} ASINs out of {total} SKUs")
    return fee_map


def fetch_discount_flags(access_token, items):
    """For each SKU, check Listings Items API for an active discount.

    A SKU is flagged has_discount=True when, inside attributes.purchasable_offer,
    either discounted_price or sale_price has a schedule entry with:
      - at least one of start_at / end_at set (real time window), AND
      - value_with_tax at least $0.01 below our_price.

    Returns dict: sku -> bool.
    """
    from urllib.parse import quote
    headers = sp_api_headers(access_token)
    result = {}
    print(f"  Checking discount status for {len(items)} SKUs via Listings Items API...")

    for idx, item in enumerate(items):
        sku = item["sku"]
        sku_encoded = quote(sku, safe="")
        url = (f"{SP_API_BASE}/listings/2021-08-01/items/{MY_SELLER_ID}/{sku_encoded}"
               f"?marketplaceIds={MARKETPLACE_ID}&includedData=attributes")
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            for retry in range(2):
                if resp.status_code != 429:
                    break
                time.sleep(10 * (retry + 1))
                resp = requests.get(url, headers=headers, timeout=30)
            if resp.status_code != 200:
                result[sku] = False
                continue

            attrs = resp.json().get("attributes", {}) or {}
            offers = attrs.get("purchasable_offer", []) or []

            # Find our_price value as the reference
            our_price = None
            for offer in offers:
                for pricing in (offer.get("our_price") or []):
                    for sched in (pricing.get("schedule") or []):
                        v = sched.get("value_with_tax")
                        if v is not None:
                            our_price = float(v)
                            break
                    if our_price is not None:
                        break
                if our_price is not None:
                    break

            # Look for an active discount in either field
            has_discount = False
            if our_price is not None:
                for offer in offers:
                    for field in ("discounted_price", "sale_price"):
                        for entry in (offer.get(field) or []):
                            for sched in (entry.get("schedule") or []):
                                if not (sched.get("start_at") or sched.get("end_at")):
                                    continue
                                v = sched.get("value_with_tax")
                                if v is None:
                                    continue
                                if float(v) <= our_price - 0.01:
                                    has_discount = True
                                    break
                            if has_discount:
                                break
                        if has_discount:
                            break
                    if has_discount:
                        break
            result[sku] = has_discount
        except Exception:
            result[sku] = False

        if (idx + 1) % 100 == 0:
            print(f"  Discount progress: {idx + 1}/{len(items)}")
        time.sleep(0.5)  # stay under Listings Items API rate limits

    flagged = sum(1 for v in result.values() if v)
    print(f"  {flagged}/{len(result)} SKUs have an active discount")
    return result


def get_fee_estimates(access_token, items=None, buy_box_map=None, fba_asins=None):
    """Fetch per-ASIN total fees in CAD.

    1. Fetches CAD prices from GET_MERCHANT_LISTINGS_ALL_DATA report
    2. For each SKU, sends CAD price to SKU fee endpoint
    3. Gets back exact TotalFeesEstimate in CAD (matches Seller Central)

    Returns fee_map: asin -> {total_fee, cad_price}
    """
    headers = sp_api_headers(access_token)

    # Get CAD prices + report SKUs from merchant listings report (one call, all ASINs)
    report_data = {}
    if items:
        print("  Fetching CAD prices + SKUs from merchant listings report...")
        report_data = _get_cad_prices_from_report(headers)

    if items and buy_box_map:
        fee_map = _get_fees_per_sku(headers, items, buy_box_map, report_data, fba_asins)
    else:
        print("  WARNING: No items/buy_box_map — fees will be 0")
        fee_map = {}

    return fee_map


def main():
    print("=== Amazon CA Buy Box Monitor ===")
    print(f"Time (UTC): {datetime.now(timezone.utc).isoformat()}")

    print("\n[1/8] Fetching LWA access token...")
    access_token = get_lwa_access_token()
    print("  OK")

    print("\n[2/8] Fetching FBA inventory...")
    inventory = get_fba_inventory(access_token)
    print(f"  {len(inventory)} SKUs with stock or inbound > 0.")

    # Expand the universe to include every active CA listing so we catch
    # NARF-enrolled ASINs that have zero fulfillable AND zero inbound today.
    # These enter the loop with stock=0, inbound=0 so we still see buy-box
    # loss while we are completely out of stock.
    print("  Expanding via merchant listings report (NARF coverage)...")
    inv_skus = {item["sku"] for item in inventory}
    added = 0
    for sku, lst in get_ca_listings(access_token).items():
        if sku in inv_skus:
            continue
        sku_lower = sku.lower()
        if (sku_lower.startswith("amzn.gr") or
            sku_lower.startswith("dnu") or
            sku_lower.endswith("_ln")):
            continue
        if not lst.get("asin"):
            continue
        inventory.append({
            "sku": sku,
            "asin": lst["asin"],
            "name": (lst.get("name") or lst["asin"])[:70],
            "stock": 0,
            "inbound": 0,
        })
        added += 1
    print(f"  Added {added} listings-only SKUs (zero inventory); total now {len(inventory)}")

    if not inventory:
        send_slack_alert([], 0)
        return

    print("\n[3/8] Checking buy box and prices per SKU...")
    buy_box_map = check_buy_box(access_token, inventory)

    # Resolve competitor seller IDs → store names once we have them all.
    all_seller_ids = set()
    for info in buy_box_map.values():
        for offer in info.get("competitor_offers", []) or []:
            sid = offer.get("seller_id")
            if sid:
                all_seller_ids.add(sid)
    enrich_seller_names(all_seller_ids)
    # Refresh winner_seller labels now that the cache is populated.
    for info in buy_box_map.values():
        winner_id = info.get("winner_seller_id")
        if winner_id:
            info["winner_seller"] = get_seller_name(winner_id)

    print("\n[4/8] Classifying FBA vs NARF...")
    fba_asins = get_fulfillment_types(access_token)
    # fba_asins = set of ASINs with Canadian FC inventory (FBA)
    # ASINs NOT in this set = NARF
    if fba_asins is not None:
        fba = sum(1 for item in inventory if item["asin"] in fba_asins)
        narf = len(inventory) - fba
        print(f"  Canada inventory: {fba} FBA, {narf} NARF (out of {len(inventory)})")

    print("\n[5/8] Loading product cost data...")
    product_costs = load_product_costs()
    fairtex_msrp_map = load_fairtex_msrp()

    print("\n[6/8] Fetching fee estimates (referral + fulfillment) per ASIN...")
    fee_estimates = get_fee_estimates(access_token, inventory, buy_box_map, fba_asins)

    print("\n[6b/8] Checking active discount status per SKU...")
    discount_flags = fetch_discount_flags(access_token, inventory)

    def _build_total_cost(asin, ft, cost_data):
        """Total Cost = Product Cost (CAD) + Amazon Total Fees (CAD).
        Product cost comes from CSV (FBA or NARF column based on fulfillment type).
        Total fee comes from Amazon's per-ASIN fee estimate (all fee components).
        """
        if not cost_data:
            return None
        product_cost = cost_data["fba_cost"] if ft == "FBA" else cost_data["narf_cost"]
        if product_cost is None:
            return None
        fees = fee_estimates.get(asin, {})
        total_fee = fees.get("total_fee", 0) or 0
        return round(product_cost + total_fee, 2)

    def _get_cad_msrp(asin, fallback_info):
        """Get the CAD MSRP. For FBA products, the fee estimates store
        the converted CAD price. For NARF, SP-API price is already CAD."""
        fee_data = fee_estimates.get(asin, {})
        cad_price = fee_data.get("cad_price")
        if cad_price:
            return f"${cad_price:.2f}"
        return fallback_info.get("our_msrp", "")


    print("\n[7/8] Flagging and alerting...")
    flagged = []
    for item in inventory:
        info = buy_box_map.get(item["sku"], {})
        ft = ("FBA" if item["asin"] in fba_asins else "NARF") if fba_asins is not None else "Unknown"
        if not info.get("has_buy_box"):
            item["winner_seller"] = info.get("winner_seller", "Unknown")
            item["winner_url"]    = info.get("winner_url", "")
            item["winner_price"]  = info.get("winner_price", "")
            total_cost = _build_total_cost(item["asin"], ft, product_costs.get(item["asin"]))
            lowest_msrp = round(total_cost / 0.85, 2) if total_cost is not None else None
            item["total_cost"]     = total_cost
            item["lowest_msrp"]    = lowest_msrp
            cad_msrp = _get_cad_msrp(item["asin"], info)
            item["action_items"] = compute_action_items(
                cad_msrp,
                fairtex_msrp_map.get(item["asin"]),
                False,
                item["winner_price"],
                discount_flags.get(item["sku"], False),
            )
            item["recommendation"] = item["action_items"]
            flagged.append(item)
    print(f"  Flagged: {len(flagged)}")
    for p in flagged:
        rec_str = f" | Rec: {p['recommendation']}" if p.get("recommendation") else ""
        print(f"    - {p['sku']} | {p['asin']} | stock: {p['stock']} | winner: {p['winner_seller']} @ {p['winner_price']}{rec_str}")

    non_compliant_count = 0
    for item in inventory:
        info = buy_box_map.get(item["sku"], {})
        if compute_msrp_check(
            _get_cad_msrp(item["asin"], info),
            fairtex_msrp_map.get(item["asin"]),
        ) == "MSRP Lower than Fairtex":
            non_compliant_count += 1
    send_slack_alert(
        flagged, len(inventory), non_compliant_count,
        "https://fairtex-buybox-monitor-ca.vercel.app/",
    )

    # Save results to JSON for the Vercel dashboard
    print("\n[8/8] Saving dashboard data...")
    all_products = []
    for item in inventory:
        info = buy_box_map.get(item["sku"], {})
        cost_data = product_costs.get(item["asin"])
        ft = ("FBA" if item["asin"] in fba_asins else "NARF") if fba_asins is not None else "Unknown"
        # Use CAD MSRP from fee report (authoritative) instead of USD from SP-API
        our_msrp = _get_cad_msrp(item["asin"], info)
        our_landed = our_msrp  # default
        # For FBA: landed price = MSRP (no extra fees)
        # For NARF: landed price = MSRP + 14% import fees
        if ft == "NARF" and our_msrp:
            try:
                msrp_val = float(our_msrp.replace("$", "").replace(",", ""))
                our_landed = f"${msrp_val * (1 + NARF_IMPORT_FEE_RATE):.2f}"
            except ValueError:
                pass
        # Total cost = product cost + referral fee + fulfillment fee (FBA or NARF)
        total_cost  = _build_total_cost(item["asin"], ft, cost_data)
        lowest_msrp = round(total_cost / 0.85, 2) if total_cost is not None else None
        fairtex_cad = fairtex_msrp_map.get(item["asin"])
        has_buy_box = info.get("has_buy_box", True)
        winner_price_str = info.get("winner_price", "") if not has_buy_box else ""
        winner_seller = info.get("winner_seller", "") if not has_buy_box else ""
        has_discount = discount_flags.get(item["sku"], False)
        msrp_check = compute_msrp_check(our_msrp, fairtex_cad)
        msrp_diff_reason = compute_msrp_diff_reason(
            our_msrp, fairtex_cad, has_buy_box, winner_price_str, winner_seller, has_discount,
        )
        action_items = compute_action_items(
            our_msrp, fairtex_cad, has_buy_box, winner_price_str, has_discount,
        )
        product = {
            "sku":                     item["sku"],
            "asin":                    item["asin"],
            "name":                    item["name"],
            "stock":                   item["stock"],
            "our_msrp":                our_msrp,
            "our_landed":              our_landed,
            "has_buy_box":             has_buy_box,
            "total_cost":              total_cost,
            "lowest_msrp":             lowest_msrp,
            "fulfillment_type":        ft,
            "pricing_as_per_fairtex":  fairtex_cad,
            "msrp_check":              msrp_check,
            "msrp_diff_reason":        msrp_diff_reason,
            "has_discount":            has_discount,
            "inbound":                 item.get("inbound", 0),
        }
        if not has_buy_box:
            product["winner_seller"]  = info.get("winner_seller", "")
            product["winner_url"]     = info.get("winner_url", "")
            product["winner_price"]   = info.get("winner_price", "")
        # action_items is the new field; recommendation kept as alias for
        # any consumers still reading the old name.
        product["action_items"]   = action_items
        product["recommendation"] = action_items
        all_products.append(product)

    dashboard_data = {
        "last_updated":  datetime.now(timezone.utc).isoformat(),
        "total_checked": len(inventory),
        "buy_box_owned": len(inventory) - len(flagged),
        "total_flagged": len(flagged),
        "products":      all_products,
    }

    data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "dashboard", "data")
    os.makedirs(data_dir, exist_ok=True)
    with open(os.path.join(data_dir, "status.json"), "w") as f:
        json.dump(dashboard_data, f, indent=2)
    print(f"  Saved to dashboard/data/status.json")

    # ------------------------------------------------------------------
    # MSRP violations: record today, prune, rebuild rollup summary
    # ------------------------------------------------------------------
    print("\n[9/9] MSRP violations tracking...")
    today_iso, todays_violations = _record_todays_violations(inventory, buy_box_map, fairtex_msrp_map)
    history = _load_violations_history()
    _prune_violations_history(history, VIOLATIONS_RETENTION_DAYS)
    history[today_iso] = todays_violations
    _save_violations_history(history)
    summary = _build_violations_summary(history)
    _save_violations_summary(summary)
    print(f"  Today: {len(todays_violations)} violations across "
          f"{len({(v['asin'], v['seller_id']) for v in todays_violations})} unique (asin, seller) pairs")
    print(f"  Summary rollup rows (current + previous month): {len(summary['entries'])}")

    print("\n=== Done ===")


if __name__ == "__main__":
    main()
