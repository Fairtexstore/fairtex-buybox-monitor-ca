"""Monthly MSRP violations digest to Slack.

Loads dashboard/data/violations_summary.json, filters to the previous
calendar month, keeps only "repeat offender" sellers (max(w1..w4) >= 2),
and posts a per-market top-5 digest to the configured Slack channel.

Env:
  SLACK_BOT_TOKEN     required (chat:write scope)
  SLACK_CEO_USER_ID   optional; when set, the message tags this user
"""
import calendar
import json
import os
import urllib.request
from datetime import datetime, timezone

SLACK_CHANNEL = "C0AMDJ91151"
DASHBOARD_URL = "https://fairtex-buybox-monitor-ca.vercel.app/#violations"

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SUMMARY_PATH = os.path.join(_REPO_ROOT, "dashboard", "data", "violations_summary.json")


def _post_slack(text):
    body = json.dumps({"channel": SLACK_CHANNEL, "text": text}).encode("utf-8")
    req = urllib.request.Request(
        "https://slack.com/api/chat.postMessage",
        data=body,
        headers={
            "Authorization": f"Bearer {os.environ['SLACK_BOT_TOKEN']}",
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        result = json.loads(resp.read().decode("utf-8"))
    if not result.get("ok"):
        raise RuntimeError(f"Slack error: {result.get('error')}")


def _previous_month():
    today = datetime.now(timezone.utc).date()
    if today.month == 1:
        y, m = today.year - 1, 12
    else:
        y, m = today.year, today.month - 1
    return f"{y:04d}-{m:02d}", f"{calendar.month_name[m]} {y}"


def main():
    prev_month_key, prev_month_name = _previous_month()

    try:
        with open(SUMMARY_PATH, "r", encoding="utf-8") as f:
            summary = json.load(f)
    except FileNotFoundError:
        _post_slack(
            f"*MSRP Violations - {prev_month_name}*\n"
            f"No violations summary file found. Dashboard: {DASHBOARD_URL}"
        )
        return

    entries = [e for e in summary.get("entries", []) if e.get("month") == prev_month_key]
    ceo = (os.environ.get("SLACK_CEO_USER_ID") or "").strip()
    tag = f"<@{ceo}> " if ceo else ""

    if not entries:
        _post_slack(
            f"*MSRP Violations - {prev_month_name}*\n"
            f"No competitor MSRP violations recorded in the previous month.\n"
            f"{tag}{DASHBOARD_URL}"
        )
        return

    def _max_wk(e):
        return max(e.get("week_1", 0), e.get("week_2", 0),
                   e.get("week_3", 0), e.get("week_4", 0))

    repeat = [e for e in entries if _max_wk(e) >= 2]

    total_sellers = len({(e["market"], e["seller_id"]) for e in entries})
    repeat_sellers = len({(e["market"], e["seller_id"]) for e in repeat})

    lines = [f"*MSRP Violations - {prev_month_name}*"]
    lines.append(
        f"{total_sellers} unique competitor sellers ranged below Fairtex MSRP · "
        f"{repeat_sellers} qualify as repeat offenders (≥2 days in any week)."
    )

    if not repeat:
        lines.append("\nNo repeat offenders this month.")
    else:
        by_market = {}
        for e in repeat:
            by_market.setdefault(e["market"], []).append(e)
        for market, rows in sorted(by_market.items()):
            rows.sort(key=lambda x: x.get("month_total", 0), reverse=True)
            top = rows[:5]
            lines.append(f"\n*{market}* — top {len(top)} repeat offenders:")
            for r in top:
                label = r.get("seller_name") or r.get("seller_id") or "Unknown"
                sku_or_asin = r.get("sku") or r.get("asin") or "?"
                lines.append(
                    f"• {label} on {sku_or_asin} — "
                    f"W1:{r.get('week_1', 0)} W2:{r.get('week_2', 0)} "
                    f"W3:{r.get('week_3', 0)} W4:{r.get('week_4', 0)} "
                    f"(total {r.get('month_total', 0)})"
                )

    lines.append(f"\n{tag}{DASHBOARD_URL}")
    _post_slack("\n".join(lines))


if __name__ == "__main__":
    main()
