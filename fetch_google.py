"""
fetch_google.py — Google Ads API data fetcher for Carousel Media
Fetches: spend, impressions, clicks, conversions, revenue, ROAS, CAC
Breakdowns: campaigns, ad groups, geography, device, daily trend
Outputs: google_7d.json, google_28d.json, google_month.json

Required GitHub Secrets:
  GOOGLE_DEVELOPER_TOKEN   — from Google Ads API Center
  GOOGLE_CLIENT_ID         — OAuth2 client ID
  GOOGLE_CLIENT_SECRET     — OAuth2 client secret
  GOOGLE_REFRESH_TOKEN     — OAuth2 refresh token
  GOOGLE_CUSTOMER_ID       — Google Ads account ID (no dashes, e.g. 1234567890)

Setup guide:
  1. Go to https://developers.google.com/google-ads/api/docs/get-started/introduction
  2. Apply for developer token in Google Ads > Tools > API Center
  3. Create OAuth2 credentials in Google Cloud Console
  4. Run oauth flow once to get refresh token (use google-auth-oauthlib)
"""

import os, json, urllib.request, urllib.parse, urllib.error
from datetime import datetime, timedelta

# ── Credentials from environment ─────────────────────────────────────────────
DEVELOPER_TOKEN  = os.environ.get("GOOGLE_DEVELOPER_TOKEN", "")
CLIENT_ID        = os.environ.get("GOOGLE_CLIENT_ID", "")
CLIENT_SECRET    = os.environ.get("GOOGLE_CLIENT_SECRET", "")
REFRESH_TOKEN    = os.environ.get("GOOGLE_REFRESH_TOKEN", "")
CUSTOMER_ID      = os.environ.get("GOOGLE_CUSTOMER_ID", "").replace("-", "")  # strip dashes

GOOGLE_ADS_VERSION = "v16"
API_BASE = f"https://googleads.googleapis.com/{GOOGLE_ADS_VERSION}/customers/{CUSTOMER_ID}"

# ── Check credentials ─────────────────────────────────────────────────────────
def has_credentials():
    return all([DEVELOPER_TOKEN, CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN, CUSTOMER_ID])

# ── Get OAuth2 access token ───────────────────────────────────────────────────
def get_access_token():
    data = urllib.parse.urlencode({
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "refresh_token": REFRESH_TOKEN,
        "grant_type":    "refresh_token",
    }).encode()
    req = urllib.request.Request(
        "https://oauth2.googleapis.com/token",
        data=data,
        method="POST"
    )
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())["access_token"]

# ── Google Ads Query (GAQL) ───────────────────────────────────────────────────
def gaql(query, access_token):
    """Run a GAQL query against Google Ads API and return rows."""
    url = f"{API_BASE}/googleAds:searchStream"
    body = json.dumps({"query": query}).encode()
    req = urllib.request.Request(url, data=body, method="POST", headers={
        "Content-Type":          "application/json",
        "Authorization":         f"Bearer {access_token}",
        "developer-token":       DEVELOPER_TOKEN,
        "login-customer-id":     CUSTOMER_ID,
    })
    try:
        with urllib.request.urlopen(req) as r:
            # searchStream returns newline-delimited JSON objects
            raw = r.read().decode()
            rows = []
            for line in raw.strip().split("\n"):
                if not line.strip():
                    continue
                obj = json.loads(line)
                if isinstance(obj, list):
                    for batch in obj:
                        rows.extend(batch.get("results", []))
                elif "results" in obj:
                    rows.extend(obj["results"])
            return rows
    except urllib.error.HTTPError as e:
        body = e.read().decode()
        print(f"  Google Ads API error: {e.code} — {body[:300]}")
        return []
    except Exception as e:
        print(f"  Google Ads warning: {e}")
        return []

# ── Helpers ───────────────────────────────────────────────────────────────────
def flt(v):
    try: return round(float(v), 2)
    except: return 0.0

def num(v):
    try: return int(float(v))
    except: return 0

def micros(v):
    """Convert Google Ads micros (millionths) to rupees."""
    try: return round(float(v) / 1_000_000, 2)
    except: return 0.0

def safe_div(a, b, decimals=2):
    return round(a / b, decimals) if b else 0

# ── Date range helpers ────────────────────────────────────────────────────────
def date_range_for(preset):
    today = datetime.utcnow().date()
    if preset == "last_7d":
        return (today - timedelta(days=7)).isoformat(), today.isoformat()
    elif preset == "last_28d":
        return (today - timedelta(days=28)).isoformat(), today.isoformat()
    elif preset == "this_month":
        return today.replace(day=1).isoformat(), today.isoformat()
    return (today - timedelta(days=28)).isoformat(), today.isoformat()

# ── Main fetch function ───────────────────────────────────────────────────────
def fetch_for_range(preset, access_token):
    since, until = date_range_for(preset)
    print(f"  Range: {since} to {until}")

    date_cond = f"segments.date BETWEEN '{since}' AND '{until}'"

    # ── Account summary ───────────────────────────────────────────────────────
    summary_rows = gaql(f"""
        SELECT
            metrics.cost_micros,
            metrics.impressions,
            metrics.clicks,
            metrics.ctr,
            metrics.average_cpm,
            metrics.average_cpc,
            metrics.conversions,
            metrics.conversions_value,
            metrics.search_impression_share
        FROM customer
        WHERE {date_cond}
        LIMIT 1
    """, access_token)

    summary_raw = summary_rows[0].get("metrics", {}) if summary_rows else {}
    spend    = micros(summary_raw.get("costMicros", 0))
    impr     = num(summary_raw.get("impressions", 0))
    clicks   = num(summary_raw.get("clicks", 0))
    convs    = flt(summary_raw.get("conversions", 0))
    revenue  = flt(summary_raw.get("conversionsValue", 0))

    summary = {
        "spend":        spend,
        "impressions":  impr,
        "clicks":       clicks,
        "ctr":          flt(summary_raw.get("ctr", 0)),
        "cpm":          micros(summary_raw.get("averageCpm", 0)),
        "cpc":          micros(summary_raw.get("averageCpc", 0)),
        "conversions":  num(convs),
        "revenue":      revenue,
        "roas":         safe_div(revenue, spend),
        "cac":          safe_div(spend, convs),
        "impression_share": flt(summary_raw.get("searchImpressionShare", 0)),
    }

    # ── Campaigns ─────────────────────────────────────────────────────────────
    camp_rows = gaql(f"""
        SELECT
            campaign.id,
            campaign.name,
            campaign.status,
            campaign.advertising_channel_type,
            metrics.cost_micros,
            metrics.impressions,
            metrics.clicks,
            metrics.ctr,
            metrics.conversions,
            metrics.conversions_value,
            metrics.average_cpc,
            metrics.average_cpm
        FROM campaign
        WHERE {date_cond}
            AND campaign.status != 'REMOVED'
        ORDER BY metrics.cost_micros DESC
        LIMIT 25
    """, access_token)

    campaigns = []
    for row in camp_rows:
        c  = row.get("campaign", {})
        m  = row.get("metrics", {})
        cs = micros(m.get("costMicros", 0))
        cp = flt(m.get("conversions", 0))
        cr = flt(m.get("conversionsValue", 0))
        campaigns.append({
            "id":         c.get("id", ""),
            "name":       c.get("name", ""),
            "status":     c.get("status", ""),
            "type":       c.get("advertisingChannelType", ""),
            "spend":      cs,
            "impressions":num(m.get("impressions", 0)),
            "clicks":     num(m.get("clicks", 0)),
            "ctr":        flt(m.get("ctr", 0)),
            "cpc":        micros(m.get("averageCpc", 0)),
            "conversions":num(cp),
            "revenue":    cr,
            "roas":       safe_div(cr, cs),
            "cac":        safe_div(cs, cp),
        })

    # ── Ad Groups ─────────────────────────────────────────────────────────────
    ag_rows = gaql(f"""
        SELECT
            ad_group.id,
            ad_group.name,
            ad_group.status,
            campaign.name,
            metrics.cost_micros,
            metrics.impressions,
            metrics.clicks,
            metrics.ctr,
            metrics.conversions,
            metrics.conversions_value,
            metrics.average_cpc
        FROM ad_group
        WHERE {date_cond}
            AND ad_group.status != 'REMOVED'
        ORDER BY metrics.cost_micros DESC
        LIMIT 30
    """, access_token)

    adgroups = []
    for row in ag_rows:
        ag = row.get("adGroup", {})
        c  = row.get("campaign", {})
        m  = row.get("metrics", {})
        ags = micros(m.get("costMicros", 0))
        agp = flt(m.get("conversions", 0))
        agr = flt(m.get("conversionsValue", 0))
        adgroups.append({
            "id":           ag.get("id", ""),
            "name":         ag.get("name", ""),
            "status":       ag.get("status", ""),
            "campaign":     c.get("name", ""),
            "spend":        ags,
            "impressions":  num(m.get("impressions", 0)),
            "clicks":       num(m.get("clicks", 0)),
            "ctr":          flt(m.get("ctr", 0)),
            "cpc":          micros(m.get("averageCpc", 0)),
            "conversions":  num(agp),
            "revenue":      agr,
            "roas":         safe_div(agr, ags),
            "cac":          safe_div(ags, agp),
        })

    # ── Keywords / Search Terms ───────────────────────────────────────────────
    kw_rows = gaql(f"""
        SELECT
            search_term_view.search_term,
            metrics.cost_micros,
            metrics.impressions,
            metrics.clicks,
            metrics.ctr,
            metrics.conversions,
            metrics.conversions_value
        FROM search_term_view
        WHERE {date_cond}
        ORDER BY metrics.cost_micros DESC
        LIMIT 30
    """, access_token)

    keywords = []
    for row in kw_rows:
        st = row.get("searchTermView", {})
        m  = row.get("metrics", {})
        ks = micros(m.get("costMicros", 0))
        kp = flt(m.get("conversions", 0))
        kr = flt(m.get("conversionsValue", 0))
        keywords.append({
            "term":        st.get("searchTerm", ""),
            "spend":       ks,
            "impressions": num(m.get("impressions", 0)),
            "clicks":      num(m.get("clicks", 0)),
            "ctr":         flt(m.get("ctr", 0)),
            "conversions": num(kp),
            "revenue":     kr,
            "roas":        safe_div(kr, ks),
            "cac":         safe_div(ks, kp),
        })

    # ── Geography ─────────────────────────────────────────────────────────────
    geo_rows = gaql(f"""
        SELECT
            geographic_view.country_criterion_id,
            segments.geo_target_region,
            metrics.cost_micros,
            metrics.impressions,
            metrics.clicks,
            metrics.conversions,
            metrics.conversions_value
        FROM geographic_view
        WHERE {date_cond}
        ORDER BY metrics.cost_micros DESC
        LIMIT 30
    """, access_token)

    geography = []
    for row in geo_rows:
        seg = row.get("segments", {})
        m   = row.get("metrics", {})
        gs  = micros(m.get("costMicros", 0))
        gp  = flt(m.get("conversions", 0))
        gr  = flt(m.get("conversionsValue", 0))
        gimpr = num(m.get("impressions", 0))
        gclicks = num(m.get("clicks", 0))
        geography.append({
            "region":      seg.get("geoTargetRegion", "Unknown"),
            "spend":       gs,
            "impressions": gimpr,
            "clicks":      gclicks,
            "ctr":         safe_div(gclicks * 100, gimpr, 2),
            "conversions": num(gp),
            "revenue":     gr,
            "roas":        safe_div(gr, gs),
            "cac":         safe_div(gs, gp),
        })

    # ── Device breakdown ──────────────────────────────────────────────────────
    dev_rows = gaql(f"""
        SELECT
            segments.device,
            metrics.cost_micros,
            metrics.impressions,
            metrics.clicks,
            metrics.ctr,
            metrics.conversions,
            metrics.conversions_value
        FROM campaign
        WHERE {date_cond}
        ORDER BY metrics.cost_micros DESC
    """, access_token)

    device_map = {}
    for row in dev_rows:
        device = row.get("segments", {}).get("device", "UNKNOWN")
        m = row.get("metrics", {})
        if device not in device_map:
            device_map[device] = {"device": device.replace("_", " ").title(), "spend": 0, "impressions": 0, "clicks": 0, "conversions": 0, "revenue": 0}
        device_map[device]["spend"]       += micros(m.get("costMicros", 0))
        device_map[device]["impressions"] += num(m.get("impressions", 0))
        device_map[device]["clicks"]      += num(m.get("clicks", 0))
        device_map[device]["conversions"] += num(flt(m.get("conversions", 0)))
        device_map[device]["revenue"]     += flt(m.get("conversionsValue", 0))

    devices = []
    for d in device_map.values():
        d["ctr"]  = safe_div(d["clicks"] * 100, d["impressions"])
        d["roas"] = safe_div(d["revenue"], d["spend"])
        d["cac"]  = safe_div(d["spend"], d["conversions"])
        devices.append(d)
    devices.sort(key=lambda x: x["spend"], reverse=True)

    # ── Daily trend ───────────────────────────────────────────────────────────
    trend_rows = gaql(f"""
        SELECT
            segments.date,
            metrics.cost_micros,
            metrics.impressions,
            metrics.clicks,
            metrics.conversions,
            metrics.conversions_value
        FROM customer
        WHERE {date_cond}
        ORDER BY segments.date ASC
        LIMIT 90
    """, access_token)

    spend_trend = []
    for row in trend_rows:
        seg = row.get("segments", {})
        m   = row.get("metrics", {})
        ds  = micros(m.get("costMicros", 0))
        dp  = flt(m.get("conversions", 0))
        dr  = flt(m.get("conversionsValue", 0))
        date_str = seg.get("date", "")
        spend_trend.append({
            "day":         date_str[-5:] if date_str else "",
            "date":        date_str,
            "spend":       ds,
            "impressions": num(m.get("impressions", 0)),
            "clicks":      num(m.get("clicks", 0)),
            "conversions": num(dp),
            "revenue":     dr,
            "roas":        safe_div(dr, ds),
            "cac":         safe_div(ds, dp),
        })

    # ── Pacing ────────────────────────────────────────────────────────────────
    today = datetime.utcnow()
    days_elapsed = today.day
    days_remaining = 30 - days_elapsed
    daily_avg = safe_div(summary["spend"], days_elapsed)
    projected_month = round(daily_avg * 30, 2)
    proj_convs = safe_div(summary["conversions"], days_elapsed) * 30
    pacing = {
        "days_elapsed":        days_elapsed,
        "days_remaining":      days_remaining,
        "spend_to_date":       summary["spend"],
        "daily_avg":           daily_avg,
        "projected_month":     projected_month,
        "conversions_to_date": summary["conversions"],
        "projected_conversions": round(proj_convs),
        "projected_cac":       safe_div(projected_month, proj_convs),
    }

    return {
        "platform":      "google",
        "summary":       summary,
        "campaigns":     campaigns,
        "adgroups":      adgroups,
        "keywords":      keywords,
        "geography":     geography,
        "devices":       devices,
        "spendTrend":    spend_trend,
        "pacing":        pacing,
        "lastUpdated":   datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        "dateRange":     f"{since} to {until}",
        "shopify_connected": False,  # Google doesn't pull Shopify directly
    }

# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if not has_credentials():
        print("⚠ Google Ads credentials not set — skipping Google fetch.")
        print("  Set these GitHub Secrets: GOOGLE_DEVELOPER_TOKEN, GOOGLE_CLIENT_ID,")
        print("  GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN, GOOGLE_CUSTOMER_ID")
        # Write empty placeholder files so dashboard doesn't break
        empty = {"platform":"google","summary":{},"campaigns":[],"adgroups":[],"keywords":[],"geography":[],"devices":[],"spendTrend":[],"pacing":{},"lastUpdated":"Not configured","dateRange":"","shopify_connected":False}
        for fname in ["google_7d.json","google_28d.json","google_month.json"]:
            with open(fname,"w") as f: json.dump(empty,f)
        exit(0)

    print("Authenticating with Google Ads API...")
    try:
        token = get_access_token()
        print("  ✓ Access token obtained")
    except Exception as e:
        print(f"  ✗ OAuth2 failed: {e}")
        exit(1)

    for preset, filename in [
        ("last_7d",    "google_7d.json"),
        ("last_28d",   "google_28d.json"),
        ("this_month", "google_month.json"),
    ]:
        print(f"Fetching Google Ads {preset}...")
        try:
            data = fetch_for_range(preset, token)
            with open(filename, "w") as f:
                json.dump(data, f, indent=2)
            print(f"  ✓ Saved {filename} — {len(data['campaigns'])} campaigns, {len(data['keywords'])} search terms")
        except Exception as e:
            print(f"  ✗ Failed {preset}: {e}")

    print("\nGoogle Ads fetch complete!")
