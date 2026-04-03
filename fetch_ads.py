import os, json, urllib.request, urllib.parse
from datetime import datetime, timedelta

TOKEN = os.environ["META_ACCESS_TOKEN"]
ACCOUNT = os.environ["META_AD_ACCOUNT_ID"]
BASE = "https://graph.facebook.com/v19.0"

# Shopify credentials - add these as GitHub Secrets
SHOPIFY_TOKEN = os.environ.get("SHOPIFY_ADMIN_TOKEN", "")
SHOPIFY_STORE = os.environ.get("SHOPIFY_STORE_URL", "")  # e.g. your-store.myshopify.com

def get(endpoint, params={}):
    p = dict(params)
    p["access_token"] = TOKEN
    url = f"{BASE}/{endpoint}?{urllib.parse.urlencode(p)}"
    try:
        with urllib.request.urlopen(url) as r:
            d = json.loads(r.read())
        if "error" in d:
            raise Exception(d["error"]["message"])
        return d
    except Exception as e:
        print(f"  Warning: {endpoint} — {e}")
        return {"data": []}

def shopify_get(endpoint, params={}):
    if not SHOPIFY_TOKEN or not SHOPIFY_STORE:
        return {"orders": []}
    qs = urllib.parse.urlencode(params)
    url = f"https://{SHOPIFY_STORE}/admin/api/2024-01/{endpoint}.json?{qs}"
    req = urllib.request.Request(url, headers={"X-Shopify-Access-Token": SHOPIFY_TOKEN})
    try:
        with urllib.request.urlopen(req) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f"  Shopify warning: {endpoint} — {e}")
        return {"orders": []}

def flt(v):
    try: return round(float(v), 2)
    except: return 0

def num(v):
    try: return int(float(v))
    except: return 0

def ga(actions, t):
    if not actions: return 0
    return next((flt(a.get("value", 0)) for a in actions if a.get("action_type") == t), 0)

def safe_ins(data_list):
    """Safely get first insight row — returns empty dict if no data"""
    if not data_list:
        return {}
    return data_list[0] if data_list else {}

def fetch_shopify_revenue_by_province(since, until):
    """Fetch Shopify orders and group revenue by Indian state/province"""
    if not SHOPIFY_TOKEN or not SHOPIFY_STORE:
        return {}
    print("  Fetching Shopify orders for geography revenue...")
    province_revenue = {}
    page_info = None
    while True:
        params = {
            "status": "any",
            "financial_status": "paid",
            "created_at_min": f"{since}T00:00:00+05:30",
            "created_at_max": f"{until}T23:59:59+05:30",
            "limit": 250,
            "fields": "total_price,shipping_address"
        }
        if page_info:
            params["page_info"] = page_info
        result = shopify_get("orders", params)
        orders = result.get("orders", [])
        if not orders:
            break
        for order in orders:
            addr = order.get("shipping_address") or {}
            province = addr.get("province") or addr.get("city") or "Unknown"
            revenue = flt(order.get("total_price", 0))
            if province not in province_revenue:
                province_revenue[province] = {"revenue": 0, "orders": 0}
            province_revenue[province]["revenue"] += revenue
            province_revenue[province]["orders"] += 1
        if len(orders) < 250:
            break
        # Shopify pagination via link header not easily accessible here, break after first page
        break
    return province_revenue

def fetch_shopify_total_revenue(since, until):
    """Fetch Total Sales from Shopify = total_price of all non-voided orders
    Matches 'Total sales' number shown in Shopify Finance Summary"""
    if not SHOPIFY_TOKEN or not SHOPIFY_STORE:
        return 0.0, 0
    print(f"  Fetching Shopify total sales ({since} to {until} IST)...")
    total_revenue = 0.0
    total_orders  = 0
    min_id = None

    while True:
        params = {
            "status": "any",
            "created_at_min": f"{since}T00:00:00+05:30",
            "created_at_max": f"{until}T23:59:59+05:30",
            "limit": 250,
            "fields": "id,total_price,financial_status",
            "order": "id asc",
        }
        if min_id:
            params["since_id"] = min_id

        result = shopify_get("orders", params)
        orders = result.get("orders", [])
        if not orders:
            break

        for order in orders:
            # Skip only voided orders — matches Shopify's Total Sales calculation
            if order.get("financial_status") != "voided":
                total_revenue += flt(order.get("total_price", 0))
                total_orders  += 1

        min_id = orders[-1].get("id")
        if len(orders) < 250:
            break

    print(f"  Shopify total sales: Rs.{round(total_revenue, 2)} from {total_orders} orders")
    return round(total_revenue, 2), total_orders

def fetch_for_range(date_preset, since=None, until=None):
    dp = {"date_preset": date_preset} if date_preset else {"time_range": json.dumps({"since": since, "until": until})}
    label = date_preset or f"{since} to {until}"
    print(f"  Range: {label}")

    # Determine since/until for Shopify calls — use IST (UTC+5:30)
    today = datetime.utcnow() + timedelta(hours=5, minutes=30)
    if date_preset == "last_7d":
        sh_since = (today - timedelta(days=7)).strftime("%Y-%m-%d")
        sh_until = today.strftime("%Y-%m-%d")
    elif date_preset == "last_28d":
        sh_since = (today - timedelta(days=28)).strftime("%Y-%m-%d")
        sh_until = today.strftime("%Y-%m-%d")
    elif date_preset == "this_month":
        sh_since = today.replace(day=1).strftime("%Y-%m-%d")
        sh_until = today.strftime("%Y-%m-%d")
    else:
        sh_since = since or (today - timedelta(days=28)).strftime("%Y-%m-%d")
        sh_until = until or today.strftime("%Y-%m-%d")

    # ── Summary ──────────────────────────────────────────────────
    # Use link_clicks instead of clicks(all) for funnel accuracy
    s = get(f"{ACCOUNT}/insights", {
        "fields": "spend,impressions,clicks,ctr,cpm,reach,actions,action_values,frequency",
        **dp
    })
    ins = safe_ins(s.get("data", []))
    act = ins.get("actions", [])
    av = ins.get("action_values", [])
    spend = flt(ins.get("spend", 0))
    purchases = ga(act, "purchase")
    meta_revenue = ga(av, "purchase")
    landing = ga(act, "landing_page_view")
    atc = ga(act, "add_to_cart")
    link_clicks = ga(act, "link_click")

    # ── Shopify total revenue (overrides Meta pixel revenue if available) ──
    shopify_total_revenue, shopify_total_orders = fetch_shopify_total_revenue(sh_since, sh_until)
    revenue = shopify_total_revenue if shopify_total_revenue > 0 else meta_revenue
    revenue_source = "shopify" if shopify_total_revenue > 0 else "meta_pixel"

    summary = {
        "spend": spend, "impressions": num(ins.get("impressions", 0)),
        "clicks": num(ins.get("clicks", 0)),
        "link_clicks": num(link_clicks),
        "ctr": flt(ins.get("ctr", 0)), "cpm": flt(ins.get("cpm", 0)),
        "reach": num(ins.get("reach", 0)), "conversions": num(purchases),
        "revenue": flt(revenue),
        "revenue_source": revenue_source,
        "shopify_orders": shopify_total_orders,
        "roas": round(revenue / spend, 2) if spend > 0 else 0,
        "cac": round(spend / purchases, 2) if purchases > 0 else 0,
        "landing_views": num(landing), "add_to_cart": num(atc),
        "frequency": flt(ins.get("frequency", 0)),
    }

    # ── Campaigns ────────────────────────────────────────────────
    cf_fields = "spend,impressions,clicks,ctr,cpm,reach,actions,action_values,purchase_roas,frequency"
    if date_preset:
        cf = f"id,name,status,objective,insights.date_preset({date_preset}){{{cf_fields}}}"
    else:
        cf = f"id,name,status,objective,insights.time_range({json.dumps({'since':since,'until':until})}){{{cf_fields}}}"
    camps = get(f"{ACCOUNT}/campaigns", {"fields": cf, "limit": 20})
    campaigns = []
    for c in camps.get("data", []):
        ci = safe_ins(c.get("insights", {}).get("data", []))
        ca = ci.get("actions", []); cv = ci.get("action_values", [])
        cs = flt(ci.get("spend", 0)); cp = ga(ca, "purchase")
        cr = ga(cv, "purchase"); cl = ga(ca, "landing_page_view")
        cc = ga(ca, "add_to_cart"); cimpr = num(ci.get("impressions", 0))
        # Use link_click for funnel instead of clicks(all)
        clink_clicks = num(ga(ca, "link_click"))
        creach = num(ci.get("reach", 0))
        pr = ci.get("purchase_roas", [])
        croas = 0
        if isinstance(pr, list):
            for item in pr:
                croas = flt(item.get("value", 0)) if isinstance(item, dict) else flt(item)
                break
        funnel = {
            "reach": creach, "impressions": cimpr,
            "link_clicks": clink_clicks,  # Link clicks
            "landing_views": num(cl), "add_to_cart": num(cc), "purchases": num(cp),
            "dropoffs": {
                "impressions_to_link_clicks": round((clink_clicks / cimpr) * 100, 2) if cimpr > 0 else 0,
                "link_clicks_to_landing": round((cl / clink_clicks) * 100, 1) if clink_clicks > 0 else 0,
                "landing_to_cart": round((cc / cl) * 100, 1) if cl > 0 else 0,
                "cart_to_purchase": round((cp / cc) * 100, 1) if cc > 0 else 0,
            }
        }
        campaigns.append({
            "id": c["id"], "name": c["name"], "status": c["status"], "objective": c.get("objective", ""),
            "spend": cs, "impressions": cimpr, "link_clicks": clink_clicks,
            "ctr": flt(ci.get("ctr", 0)), "cpm": flt(ci.get("cpm", 0)),
            "reach": creach, "roas": croas, "cac": round(cs / cp, 2) if cp > 0 else 0,
            "purchases": num(cp), "revenue": flt(cr), "frequency": flt(ci.get("frequency", 0)),
            "atc": num(cc), "atc_abandon_rate": round((1 - cp / cc) * 100, 1) if cc > 0 else 0,
            "funnel": funnel
        })

    # ── Ad Sets ──────────────────────────────────────────────────
    as_fields = "spend,impressions,clicks,actions,action_values,frequency"
    if date_preset:
        af = f"id,name,status,daily_budget,targeting,insights.date_preset({date_preset}){{{as_fields}}}"
    else:
        af = f"id,name,status,daily_budget,targeting,insights.time_range({json.dumps({'since':since,'until':until})}){{{as_fields}}}"
    asets = get(f"{ACCOUNT}/adsets", {"fields": af, "limit": 20})
    adsets = []
    for a in asets.get("data", []):
        ai = safe_ins(a.get("insights", {}).get("data", []))
        aa = ai.get("actions", [])
        asp = flt(ai.get("spend", 0)); ap = ga(aa, "purchase"); aatc = ga(aa, "add_to_cart")
        alink = num(ga(aa, "link_click"))
        t = a.get("targeting", {})
        g = t.get("genders")
        gender = "All" if not g else ("Male" if g == [1] else "Female" if g == [2] else "All")
        adsets.append({
            "id": a["id"], "name": a["name"], "status": a["status"],
            "daily_budget": flt(num(a.get("daily_budget", 0)) / 100),
            "spend": asp, "impressions": num(ai.get("impressions", 0)),
            "link_clicks": alink, "purchases": num(ap),
            "cac": round(asp / ap, 2) if ap > 0 else 0,
            "atc": num(aatc), "atc_abandon_rate": round((1 - ap / aatc) * 100, 1) if aatc > 0 else 0,
            "frequency": flt(ai.get("frequency", 0)),
            "age": f"{t.get('age_min', 18)}–{t.get('age_max', 65)}", "gender": gender
        })

    # ── Ads / Creatives ──────────────────────────────────────────
    ad_fields = "spend,impressions,clicks,ctr,actions,action_values,frequency"
    if date_preset:
        adf = f"id,name,status,adset_id,insights.date_preset({date_preset}){{{ad_fields}}}"
    else:
        adf = f"id,name,status,adset_id,insights.time_range({json.dumps({'since':since,'until':until})}){{{ad_fields}}}"
    ads_raw = get(f"{ACCOUNT}/ads", {"fields": adf, "limit": 30})
    ads = []
    for a in ads_raw.get("data", []):
        ai = safe_ins(a.get("insights", {}).get("data", []))
        aa = ai.get("actions", []); av3 = ai.get("action_values", [])
        asp2 = flt(ai.get("spend", 0)); ap2 = ga(aa, "purchase")
        aimpr = num(ai.get("impressions", 0)); alink2 = num(ga(aa, "link_click"))
        atc3 = ga(aa, "add_to_cart")
        thumb_stop = round((alink2 / aimpr) * 100, 2) if aimpr > 0 else 0
        # Hook rate via video_view action
        v3s = num(ga(aa, "video_view"))
        hook_rate = round((v3s / aimpr) * 100, 2) if aimpr > 0 else 0
        ads.append({
            "id": a["id"], "name": a["name"], "status": a["status"], "adset": a.get("adset_id", ""),
            "spend": asp2, "impressions": aimpr, "link_clicks": alink2,
            "ctr": flt(ai.get("ctr", 0)), "purchases": num(ap2),
            "cac": round(asp2 / ap2, 2) if ap2 > 0 else 0,
            "atc": num(atc3), "frequency": flt(ai.get("frequency", 0)),
            "hook_rate": hook_rate, "thumb_stop_rate": thumb_stop,
            "revenue": flt(ga(av3, "purchase")),
            "roas": round(flt(ga(av3, "purchase")) / asp2, 2) if asp2 > 0 else 0,
        })

    # ── Audience age/gender ──────────────────────────────────────
    aud = get(f"{ACCOUNT}/insights", {
        "fields": "reach,clicks,spend,actions,action_values,frequency",
        "breakdowns": "age,gender", "limit": 50, **dp
    })
    age_map, gmap = {}, {"male": 0, "female": 0}
    for r in aud.get("data", []):
        age = r.get("age", "Unknown")
        rs = flt(r.get("spend", 0)); rp = ga(r.get("actions", []), "purchase")
        rr = ga(r.get("action_values", []), "purchase"); rreach = num(r.get("reach", 0))
        if age not in age_map:
            age_map[age] = {"group": age, "reach": 0, "clicks": 0, "spend": 0, "purchases": 0, "revenue": 0, "frequency": 0, "count": 0}
        age_map[age]["reach"] += rreach
        age_map[age]["clicks"] += num(r.get("clicks", 0))
        age_map[age]["spend"] += rs; age_map[age]["purchases"] += num(rp)
        age_map[age]["revenue"] += flt(rr); age_map[age]["frequency"] += flt(r.get("frequency", 0))
        age_map[age]["count"] += 1
        if r.get("gender") == "male": gmap["male"] += rreach
        elif r.get("gender") == "female": gmap["female"] += rreach
    for ag in age_map:
        sp = age_map[ag]["spend"]; pu = age_map[ag]["purchases"]; cnt = age_map[ag]["count"]
        age_map[ag]["cac"] = round(sp / pu, 2) if pu > 0 else 0
        age_map[ag]["roas"] = round(age_map[ag]["revenue"] / sp, 2) if sp > 0 else 0
        age_map[ag]["avg_frequency"] = round(age_map[ag]["frequency"] / cnt, 2) if cnt > 0 else 0
    total_g = gmap["male"] + gmap["female"] or 1

    # ── Placement breakdown ───────────────────────────────────────
    plac = get(f"{ACCOUNT}/insights", {
        "fields": "reach,clicks,spend,impressions,actions,action_values",
        "breakdowns": "publisher_platform,platform_position", "limit": 30, **dp
    })
    placements = []
    for r in plac.get("data", []):
        rs = flt(r.get("spend", 0)); rp = ga(r.get("actions", []), "purchase")
        rr = ga(r.get("action_values", []), "purchase"); rimpr = num(r.get("impressions", 0))
        placements.append({
            "platform": r.get("publisher_platform", "Unknown"),
            "position": r.get("platform_position", "Unknown"),
            "spend": rs, "impressions": rimpr, "clicks": num(r.get("clicks", 0)),
            "reach": num(r.get("reach", 0)), "purchases": num(rp), "revenue": flt(rr),
            "cac": round(rs / rp, 2) if rp > 0 else 0,
            "ctr": round(num(r.get("clicks", 0)) / rimpr * 100, 2) if rimpr > 0 else 0,
            "roas": round(rr / rs, 2) if rs > 0 else 0,
        })
    placements.sort(key=lambda x: x["spend"], reverse=True)

    # ── Geography (Meta + Shopify revenue) ───────────────────────
    geo = get(f"{ACCOUNT}/insights", {
        "fields": "reach,clicks,spend,actions,action_values,impressions",
        "breakdowns": "region", "limit": 30, **dp
    })
    # Get Shopify revenue by province
    shopify_revenue = fetch_shopify_revenue_by_province(sh_since, sh_until)

    geography = []
    for r in geo.get("data", []):
        rs = flt(r.get("spend", 0)); rp = ga(r.get("actions", []), "purchase")
        rr = ga(r.get("action_values", []), "purchase")
        rimpr = num(r.get("impressions", 0))
        region = r.get("region", "Unknown")
        # Try to match Shopify province revenue
        sh_data = shopify_revenue.get(region, {})
        shopify_rev = flt(sh_data.get("revenue", 0))
        shopify_orders = num(sh_data.get("orders", 0))
        # Use Shopify revenue if available, otherwise fall back to Meta pixel revenue
        actual_revenue = shopify_rev if shopify_rev > 0 else flt(rr)
        geography.append({
            "region": region, "reach": num(r.get("reach", 0)),
            "clicks": num(r.get("clicks", 0)), "impressions": rimpr, "spend": rs,
            "purchases": num(rp), "revenue": actual_revenue,
            "shopify_orders": shopify_orders,
            "revenue_source": "shopify" if shopify_rev > 0 else "meta_pixel",
            "cac": round(rs / rp, 2) if rp > 0 else 0,
            "roas": round(actual_revenue / rs, 2) if rs > 0 else 0,
            "ctr": round(num(r.get("clicks", 0)) / rimpr * 100, 2) if rimpr > 0 else 0
        })
    geography.sort(key=lambda x: x["spend"], reverse=True)

    # ── Daily trend ───────────────────────────────────────────────
    tparams = {"fields": "spend,date_start,impressions,clicks,actions,action_values", "time_increment": 1, "limit": 60}
    if date_preset == "last_7d": tparams["date_preset"] = "last_7d"
    elif date_preset in ("last_28d", "this_month"): tparams["date_preset"] = "last_28d"
    elif since: tparams["time_range"] = json.dumps({"since": since, "until": until})
    else: tparams["date_preset"] = "last_28d"
    trend_raw = get(f"{ACCOUNT}/insights", tparams)
    trend = []
    for d in trend_raw.get("data", []):
        da = d.get("actions", []); dav = d.get("action_values", [])
        ds = flt(d.get("spend", 0)); dp2 = ga(da, "purchase"); dr = ga(dav, "purchase")
        trend.append({
            "day": d.get("date_start", "")[-5:], "spend": ds,
            "impressions": num(d.get("impressions", 0)),
            "link_clicks": num(ga(da, "link_click")),
            "purchases": num(dp2), "revenue": flt(dr),
            "cac": round(ds / dp2, 2) if dp2 > 0 else 0,
            "roas": round(dr / ds, 2) if ds > 0 else 0,
        })

    # ── Hour of day (always last_7d — works with any date preset) ─
    hour_raw = get(f"{ACCOUNT}/insights", {
        "fields": "spend,impressions,clicks,actions",
        "breakdowns": "hourly_stats_aggregated_by_advertiser_time_zone",
        "date_preset": "last_7d", "limit": 24
    })
    hours = []
    for h in hour_raw.get("data", []):
        ha = h.get("actions", []); hs = flt(h.get("spend", 0)); hp = ga(ha, "purchase")
        hours.append({
            "hour": h.get("hourly_stats_aggregated_by_advertiser_time_zone", "00")[:2],
            "spend": hs, "impressions": num(h.get("impressions", 0)),
            "clicks": num(h.get("clicks", 0)), "purchases": num(hp),
            "cac": round(hs / hp, 2) if hp > 0 else 0,
        })
    hours.sort(key=lambda x: int(x["hour"]) if str(x["hour"]).isdigit() else 0)

    # ── Budget pacing ─────────────────────────────────────────────
    today_dt = datetime.utcnow()
    days_elapsed = today_dt.day
    days_remaining = 30 - days_elapsed
    daily_avg = round(summary["spend"] / days_elapsed, 2) if days_elapsed > 0 else 0
    projected_month = round(daily_avg * 30, 2)
    pacing = {
        "days_elapsed": days_elapsed, "days_remaining": days_remaining,
        "spend_to_date": summary["spend"], "daily_avg": daily_avg,
        "projected_month": projected_month,
        "purchases_to_date": summary["conversions"],
        "projected_purchases": round(summary["conversions"] / days_elapsed * 30) if days_elapsed > 0 else 0,
        "projected_cac": round(projected_month / (summary["conversions"] / days_elapsed * 30), 2) if summary["conversions"] > 0 and days_elapsed > 0 else 0,
    }

    return {
        "summary": summary, "campaigns": campaigns, "adsets": adsets, "ads": ads,
        "spendTrend": trend, "audienceAge": sorted(age_map.values(), key=lambda x: x["group"]),
        "audienceGender": {"male": round(gmap["male"] / total_g * 100), "female": round(gmap["female"] / total_g * 100)},
        "geography": geography, "placements": placements, "hours": hours, "pacing": pacing,
        "lastUpdated": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        "dateRange": date_preset or f"{since} to {until}",
        "shopify_connected": bool(SHOPIFY_TOKEN and SHOPIFY_STORE)
    }

print("Fetching last 7 days...")
d7 = fetch_for_range("last_7d")
with open("data_7d.json", "w") as f: json.dump(d7, f, indent=2)
print(f"  ✓ Saved data_7d.json — {len(d7['campaigns'])} campaigns")

print("Fetching last 28 days...")
d28 = fetch_for_range("last_28d")
with open("data_28d.json", "w") as f: json.dump(d28, f, indent=2)
with open("data.json", "w") as f: json.dump(d28, f, indent=2)
print(f"  ✓ Saved data_28d.json — {len(d28['campaigns'])} campaigns")

print("Fetching this month...")
dm = fetch_for_range("this_month")
with open("data_month.json", "w") as f: json.dump(dm, f, indent=2)
print(f"  ✓ Saved data_month.json — {len(dm['campaigns'])} campaigns")

print(f"\nAll done! Shopify connected: {d28['shopify_connected']}")
print(f"Campaigns: {len(d28['campaigns'])} · Ads: {len(d28['ads'])} · Geos: {len(d28['geography'])} · Placements: {len(d28['placements'])}")
