import os
import json
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# ---- Env ----
SHOPIFY_STORE_DOMAIN = os.getenv("SHOPIFY_STORE_DOMAIN", "")
SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN", "")
SHOPIFY_API_VERSION  = os.getenv("SHOPIFY_API_VERSION", "2024-10")

FB_ACCESS_TOKEN      = os.getenv("FB_ACCESS_TOKEN", "")
FB_AD_ACCOUNT_ID     = os.getenv("FB_AD_ACCOUNT_ID", "")

CJ_EMAIL             = os.getenv("CJ_EMAIL", "")
CJ_API_KEY           = os.getenv("CJ_API_KEY", "")

# ---- Shopify ----
def fetch_shopify_sales_total(start_date, end_date, revenue_basis="total_price"):
    """
    Returns (total_sales_float, orders_df).
    revenue_basis:
      - 'total_price'      (includes tax+shipping)
      - 'subtotal_price'   (excludes tax+shipping)
      - 'line_items'       (sum of total_line_items_price)
    """
    if not SHOPIFY_STORE_DOMAIN or not SHOPIFY_ACCESS_TOKEN:
        raise RuntimeError("Missing Shopify credentials (domain/token).")

    url = f"https://{SHOPIFY_STORE_DOMAIN}/admin/api/{SHOPIFY_API_VERSION}/orders.json"
    headers = {"X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN}
    params = {
        "status": "any",
        "limit": 250,
        "created_at_min": pd.to_datetime(start_date).tz_localize("UTC").isoformat(),
        "created_at_max": pd.to_datetime(end_date).tz_localize("UTC").isoformat(),
        "fields": "id,created_at,total_price,subtotal_price,total_line_items_price"
    }

    orders = []
    while True:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        if not resp.ok:
            raise RuntimeError(f"Shopify {resp.status_code}: {resp.text[:400]}")
        batch = resp.json().get("orders", [])
        if not batch:
            break
        orders.extend(batch)

        # pagination via Link header (page_info)
        link = resp.headers.get("Link", "")
        if 'rel="next"' in link:
            import re
            m = re.search(r'page_info=([^&>]+)', link)
            if m:
                params = {"limit": 250, "page_info": m.group(1)}
            else:
                break
        else:
            break

    df = pd.json_normalize(orders)
    if df.empty:
        return 0.0, df

    if revenue_basis == "subtotal_price":
        total = pd.to_numeric(df.get("subtotal_price", 0), errors="coerce").sum()
    elif revenue_basis == "line_items":
        total = pd.to_numeric(df.get("total_line_items_price", 0), errors="coerce").sum()
    else:  # total_price
        total = pd.to_numeric(df.get("total_price", 0), errors="coerce").sum()

    return float(total), df


# ---- Meta (Facebook) ----
def fetch_fb_ad_spend_total(start_date, end_date):
    """
    Returns (total_spend_float, daily_df[date, fb_spend]).
    """
    if not FB_ACCESS_TOKEN or not FB_AD_ACCOUNT_ID:
        raise RuntimeError("Missing Facebook credentials (token/ad account).")

    url = f"https://graph.facebook.com/v19.0/{FB_AD_ACCOUNT_ID}/insights"
    params = {
        "access_token": FB_ACCESS_TOKEN,
        "time_range": json.dumps({
            "since": str(pd.to_datetime(start_date).date()),
            "until": str(pd.to_datetime(end_date).date()),
        }),
        "fields": "date_start,spend",
        "level": "account",
        "time_increment": 1,
    }
    resp = requests.get(url, params=params, timeout=30)
    if not resp.ok:
        raise RuntimeError(f"Meta {resp.status_code}: {resp.text[:400]}")
    data = resp.json().get("data", [])
    if not data:
        return 0.0, pd.DataFrame(columns=["date", "fb_spend"])
    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date_start"]).dt.date
    df["fb_spend"] = pd.to_numeric(df["spend"], errors="coerce").fillna(0.0)
    return float(df["fb_spend"].sum()), df[["date", "fb_spend"]]


# ---- CJdropshipping ----
CJ_BASE = "https://developers.cjdropshipping.com/api2.0/v1"
_cj_token_cache = {"accessToken": None, "expiry": None}

def _cj_get_access_token(force=False):
    """
    Get/refresh CJ access token. Caches until expiry.
    Requires CJ_EMAIL + CJ_API_KEY in env.
    """
    if not CJ_EMAIL or not CJ_API_KEY:
        raise RuntimeError("Missing CJ credentials (CJ_EMAIL/CJ_API_KEY).")

    now = pd.Timestamp.utcnow()
    if (
        not force
        and _cj_token_cache["accessToken"]
        and _cj_token_cache["expiry"] is not None
        and now < _cj_token_cache["expiry"]
    ):
        return _cj_token_cache["accessToken"]

    url = f"{CJ_BASE}/authentication/getAccessToken"
    resp = requests.post(url, json={"email": CJ_EMAIL, "apiKey": CJ_API_KEY}, timeout=30)
    if not resp.ok:
        raise RuntimeError(f"CJ auth {resp.status_code}: {resp.text[:400]}")
    data = resp.json().get("data", {})
    token = data.get("accessToken")
    expiry_raw = data.get("accessTokenExpiryDate")  # e.g. "2025-08-18T09:16:33+08:00"
    expiry = pd.to_datetime(expiry_raw, utc=True, errors="coerce") if expiry_raw else (now + pd.Timedelta(days=14))
    _cj_token_cache.update({"accessToken": token, "expiry": expiry})
    return token

def _cj_headers():
    return {"CJ-Access-Token": _cj_get_access_token()}

def fetch_cj_orders_list(page_size=100, page_num=1, status=None):
    """
    Returns raw list payload: { list: [...], total: N, ... }
    """
    params = {"pageNum": page_num, "pageSize": page_size}
    if status:
        params["status"] = status  # CREATED, UNPAID, UNSHIPPED, SHIPPED, DELIVERED, etc.
    url = f"{CJ_BASE}/shopping/order/list"
    r = requests.get(url, headers=_cj_headers(), params=params, timeout=30)
    if not r.ok:
        raise RuntimeError(f"CJ list {r.status_code}: {r.text[:400]}")
    return r.json().get("data", {})

def fetch_cj_costs_by_day(start_date, end_date):
    """
    Returns (daily_cost_df[date, cj_cost], total_cost_float).
    Cost per order = prefer orderAmount; else (productAmount + postageAmount).
    Aggregated by createDate (UTC) day.
    """
    start_ts = pd.to_datetime(start_date, utc=True)
    end_ts   = pd.to_datetime(end_date, utc=True) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

    page_num = 1
    page_size = 100
    rows = []

    while True:
        data = fetch_cj_orders_list(page_size=page_size, page_num=page_num)
        items = (data or {}).get("list", [])
        if not items:
            break

        for it in items:
            c_at = it.get("createDate")  # e.g. "2021-03-31 00:46:39"
            if not c_at:
                continue
            c_ts = pd.to_datetime(c_at, utc=True, errors="coerce")
            if c_ts is pd.NaT:
                continue
            if start_ts <= c_ts <= end_ts:
                order_amt   = pd.to_numeric(it.get("orderAmount"), errors="coerce")
                product_amt = pd.to_numeric(it.get("productAmount"), errors="coerce")
                postage_amt = pd.to_numeric(it.get("postageAmount"), errors="coerce")

                if pd.notna(order_amt):
                    cost = float(order_amt)
                else:
                    pa = 0.0 if pd.isna(product_amt) else float(product_amt)
                    po = 0.0 if pd.isna(postage_amt) else float(postage_amt)
                    cost = pa + po

                rows.append({"date": c_ts.date(), "cj_cost": cost})

        total = (data or {}).get("total", 0)
        if page_num * page_size >= int(total or 0):
            break
        page_num += 1

    if not rows:
        df = pd.DataFrame(columns=["date", "cj_cost"])
        return df, 0.0

    df = pd.DataFrame(rows).groupby("date", as_index=False)["cj_cost"].sum()
    return df, float(df["cj_cost"].sum())
