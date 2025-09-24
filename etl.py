import os, json, requests, pandas as pd
from dotenv import load_dotenv
load_dotenv()

SHOPIFY_STORE_DOMAIN = os.getenv("SHOPIFY_STORE_DOMAIN", "")
SHOPIFY_ACCESS_TOKEN = os.getenv("SHOPIFY_ACCESS_TOKEN", "")
SHOPIFY_API_VERSION  = os.getenv("SHOPIFY_API_VERSION", "2024-10")
FB_ACCESS_TOKEN      = os.getenv("FB_ACCESS_TOKEN", "")
FB_AD_ACCOUNT_ID     = os.getenv("FB_AD_ACCOUNT_ID", "")

def fetch_shopify_sales_total(start_date, end_date, revenue_basis="total_price"):
    """
    Returns (total_sales_float, orders_df).
    revenue_basis options:
      - 'total_price' (includes tax+shipping)
      - 'subtotal_price' (excludes tax+shipping)
      - 'line_items' (sum of total_line_items_price)
    """
    if not SHOPIFY_STORE_DOMAIN or not SHOPIFY_ACCESS_TOKEN:
        raise RuntimeError("Missing Shopify credentials.")

    url = f"https://{SHOPIFY_STORE_DOMAIN}/admin/api/{SHOPIFY_API_VERSION}/orders.json"
    headers = {"X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN}
    params = {
        "status": "any",
        "limit": 250,
        "created_at_min": pd.to_datetime(start_date).tz_localize("UTC").isoformat(),
        "created_at_max": pd.to_datetime(end_date).tz_localize("UTC").isoformat(),
        "fields": "id,created_at,total_price,total_tax,total_discounts,total_line_items_price,subtotal_price"
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
        # pagination: Link header with page_info
        link = resp.headers.get("Link", "")
        if 'rel="next"' in link:
            import re
            m = re.search(r'page_info=([^&>]+)', link)
            params = {"limit": 250, "page_info": m.group(1)} if m else None
            if not params: break
        else:
            break

    df = pd.json_normalize(orders)
    if df.empty:
        return 0.0, df

    if revenue_basis == "subtotal_price":
        total = pd.to_numeric(df.get("subtotal_price", 0), errors="coerce").sum()
    elif revenue_basis == "line_items":
        total = pd.to_numeric(df.get("total_line_items_price", 0), errors="coerce").sum()
    else:  # 'total_price'
        total = pd.to_numeric(df.get("total_price", 0), errors="coerce").sum()

    return float(total), df

def fetch_fb_ad_spend_total(start_date, end_date):
    """Returns (total_spend_float, df_with_daily_spend)."""
    if not FB_ACCESS_TOKEN or not FB_AD_ACCOUNT_ID:
        raise RuntimeError("Missing Facebook credentials.")

    url = f"https://graph.facebook.com/v19.0/{FB_AD_ACCOUNT_ID}/insights"
    params = {
        "access_token": FB_ACCESS_TOKEN,
        "time_range": json.dumps({
            "since": str(pd.to_datetime(start_date).date()),
            "until": str(pd.to_datetime(end_date).date())
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
        return 0.0, pd.DataFrame(columns=["date","fb_spend"])
    df = pd.DataFrame(data)
    df["date"] = pd.to_datetime(df["date_start"]).dt.date
    df["fb_spend"] = pd.to_numeric(df["spend"], errors="coerce").fillna(0.0)
    return float(df["fb_spend"].sum()), df[["date","fb_spend"]]
