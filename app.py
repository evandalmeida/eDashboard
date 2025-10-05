import os, json
from datetime import date, timedelta
from flask import Flask, render_template, request, redirect, url_for, flash
from dotenv import load_dotenv
import pandas as pd

from etl import (
    fetch_shopify_sales_total,
    fetch_fb_ad_spend_total,
    fetch_cj_costs_by_day,
)

load_dotenv()
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "dev-secret")  # for flash messages

LOCAL_TZ = "America/New_York"


def money(x):
    try:
        return f"${float(x):,.2f}"
    except Exception:
        return "-"


def pct(x):
    try:
        return f"{float(x)*100:,.1f}%"
    except Exception:
        return "—"


def _amount_series(orders_df: pd.DataFrame, basis: str) -> pd.Series:
    """
    FIX: orders_df.get(col, 0) returns a scalar 0 when missing, which breaks .fillna().
    We instead return a 0.0 Series aligned to the index if the column is absent.
    """
    if basis == "subtotal_price":
        col = "subtotal_price"
    elif basis == "line_items":
        col = "total_line_items_price"
    else:
        col = "total_price"

    if col in orders_df.columns:
        s = pd.to_numeric(orders_df[col], errors="coerce")
    else:
        s = pd.Series(0.0, index=orders_df.index, dtype="float64")
    return s.fillna(0.0)


def build_sales_aggregates(orders_df, basis, start_dt, end_dt):
    """Return dict with:
       - daily line: dates, values
       - avg_by_dow: labels, values
       - avg_by_hour: hours, values
    """
    # Build date index (inclusive start..end) in local tz
    start_ts = pd.Timestamp(start_dt).tz_localize(LOCAL_TZ)
    end_ts = pd.Timestamp(end_dt).tz_localize(LOCAL_TZ)
    date_index = pd.date_range(start_ts, end_ts, freq="D")

    if orders_df is None or orders_df.empty:
        return {
            "daily": {"dates": [d.date().isoformat() for d in date_index], "values": [0]*len(date_index)},
            "dow":   {"labels": ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"], "values": [0]*7},
            "hour":  {"hours": list(range(24)), "values": [0]*24},
        }

    # Parse & convert order timestamps
    created_utc = pd.to_datetime(orders_df["created_at"], utc=True, errors="coerce")
    created_local = created_utc.dt.tz_convert(LOCAL_TZ)

    orders_df = orders_df.copy()
    orders_df["_created_local"] = created_local
    orders_df["_amount"] = _amount_series(orders_df, basis)

    # Daily totals (ensure zero-filled across the whole range)
    daily_sales = (
        orders_df.set_index("_created_local")["_amount"]
        .sort_index()
        .resample("D")
        .sum()
        .reindex(date_index, fill_value=0.0)
    )
    daily_dates = [d.tz_convert(None).date().isoformat() for d in daily_sales.index]
    daily_values = [float(v) for v in daily_sales.values]

    # Average by weekday (Mon–Sun)
    by_date = daily_sales.reset_index()
    by_date.columns = ["date_local", "sales"]
    by_date["dow_num"] = by_date["date_local"].dt.dayofweek
    dow_map = {0:"Mon",1:"Tue",2:"Wed",3:"Thu",4:"Fri",5:"Sat",6:"Sun"}
    by_date["dow"] = by_date["dow_num"].map(dow_map)
    avg_by_dow = (
        by_date.groupby(["dow_num","dow"], as_index=False)["sales"]
        .mean()
        .sort_values("dow_num")
    )
    dow_labels = avg_by_dow["dow"].tolist()
    dow_values = [float(x) for x in avg_by_dow["sales"].tolist()]

    # Average by hour (0–23), across days in range
    per_order = orders_df[["_created_local","_amount"]].copy()
    per_order["hour"] = per_order["_created_local"].dt.hour
    by_hour = per_order.groupby("hour", as_index=False)["_amount"].mean().rename(columns={"_amount":"sales"})
    # Ensure all 24 hours present
    hours = pd.DataFrame({"hour": list(range(24))})
    avg_by_hour = hours.merge(by_hour, how="left", on="hour").fillna({"sales": 0.0})
    hour_labels = avg_by_hour["hour"].tolist()
    hour_values = [float(x) for x in avg_by_hour["sales"].tolist()]

    return {
        "daily": {"dates": daily_dates, "values": daily_values},
        "dow":   {"labels": dow_labels, "values": dow_values},
        "hour":  {"hours": hour_labels, "values": hour_values},
    }


@app.route("/", methods=["GET", "POST"])
def dashboard():
    # Defaults
    default_start = (date.today() - timedelta(days=30)).isoformat()
    default_end = date.today().isoformat()
    basis_default = "total_price"

    if request.method == "POST":
        start = request.form.get("start", default_start)
        end   = request.form.get("end", default_end)
        basis = request.form.get("basis", basis_default)
        return redirect(url_for("dashboard", start=start, end=end, basis=basis))

    # GET with params
    start = request.args.get("start", default_start)
    end   = request.args.get("end", default_end)
    basis = request.args.get("basis", basis_default)

    # Fetch + compute
    kpis = {}
    charts = {}
    table_rows = []

    try:
        sales_total, orders_df = fetch_shopify_sales_total(start, end, revenue_basis=basis)
        ad_spend_total, fb_daily = fetch_fb_ad_spend_total(start, end)

        # CJ costs
        try:
            cj_daily, cogs_total, cj_meta = fetch_cj_costs_by_day(start, end, use_cache=True)
        except TypeError:
            # Backward-compatible signature
            cj_daily, cogs_total = fetch_cj_costs_by_day(start, end)
            cj_meta = {}

        # ROI/ROAS
        roas = (sales_total / ad_spend_total) if ad_spend_total > 0 else None
        profit = sales_total - (ad_spend_total + cogs_total)
        denom = (ad_spend_total + cogs_total)
        roi = (profit / denom) if denom > 0 else None

        kpis = {
            "sales_total": money(sales_total),
            "ad_spend_total": money(ad_spend_total),
            "cogs_total": money(cogs_total),
            "profit": money(profit),
            "roi": pct(roi) if roi is not None else "—",
            "roas": pct(roas) if roas is not None else "—",
        }

        charts = build_sales_aggregates(orders_df, basis, start, end)

        # --- Build daily table (sales, fb, cj, net) ---
        sales_daily = pd.DataFrame({
            "date": pd.to_datetime(charts["daily"]["dates"]).date,
            "shopify_sales": charts["daily"]["values"],
        })
        fb_daily = (fb_daily.rename(columns={"date": "date"})
                    if not fb_daily.empty else pd.DataFrame(columns=["date","fb_spend"]))
        cj_daily = (cj_daily.rename(columns={"date": "date"})
                    if not cj_daily.empty else pd.DataFrame(columns=["date","cj_cost"]))

        # Ensure date types match
        if not fb_daily.empty:
            fb_daily["date"] = pd.to_datetime(fb_daily["date"]).dt.date
        if not cj_daily.empty:
            cj_daily["date"] = pd.to_datetime(cj_daily["date"]).dt.date

        df = (
            sales_daily.merge(fb_daily, on="date", how="left")
                       .merge(cj_daily, on="date", how="left")
                       .fillna({"fb_spend": 0.0, "cj_cost": 0.0})
                       .sort_values("date")
        )
        df["net"] = df["shopify_sales"] - (df["fb_spend"] + df["cj_cost"])
        table_rows = df.to_dict(orient="records")

        # CJ notices
        if isinstance(cj_meta, dict) and cj_meta.get("error"):
            flash(f"CJ error: {cj_meta['error']}", "warning")
        else:
            if isinstance(cj_meta, dict) and cj_meta.get("source") == "cache":
                flash(f"CJ COGS from cache (cached_at: {cj_meta.get('cached_at')})", "info")
            if isinstance(cj_meta, dict) and cj_meta.get("truncated"):
                flash("CJ returned >1 page; only first page fetched to respect 1 req/300s. Narrow date range or try later.", "warning")

    except Exception as e:
        flash(str(e), "danger")

    return render_template(
        "dashboard.html",
        start=start, end=end, basis=basis,
        kpis=kpis,
        charts_json=json.dumps(charts),
        fb_hourly_json=None,  # REMOVED: no hourly Meta graph
        table_rows=table_rows,
    )


if __name__ == "__main__":
    app.run(debug=True)
