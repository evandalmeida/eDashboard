import streamlit as st
from datetime import date, timedelta
from dotenv import load_dotenv
from etl import fetch_shopify_sales_total, fetch_fb_ad_spend_total

load_dotenv()
st.set_page_config(page_title="Shopify Sales vs Ad Spend — MVP", page_icon="💵", layout="wide")

st.title("💵 Sales vs Ad Spend — MVP")
st.caption("Pulls total sales from Shopify and total ad spend from Meta Ads for a selected date range.")

with st.sidebar:
    start = st.date_input("Start date", date.today() - timedelta(days=30))
    end   = st.date_input("End date", date.today())
    basis = st.selectbox("Revenue basis (Shopify)", ["total_price","subtotal_price","line_items"], index=0,
                         help="total_price includes tax+shipping; subtotal_price excludes; line_items = product totals.")
    go = st.button("Fetch")

def money(x): 
    try: return f"${x:,.2f}"
    except: return "-"

if go:
    try:
        sales_total, _ = fetch_shopify_sales_total(start, end, revenue_basis=basis)
        spend_total, _ = fetch_fb_ad_spend_total(start, end)
        profit_simple = sales_total - spend_total

        c1, c2, c3 = st.columns(3)
        c1.metric("Shopify Sales", money(sales_total))
        c2.metric("Meta Ad Spend", money(spend_total))
        c3.metric("Simple Profit (Sales - Spend)", money(profit_simple))

    except Exception as e:
        st.error(str(e))
else:
    st.info("Pick dates and click **Fetch**.")
