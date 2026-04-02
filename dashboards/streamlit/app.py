import streamlit as st
import duckdb
import pandas as pd

# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------
DATA_BASE = "/home/vicky/global-commerce-intelligence-platform/data/gold"

st.set_page_config(
    page_title="Global Commerce Intelligence Dashboard",
    layout="wide"
)

# ---------------------------------------------------------
# Helper: Load a Gold table
# ---------------------------------------------------------
def load_gold(table: str) -> pd.DataFrame:
    path = f"{DATA_BASE}/{table}"
    try:
        return duckdb.sql(f"SELECT * FROM delta_scan('{path}')").df()
    except:
        return pd.DataFrame()

# ---------------------------------------------------------
# Sidebar Navigation
# ---------------------------------------------------------
st.sidebar.title("📊 Navigation")

page = st.sidebar.radio(
    "Go to",
    [
        "🏠 Overview",
        "💰 Revenue",
        "💳 Payments",
        "📦 Shipments",
        "🛒 Engagement",
        "🏬 Inventory"
    ]
)

regions = ["US", "EU", "IN", "CA"]
selected_region = st.sidebar.selectbox("Region", regions)

# ---------------------------------------------------------
# Load all Gold tables once
# ---------------------------------------------------------
revenue_df = load_gold("revenue")
payments_df = load_gold("payment_success_rate")
inventory_df = load_gold("inventory_health")
shipments_df = load_gold("shipment_performance")
engagement_df = load_gold("customer_engagement")

# ---------------------------------------------------------
# ALERT SYSTEM (only for tables that have hour_start)
# ---------------------------------------------------------
def show_alerts():
    st.subheader("🚨 Real-Time Alerts")

    # ---------------- PAYMENT ALERT ----------------
    if not payments_df.empty:
        region_df = payments_df[payments_df["region"] == selected_region]
        if not region_df.empty:
            latest = region_df.sort_values("hour_start").tail(1)
            rate = latest["success_rate"].iloc[0] * 100

            if rate < 70:
                st.error(f"❌ CRITICAL: Payment success rate dropped to {rate:.2f}%")
            elif rate < 85:
                st.warning(f"⚠️ Payment success rate is {rate:.2f}%")
            else:
                st.success(f"✅ Payment success rate healthy at {rate:.2f}%")

    # ---------------- REVENUE ALERT ----------------
    if not revenue_df.empty:
        region_df = revenue_df[revenue_df["region"] == selected_region]
        if not region_df.empty:
            latest = region_df.sort_values("hour_start").tail(1)
            rev = latest["total_revenue"].iloc[0]

            if rev < 50:
                st.error(f"❌ CRITICAL: Revenue extremely low (${rev:.2f})")
            elif rev < 150:
                st.warning(f"⚠️ Revenue lower than expected (${rev:.2f})")
            else:
                st.success(f"💰 Revenue healthy (${rev:.2f})")

    # ---------------- SHIPMENT ALERT ----------------
    if not shipments_df.empty:
        region_df = shipments_df[shipments_df["region"] == selected_region]
        if not region_df.empty:
            latest = region_df.sort_values("hour_start").tail(1)
            delayed = latest["in_transit_count"].iloc[0]

            if delayed > 50:
                st.error(f"❌ CRITICAL: {delayed} shipments stuck in transit")
            elif delayed > 20:
                st.warning(f"⚠️ {delayed} shipments delayed")
            else:
                st.success("📦 Shipments flowing normally")

    # ---------------- INVENTORY ALERT ----------------
    # inventory has NO hour_start → no sorting
    if not inventory_df.empty:
        region_df = inventory_df[inventory_df["region"] == selected_region]
        if not region_df.empty:
            # aggregate available qty
            available = region_df["total_available"].sum()

            if available < 100:
                st.error(f"❌ CRITICAL: Inventory dangerously low ({available})")
            elif available < 300:
                st.warning(f"⚠️ Inventory low ({available})")
            else:
                st.success("🏬 Inventory levels healthy")

# ---------------------------------------------------------
# PAGE: OVERVIEW
# ---------------------------------------------------------
if page == "🏠 Overview":
    st.title("📊 Global Commerce Intelligence — Overview")
    st.caption("Real-time KPIs across all regions")

    show_alerts()

    col1, col2, col3 = st.columns(3)

    # Revenue KPI
    if not revenue_df.empty:
        region_df = revenue_df[revenue_df["region"] == selected_region]
        col1.metric("Total Revenue", f"${region_df['total_revenue'].sum():,.2f}")
    else:
        col1.metric("Total Revenue", "No data")

    # Payment KPI
    if not payments_df.empty:
        region_df = payments_df[payments_df["region"] == selected_region]
        col2.metric("Payment Success Rate", f"{region_df['success_rate'].mean()*100:.2f}%")
    else:
        col2.metric("Payment Success Rate", "No data")

    # Shipment KPI
    if not shipments_df.empty:
        region_df = shipments_df[shipments_df["region"] == selected_region]
        col3.metric("Delivered Shipments", f"{region_df['delivered_count'].sum():,}")
    else:
        col3.metric("Delivered Shipments", "No data")

    st.subheader("📈 Revenue Trend")
    if not revenue_df.empty:
        region_df = revenue_df[revenue_df["region"] == selected_region]
        st.line_chart(region_df.set_index("hour_start")["total_revenue"])
    else:
        st.info("No revenue data available.")

# ---------------------------------------------------------
# PAGE: REVENUE
# ---------------------------------------------------------
elif page == "💰 Revenue":
    st.title("💰 Revenue Analytics")
    if not revenue_df.empty:
        region_df = revenue_df[revenue_df["region"] == selected_region]
        st.line_chart(region_df.set_index("hour_start")["total_revenue"])
        st.dataframe(region_df)
    else:
        st.info("No revenue data available.")

# ---------------------------------------------------------
# PAGE: PAYMENTS
# ---------------------------------------------------------
elif page == "💳 Payments":
    st.title("💳 Payment Success Rate")
    if not payments_df.empty:
        region_df = payments_df[payments_df["region"] == selected_region]
        st.line_chart(region_df.set_index("hour_start")["success_rate"])
        st.dataframe(region_df)
    else:
        st.info("No payment data available.")

# ---------------------------------------------------------
# PAGE: SHIPMENTS
# ---------------------------------------------------------
elif page == "📦 Shipments":
    st.title("📦 Shipment Performance")
    if not shipments_df.empty:
        region_df = shipments_df[shipments_df["region"] == selected_region]
        st.bar_chart(region_df.set_index("hour_start")[["delivered_count", "in_transit_count"]])
        st.dataframe(region_df)
    else:
        st.info("No shipment data available.")

# ---------------------------------------------------------
# PAGE: ENGAGEMENT
# ---------------------------------------------------------
elif page == "🛒 Engagement":
    st.title("🛒 Customer Engagement")
    if not engagement_df.empty:
        region_df = engagement_df[engagement_df["region"] == selected_region]
        st.bar_chart(region_df.set_index("hour_start")["event_count"])
        st.dataframe(region_df)
    else:
        st.info("No engagement data available.")

# ---------------------------------------------------------
# PAGE: INVENTORY
# ---------------------------------------------------------
elif page == "🏬 Inventory":
    st.title("🏬 Inventory Health")
    if not inventory_df.empty:
        region_df = inventory_df[inventory_df["region"] == selected_region]
        st.dataframe(region_df)
    else:
        st.info("No inventory data available.")