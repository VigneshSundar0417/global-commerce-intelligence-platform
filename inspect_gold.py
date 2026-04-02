import duckdb

BASE = "/home/vicky/global-commerce-intelligence-platform/data/gold"

tables = {
    "revenue": "revenue",
    "payment_success_rate": "payment_success_rate",
    "shipment_performance": "shipment_performance",
    "inventory_health": "inventory_health",
    "customer_engagement": "customer_engagement"
}

con = duckdb.connect()

print("\n================ GOLD TABLE SCHEMA CHECK ================\n")

for label, table in tables.items():
    path = f"{BASE}/{table}"
    print(f"\n🔶 TABLE: {label}")
    print(f"📁 Path: {path}")

    try:
        df = con.execute(f"SELECT * FROM delta_scan('{path}') LIMIT 5").df()
        print("\n📌 First 5 rows:")
        print(df)

        print("\n📌 Columns:")
        print(df.columns.tolist())

    except Exception as e:
        print(f"❌ ERROR reading table {label}: {e}")

print("\n=========================================================\n")