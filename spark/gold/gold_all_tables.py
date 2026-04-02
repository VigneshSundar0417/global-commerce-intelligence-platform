import importlib
from spark.utils.spark_session import get_spark

spark = get_spark("gold_all_tables")

# ---------------------------------------------------
# Helper to run any Gold module with a main() function
# ---------------------------------------------------
def run_gold_job(module_name: str):
    print(f"▶ Running Gold job: {module_name}")
    module = importlib.import_module(f"spark.gold.{module_name}")
    if hasattr(module, "main"):
        module.main()
    else:
        # Fallback: run entire script on import
        print(f"⚠ {module_name} has no main() — executing on import.")
    print(f"✔ Completed: {module_name}\n")


# ---------------------------------------------------
# List of Gold jobs in correct dependency order
# ---------------------------------------------------
GOLD_JOBS = [
    "orders_gold_daily",
    "payments_gold_daily",
    "shipments_gold_sla",
    "customer_engagement_gold",
    "inventory_health_gold",
    "order_fulfillment_gold",
    "revenue_gold_hourly",
    "conversion_funnel_gold",
    "inventory_replenishment_gold",
    "customer_retention_gold",
    "business_kpis_gold"
]

# ---------------------------------------------------
# Execute all Gold jobs
# ---------------------------------------------------
def main():
    print("\n==============================")
    print("   STARTING GOLD PIPELINE")
    print("==============================\n")

    for job in GOLD_JOBS:
        run_gold_job(job)

    print("==============================")
    print("   GOLD PIPELINE COMPLETED")
    print("==============================\n")


if __name__ == "__main__":
    main()