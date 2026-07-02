from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# ── Register Bronze Tables ─────────────────────────────────────────────────────
for table in ["bronze_customers", "bronze_products", "bronze_orders", "bronze_payments"]:
    spark.sql(f"""
        ALTER TABLE workspace.default.{table}
        SET TBLPROPERTIES (
            'layer' = 'bronze',
            'source' = '/Volumes/workspace/default/raw_data/',
            'team'  = 'data-engineering'
        )
    """)

# ── Register Silver Tables ─────────────────────────────────────────────────────
for table in ["silver_customers", "silver_products", "silver_orders", "silver_payments"]:
    spark.sql(f"""
        ALTER TABLE workspace.default.{table}
        SET TBLPROPERTIES (
            'layer' = 'silver',
            'team'  = 'data-engineering'
        )
    """)

# ── Register Gold Tables ───────────────────────────────────────────────────────
for table in [
    "gold_revenue_by_category_city",
    "gold_customer_performance",
    "gold_payment_analytics",
    "gold_stock_risk"
]:
    spark.sql(f"""
        ALTER TABLE workspace.default.{table}
        SET TBLPROPERTIES (
            'layer' = 'gold',
            'team'  = 'data-engineering'
        )
    """)

print("Unity Catalog metadata registered successfully.")
print("Lineage visible in Databricks UI → Catalog → workspace → default")