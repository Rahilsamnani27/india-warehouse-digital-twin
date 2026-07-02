# ── Unity Catalog Setup ────────────────────────────────────────────────────────
# Run this notebook once in Databricks to register all tables with
# metadata, lineage and access control.
# ──────────────────────────────────────────────────────────────────────────────

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# ── 1. Create Catalog ──────────────────────────────────────────────────────────
spark.sql("CREATE CATALOG IF NOT EXISTS ecommerce_catalog")
spark.sql("USE CATALOG ecommerce_catalog")

# ── 2. Create Schemas ──────────────────────────────────────────────────────────
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze COMMENT 'Raw ingested data from DBFS'")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver COMMENT 'Cleaned and validated data'")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold   COMMENT 'Aggregated analytical tables'")

# ── 3. Register Bronze Tables ──────────────────────────────────────────────────
for table in ["bronze_customers", "bronze_products", "bronze_orders", "bronze_payments"]:
    spark.sql(f"""
        ALTER TABLE ecommerce_catalog.bronze.{table}
        SET TBLPROPERTIES (
            'owner'       = 'rahil',
            'layer'       = 'bronze',
            'source'      = 'dbfs:/warehouse/raw/',
            'team'        = 'data-engineering'
        )
    """)

# ── 4. Register Silver Tables ──────────────────────────────────────────────────
for table in ["silver_customers", "silver_products", "silver_orders", "silver_payments"]:
    spark.sql(f"""
        ALTER TABLE ecommerce_catalog.silver.{table}
        SET TBLPROPERTIES (
            'owner'       = 'rahil',
            'layer'       = 'silver',
            'team'        = 'data-engineering'
        )
    """)

# ── 5. Register Gold Tables ────────────────────────────────────────────────────
for table in [
    "gold_revenue_by_category_city",
    "gold_customer_performance",
    "gold_payment_analytics",
    "gold_stock_risk"
]:
    spark.sql(f"""
        ALTER TABLE ecommerce_catalog.gold.{table}
        SET TBLPROPERTIES (
            'owner'       = 'rahil',
            'layer'       = 'gold',
            'team'        = 'data-engineering'
        )
    """)

# ── 6. Grant Access ────────────────────────────────────────────────────────────
spark.sql("GRANT USE CATALOG ON CATALOG ecommerce_catalog TO `account users`")
spark.sql("GRANT USE SCHEMA ON SCHEMA ecommerce_catalog.gold TO `account users`")
spark.sql("GRANT SELECT ON SCHEMA ecommerce_catalog.gold TO `account users`")

print("Unity Catalog setup complete.")
print("Catalog: ecommerce_catalog")
print("Schemas: bronze, silver, gold")
print("Lineage visible in Databricks UI → Catalog → ecommerce_catalog")