# ── Unity Catalog Setup ────────────────────────────────────────────────────────
# Run this notebook once in Databricks to set up catalog, schemas and register
# table metadata and lineage for the India Warehouse Digital Twin pipeline.
# ──────────────────────────────────────────────────────────────────────────────

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# ── 1. Create Catalog ──────────────────────────────────────────────────────────
spark.sql("CREATE CATALOG IF NOT EXISTS warehouse_catalog")
spark.sql("USE CATALOG warehouse_catalog")

# ── 2. Create Schemas per layer ───────────────────────────────────────────────
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze COMMENT 'Raw ingested data from S3'")
spark.sql("CREATE SCHEMA IF NOT EXISTS silver COMMENT 'Cleaned and validated data'")
spark.sql("CREATE SCHEMA IF NOT EXISTS gold   COMMENT 'Aggregated health scores'")

# ── 3. Register table comments and metadata ───────────────────────────────────
spark.sql("""
    ALTER TABLE warehouse_catalog.bronze.bronze_warehouse_events
    SET TBLPROPERTIES (
        'owner'       = 'rahil',
        'team'        = 'data-engineering',
        'source'      = 's3://india-warehouse-digital-twin/raw/',
        'layer'       = 'bronze',
        'description' = 'Raw warehouse events ingested via Auto Loader from S3'
    )
""")

spark.sql("""
    ALTER TABLE warehouse_catalog.silver.silver_warehouse_events
    SET TBLPROPERTIES (
        'owner'       = 'rahil',
        'team'        = 'data-engineering',
        'layer'       = 'silver',
        'description' = 'Cleaned warehouse events with DLT quality expectations applied'
    )
""")

spark.sql("""
    ALTER TABLE warehouse_catalog.gold.gold_warehouse_health
    SET TBLPROPERTIES (
        'owner'       = 'rahil',
        'team'        = 'data-engineering',
        'layer'       = 'gold',
        'description' = 'Operational health score per warehouse computed from silver layer'
    )
""")

# ── 4. Grant read access ───────────────────────────────────────────────────────
spark.sql("GRANT USE CATALOG ON CATALOG warehouse_catalog TO `account users`")
spark.sql("GRANT USE SCHEMA ON SCHEMA warehouse_catalog.gold TO `account users`")
spark.sql("GRANT SELECT ON TABLE warehouse_catalog.gold.gold_warehouse_health TO `account users`")

# ── 5. Verify lineage ──────────────────────────────────────────────────────────
print("Catalog created: warehouse_catalog")
print("Schemas: bronze, silver, gold")
print("Tables registered with metadata.")
print("Lineage visible in Databricks UI → Data → warehouse_catalog")