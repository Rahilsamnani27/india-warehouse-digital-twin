import dlt
from pyspark.sql.functions import (
    col, trim, upper, when, current_timestamp, lit, to_timestamp
)

# ── Silver Layer ───────────────────────────────────────────────────────────────
# Reads from bronze, applies data quality expectations, cleans and standardizes.

@dlt.table(
    name="silver_warehouse_events",
    comment="Cleaned and validated warehouse events. Nulls removed, duplicates dropped, schema standardized.",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
    }
)
@dlt.expect_or_drop("valid_warehouse_id", "warehouse_id IS NOT NULL")
@dlt.expect_or_drop("valid_inventory_level", "inventory_level IS NOT NULL")
@dlt.expect_or_drop("valid_capacity_range", "warehouse_capacity_percentage BETWEEN 0 AND 100")
@dlt.expect_or_drop("valid_orders_received", "orders_received > 0")
@dlt.expect("no_negative_pending", "orders_pending >= 0")
@dlt.expect("valid_fulfillment_time", "fulfillment_time_hours BETWEEN 0.5 AND 120")
def silver_warehouse_events():
    return (
        dlt.read_stream("bronze_warehouse_events")

            # ── Deduplication ──────────────────────────────────────────────────
            .dropDuplicates(["warehouse_id", "product_id", "timestamp"])

            # ── Type casting ───────────────────────────────────────────────────
            .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
            .withColumn("inventory_level", col("inventory_level").cast("integer"))
            .withColumn("reorder_threshold", col("reorder_threshold").cast("integer"))
            .withColumn("orders_received", col("orders_received").cast("integer"))
            .withColumn("orders_fulfilled", col("orders_fulfilled").cast("integer"))
            .withColumn("orders_pending", col("orders_pending").cast("integer"))
            .withColumn("fulfillment_time_hours", col("fulfillment_time_hours").cast("double"))
            .withColumn("warehouse_capacity_percentage", col("warehouse_capacity_percentage").cast("double"))

            # ── Standardization ────────────────────────────────────────────────
            .withColumn("warehouse_id", trim(upper(col("warehouse_id"))))
            .withColumn("warehouse_city", trim(col("warehouse_city")))
            .withColumn("warehouse_state", trim(col("warehouse_state")))
            .withColumn("product_category", trim(col("product_category")))

            # ── Derived columns ────────────────────────────────────────────────
            .withColumn(
                "fulfillment_rate",
                (col("orders_fulfilled") / col("orders_received")).cast("double")
            )
            .withColumn(
                "inventory_status",
                when(col("inventory_level") < col("reorder_threshold"), "LOW")
                .when(col("inventory_level") < col("reorder_threshold") * 2, "MEDIUM")
                .otherwise("HEALTHY")
            )

            # ── Audit columns ──────────────────────────────────────────────────
            .withColumn("_silver_processed_at", current_timestamp())
            .withColumn("_pipeline_layer", lit("silver"))

            # ── Drop bronze metadata columns ───────────────────────────────────
            .drop("_source_file")
    )