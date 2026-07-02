import dlt
from pyspark.sql.functions import (
    col, round, when, current_timestamp, lit, avg, count
)

# ── Gold Layer ─────────────────────────────────────────────────────────────────
# Aggregates silver data per warehouse and computes operational health score.

@dlt.table(
    name="gold_warehouse_health",
    comment="Operational health score per warehouse. Aggregated from silver layer.",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    }
)
def gold_warehouse_health():
    silver = dlt.read("silver_warehouse_events")

    return (
        silver
            .groupBy(
                "warehouse_id",
                "warehouse_city",
                "warehouse_state"
            )
            .agg(
                avg("fulfillment_rate").alias("avg_fulfillment_rate"),
                avg("warehouse_capacity_percentage").alias("avg_capacity_pct"),
                avg("fulfillment_time_hours").alias("avg_fulfillment_time_hrs"),
                avg(
                    when(col("inventory_status") == "HEALTHY", 1.0)
                    .when(col("inventory_status") == "MEDIUM", 0.5)
                    .otherwise(0.0)
                ).alias("inventory_health_score"),
                count("*").alias("total_records")
            )

            # ── Operational Health Score Formula ───────────────────────────────
            # Score = (fulfillment_rate * 0.4)
            #       + (inventory_health * 0.3)
            #       + ((1 - capacity_pct/100) * 0.3)
            # Range: 0.0 to 1.0
            .withColumn(
                "operational_health_score",
                round(
                    (col("avg_fulfillment_rate") * 0.4)
                    + (col("inventory_health_score") * 0.3)
                    + ((1 - col("avg_capacity_pct") / 100) * 0.3),
                    4
                )
            )

            # ── Risk Flagging ──────────────────────────────────────────────────
            .withColumn(
                "warehouse_status",
                when(col("operational_health_score") < 0.5, "AT RISK")
                .when(col("operational_health_score") < 0.75, "NEEDS ATTENTION")
                .otherwise("HEALTHY")
            )

            .withColumn("_gold_processed_at", current_timestamp())
            .withColumn("_pipeline_layer", lit("gold"))
    )