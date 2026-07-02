import dlt
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit

# ── Configuration ──────────────────────────────────────────────────────────────
S3_RAW_PATH = "s3://india-warehouse-digital-twin/raw/warehouse_events/"

# ── Bronze Layer ───────────────────────────────────────────────────────────────
# Ingests raw CSV data from S3 into Delta Lake as-is with metadata columns.
# No transformations — full fidelity of source data is preserved.

@dlt.table(
    name="bronze_warehouse_events",
    comment="Raw warehouse operational events ingested from S3. No transformations applied.",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true",
    }
)
def bronze_warehouse_events():
    return (
        spark.readStream
            .format("cloudFiles")                      # Auto Loader for incremental S3 ingestion
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation", "dbfs:/checkpoints/bronze/schema")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("header", "true")
            .load(S3_RAW_PATH)
            .withColumn("_ingested_at", current_timestamp())       # audit column
            .withColumn("_source_file", input_file_name())         # traceability
            .withColumn("_pipeline_layer", lit("bronze"))
    )