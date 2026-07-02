import dlt
from pyspark.sql.functions import current_timestamp, lit, input_file_name

# ── Configuration ──────────────────────────────────────────────────────────────
DBFS_RAW_PATH = "/Volumes/workspace/default/raw_data/"

# ── Bronze: Customers ──────────────────────────────────────────────────────────
@dlt.table(
    name="bronze_customers",
    comment="Raw customers data ingested from DBFS",
    table_properties={"quality": "bronze"}
)
def bronze_customers():
    return (
        spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(f"{DBFS_RAW_PATH}Customers.csv")
            .withColumn("_ingested_at", current_timestamp())
            .withColumn("_pipeline_layer", lit("bronze"))
    )

# ── Bronze: Products ───────────────────────────────────────────────────────────
@dlt.table(
    name="bronze_products",
    comment="Raw products data ingested from DBFS",
    table_properties={"quality": "bronze"}
)
def bronze_products():
    return (
        spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(f"{DBFS_RAW_PATH}Products.csv")
            .withColumnRenamed("Stock_Quantity (nos.)", "Stock_Quantity")
            .withColumnRenamed("Profit Margin", "Profit_Margin")
            .withColumn("_ingested_at", current_timestamp())
            .withColumn("_pipeline_layer", lit("bronze"))
    )

# ── Bronze: Orders ─────────────────────────────────────────────────────────────
@dlt.table(
    name="bronze_orders",
    comment="Raw orders data ingested from DBFS",
    table_properties={"quality": "bronze"}
)
def bronze_orders():
    return (
        spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(f"{DBFS_RAW_PATH}Orders.csv")
            .withColumn("_ingested_at", current_timestamp())
            .withColumn("_pipeline_layer", lit("bronze"))
    )

# ── Bronze: Payments ───────────────────────────────────────────────────────────
@dlt.table(
    name="bronze_payments",
    comment="Raw payments data ingested from DBFS",
    table_properties={"quality": "bronze"}
)
def bronze_payments():
    return (
        spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(f"{DBFS_RAW_PATH}Payments.csv")
            .withColumn("_ingested_at", current_timestamp())
            .withColumn("_pipeline_layer", lit("bronze"))
    )