import dlt
from pyspark.sql.functions import current_timestamp, lit

DBFS_RAW_PATH = "/Volumes/workspace/default/raw_data/"

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
            .withColumnRenamed("Discount_%", "Discount_Pct")
            .withColumn("_ingested_at", current_timestamp())
            .withColumn("_pipeline_layer", lit("bronze"))
    )

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
            .withColumnRenamed("Quantity (nos.)", "Quantity")
            .withColumn("_ingested_at", current_timestamp())
            .withColumn("_pipeline_layer", lit("bronze"))
    )

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
            .withColumnRenamed(" Transaction_Fee ", "Transaction_Fee")
            .withColumnRenamed(" Refund_Amount ", "Refund_Amount")
            .drop("_c9", "_c10", "_c11", "_c12")
            .withColumn("_ingested_at", current_timestamp())
            .withColumn("_pipeline_layer", lit("bronze"))
    )