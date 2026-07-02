from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# Verify all tables are registered in Unity Catalog
tables = [
    "bronze_customers", "bronze_products", "bronze_orders", "bronze_payments",
    "silver_customers", "silver_products", "silver_orders", "silver_payments",
    "gold_revenue_by_category_city", "gold_customer_performance",
    "gold_payment_analytics", "gold_stock_risk"
]

for table in tables:
    count = spark.sql(f"SELECT COUNT(*) FROM workspace.default.{table}").collect()[0][0]
    print(f"{table}: {count} rows")

print("\nAll tables registered in Unity Catalog.")
print("Lineage visible in Databricks UI → Catalog → workspace → default")