import dlt
from pyspark.sql.functions import (
    col, trim, upper, lower, when, current_timestamp, lit,
    regexp_replace, to_date, expr, round
)

# ── Excel serial date converter ────────────────────────────────────────────────
# Excel epoch starts 1899-12-30
def excel_to_date(col_name):
    return expr(f"date_add(to_date('1899-12-30'), CAST(`{col_name}` AS INT))")

# ── Silver: Customers ──────────────────────────────────────────────────────────
@dlt.table(
    name="silver_customers",
    comment="Cleaned and standardized customer data",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_customer_id", "Customer_ID IS NOT NULL")
@dlt.expect_or_drop("valid_city", "City IS NOT NULL")
@dlt.expect("valid_age", "Age BETWEEN 18 AND 100")
def silver_customers():
    return (
        dlt.read("bronze_customers")
            .dropDuplicates(["Customer_ID"])
            .withColumn("Registration_Date", excel_to_date("Registration_Date"))
            .withColumn("Date_of_Birth", excel_to_date("Date_of_Birth"))
            .withColumn("City", trim(col("City")))
            .withColumn("State", trim(col("State")))
            .withColumn("Gender", trim(col("Gender")))
            .withColumn("Membership_label", trim(col("Membership_label")))
            .withColumn(
                "Preferred_Device",
                when(col("Preferred_Device").contains("Mobile"), "Mobile App")
                .when(col("Preferred_Device").contains("Desktop"), "Desktop Site")
                .otherwise("Unknown")
            )
            .withColumn("_silver_processed_at", current_timestamp())
            .withColumn("_pipeline_layer", lit("silver"))
            .drop("_ingested_at")
    )


# ── Silver: Products ───────────────────────────────────────────────────────────
@dlt.table(
    name="silver_products",
    comment="Cleaned product catalog with stock risk flag",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_product_id", "Product_ID IS NOT NULL")
@dlt.expect_or_drop("valid_selling_price", "Selling_Price > 0")
@dlt.expect("valid_stock", "Stock_Quantity >= 0")
def silver_products():
    return (
        dlt.read("bronze_products")
            .dropDuplicates(["Product_ID"])
            .withColumnRenamed("Stock_Quantity (nos.)", "Stock_Quantity")
            .withColumnRenamed("Profit Margin", "Profit_Margin")
            .withColumn("Category", trim(col("Category")))
            .withColumn("Brand", trim(col("Brand")))
            .withColumn("Product_Status", trim(col("Product_Status")))
            .withColumn("Launch_Date", excel_to_date("Launch_Date"))
            .withColumn(
                "stock_risk_flag",
                when(col("Stock_Quantity") <= col("Reorder_Level"), "AT RISK")
                .otherwise("HEALTHY")
            )
            .withColumn("_silver_processed_at", current_timestamp())
            .withColumn("_pipeline_layer", lit("silver"))
            .drop("_ingested_at")
    )


# ── Silver: Orders ─────────────────────────────────────────────────────────────
@dlt.table(
    name="silver_orders",
    comment="Cleaned orders with converted dates and amounts",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_order_id", "Order_ID IS NOT NULL")
@dlt.expect_or_drop("valid_customer_id", "Customer_ID IS NOT NULL")
@dlt.expect_or_drop("valid_total_amount", "Total_Amount > 0")
@dlt.expect("valid_quantity", "Quantity > 0")
def silver_orders():
    return (
        dlt.read("bronze_orders")
            .dropDuplicates(["Order_ID"])
            .withColumnRenamed("Quantity (nos.)", "Quantity")
            .withColumn("Order_Date", excel_to_date("Order_Date"))
            .withColumn("Order_Status", trim(col("Order_Status")))
            .withColumn("Payment_Mode", trim(col("Payment_Mode")))
            .withColumn("Total_Amount", col("Total_Amount").cast("double"))
            .withColumn("Unit_Price", col("Unit_Price").cast("double"))
            .withColumn("Discount_Amount", col("Discount_Amount").cast("double"))
            .withColumn("Tax_Amount", col("Tax_Amount").cast("double"))
            .withColumn("Shipping_Charge", col("Shipping_Charge").cast("double"))
            .withColumn("_silver_processed_at", current_timestamp())
            .withColumn("_pipeline_layer", lit("silver"))
            .drop("_ingested_at")
    )


# ── Silver: Payments ───────────────────────────────────────────────────────────
@dlt.table(
    name="silver_payments",
    comment="Cleaned payments with currency symbols removed and empty columns dropped",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("valid_payment_id", "Payment_ID IS NOT NULL")
@dlt.expect_or_drop("valid_order_id", "Order_ID IS NOT NULL")
@dlt.expect("valid_payment_status", "Payment_Status IN ('Success', 'Failed')")
def silver_payments():
    return (
        dlt.read("bronze_payments")
            .dropDuplicates(["Payment_ID"])
            # Drop empty columns
            .drop("_c9", "_c10", "_c11", "_c12")
            # Clean currency columns
            .withColumn(
                "Transaction_Fee",
                regexp_replace(
                    regexp_replace(col("Transaction_Fee"), "₹", ""),
                    ",", ""
                ).cast("double")
            )
            .withColumn(
                "Refund_Amount",
                regexp_replace(
                    regexp_replace(col("Refund_Amount"), "₹", ""),
                    ",", ""
                ).cast("double")
            )
            .withColumn("Payment_Status", trim(col("Payment_Status")))
            .withColumn("Payment_Mode", trim(col("Payment_Mode")))
            .withColumn(
                "Refund_Date",
                when(col("Refund_Date") == "NA", None)
                .otherwise(to_date(col("Refund_Date"), "dd-MM-yyyy"))
            )
            .withColumn(
                "Payment_Date",
                to_date(col("Payment_Date"), "dd-MM-yyyy")
            )
            .withColumn("_silver_processed_at", current_timestamp())
            .withColumn("_pipeline_layer", lit("silver"))
            .drop("_ingested_at")
    )