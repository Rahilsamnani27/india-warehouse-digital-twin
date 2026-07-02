import dlt
from pyspark.sql.functions import (
    col, round, sum, avg, count, when, lit,
    current_timestamp, desc, countDistinct
)

# ── Gold: Revenue by Category and City ────────────────────────────────────────
@dlt.table(
    name="gold_revenue_by_category_city",
    comment="Total revenue, orders and avg order value by product category and city",
    table_properties={"quality": "gold"}
)
def gold_revenue_by_category_city():
    orders = dlt.read("silver_orders")
    products = dlt.read("silver_products")

    return (
        orders.join(products, on="Product_ID", how="left")
            .groupBy("Category", "City")
            .agg(
                round(sum("Total_Amount"), 2).alias("total_revenue"),
                count("Order_ID").alias("total_orders"),
                round(avg("Total_Amount"), 2).alias("avg_order_value"),
                round(sum("Discount_Amount"), 2).alias("total_discount_given"),
                round(sum("Tax_Amount"), 2).alias("total_tax_collected")
            )
            .withColumn("_gold_processed_at", current_timestamp())
            .withColumn("_pipeline_layer", lit("gold"))
    )


# ── Gold: Customer Performance ────────────────────────────────────────────────
@dlt.table(
    name="gold_customer_performance",
    comment="Top customers by total spend, orders and loyalty points",
    table_properties={"quality": "gold"}
)
def gold_customer_performance():
    orders = dlt.read("silver_orders")
    customers = dlt.read("silver_customers")

    return (
        orders.join(customers, on="Customer_ID", how="left")
            .groupBy(
                "Customer_ID", "Name", "City",
                "State", "Membership_label", "Loyalty_Points"
            )
            .agg(
                round(sum("Total_Amount"), 2).alias("total_spend"),
                count("Order_ID").alias("total_orders"),
                round(avg("Total_Amount"), 2).alias("avg_order_value"),
                sum(when(col("Order_Status") == "Returned", 1).otherwise(0)).alias("total_returns"),
                sum(when(col("Order_Status") == "Cancelled", 1).otherwise(0)).alias("total_cancellations")
            )
            .withColumn(
                "customer_segment",
                when(col("total_spend") > 500000, "HIGH VALUE")
                .when(col("total_spend") > 100000, "MID VALUE")
                .otherwise("LOW VALUE")
            )
            .withColumn("_gold_processed_at", current_timestamp())
            .withColumn("_pipeline_layer", lit("gold"))
    )


# ── Gold: Payment Analytics ───────────────────────────────────────────────────
@dlt.table(
    name="gold_payment_analytics",
    comment="Payment success/failure rates and refund analysis by payment mode",
    table_properties={"quality": "gold"}
)
def gold_payment_analytics():
    payments = dlt.read("silver_payments")
    orders = dlt.read("silver_orders")

    return (
        payments.join(orders, on="Order_ID", how="left")
            .groupBy("Payment_Mode")
            .agg(
                count("Payment_ID").alias("total_transactions"),
                sum(when(col("Payment_Status") == "Success", 1).otherwise(0)).alias("successful_payments"),
                sum(when(col("Payment_Status") == "Failed", 1).otherwise(0)).alias("failed_payments"),
                round(sum("Refund_Amount"), 2).alias("total_refunds"),
                round(sum("Transaction_Fee"), 2).alias("total_transaction_fees")
            )
            .withColumn(
                "failure_rate_pct",
                round((col("failed_payments") / col("total_transactions")) * 100, 2)
            )
            .withColumn("_gold_processed_at", current_timestamp())
            .withColumn("_pipeline_layer", lit("gold"))
    )


# ── Gold: Stock Risk Analysis ─────────────────────────────────────────────────
@dlt.table(
    name="gold_stock_risk",
    comment="Products at risk of stock-out based on reorder level",
    table_properties={"quality": "gold"}
)
def gold_stock_risk():
    products = dlt.read("silver_products")
    orders = dlt.read("silver_orders")

    return (
        products.join(
            orders.groupBy("Product_ID")
                .agg(sum("Quantity").alias("total_units_sold")),
            on="Product_ID", how="left"
        )
        .withColumn(
            "days_of_stock_remaining",
            when(col("total_units_sold") > 0,
                round(col("Stock_Quantity") / (col("total_units_sold") / 365), 0))
            .otherwise(None)
        )
        .withColumn(
            "stock_status",
            when(col("Stock_Quantity") <= col("Reorder_Level"), "CRITICAL")
            .when(col("Stock_Quantity") <= col("Reorder_Level") * 1.5, "LOW")
            .otherwise("HEALTHY")
        )
        .select(
            "Product_ID", "Product_Name", "Category",
            "Brand", "Stock_Quantity", "Reorder_Level",
            "total_units_sold", "days_of_stock_remaining",
            "stock_status", "Profit_Margin"
        )
        .withColumn("_gold_processed_at", current_timestamp())
        .withColumn("_pipeline_layer", lit("gold"))
    )