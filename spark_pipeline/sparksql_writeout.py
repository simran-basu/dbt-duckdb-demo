from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower

spark = SparkSession.builder \
    .appName("Week2SparkSQLWriteOut") \
    .getOrCreate()

# --- Read raw sources ---
customers_raw = spark.read.csv("../seeds/customers.csv", header=True, inferSchema=True)
orders_raw = spark.read.csv("../seeds/orders.csv", header=True, inferSchema=True)

# --- Staging transformations (from Tuesday) ---
stg_customers = customers_raw.select(
    col("customer_id"),
    col("name").alias("customer_name"),
    col("signup_date"),
    lower(col("region")).alias("region")
)

stg_orders = orders_raw.select(
    col("order_id"),
    col("customer_id"),
    col("order_date"),
    col("order_amount"),
    lower(col("STATUS")).alias("order_status"),
    col("last_updated")
)

# --- Register as temp views so we can use Spark SQL directly ---
stg_customers.createOrReplaceTempView("stg_customers")
stg_orders.createOrReplaceTempView("stg_orders")

# --- Reimplement int_customer_alerts.sql logic as Spark SQL ---
int_customer_alerts = spark.sql("""
    WITH order_signals AS (
        SELECT
            customer_id,
            COUNT(*) AS total_orders,
            SUM(order_amount) AS total_spend,
            SUM(CASE WHEN order_status = 'pending' THEN 1 ELSE 0 END) AS pending_orders,
            SUM(CASE WHEN order_status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_orders,
            MAX(order_date) AS last_order_date
        FROM stg_orders
        GROUP BY customer_id
    )
    SELECT
        c.customer_id,
        c.customer_name,
        c.region,
        COALESCE(o.total_orders, 0) AS total_orders,
        COALESCE(o.total_spend, 0) AS total_spend,
        COALESCE(o.pending_orders, 0) AS pending_orders,
        COALESCE(o.cancelled_orders, 0) AS cancelled_orders,
        o.last_order_date,
        CASE
            WHEN COALESCE(o.total_spend, 0) > 400 THEN 'high_value_customer'
            WHEN COALESCE(o.pending_orders, 0) > 0 THEN 'pending_order_followup'
            WHEN COALESCE(o.cancelled_orders, 0) > 0 THEN 'cancellation_review'
            ELSE 'no_alert'
        END AS alert_reason,
        CASE
            WHEN COALESCE(o.total_spend, 0) > 400 THEN 1
            WHEN COALESCE(o.pending_orders, 0) > 0 THEN 2
            WHEN COALESCE(o.cancelled_orders, 0) > 0 THEN 3
            ELSE 4
        END AS alert_priority
    FROM stg_customers c
    LEFT JOIN order_signals o ON c.customer_id = o.customer_id
""")

print("=== int_customer_alerts (Spark SQL) ===")
int_customer_alerts.orderBy("customer_id").show()

# --- Write final output to Parquet, in a location DuckDB can read ---
output_path = "./output/int_customer_alerts_parquet"
int_customer_alerts.write.mode("overwrite").parquet(output_path)

print(f"Written to: {output_path}")