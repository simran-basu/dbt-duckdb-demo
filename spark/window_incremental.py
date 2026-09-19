from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, row_number, lag, lead
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("Week2WindowFunctions") \
    .getOrCreate()

orders_raw = spark.read.csv("../seeds/orders.csv", header=True, inferSchema=True)

stg_orders = orders_raw.select(
    col("order_id"),
    col("customer_id"),
    col("order_date"),
    col("order_amount"),
    lower(col("STATUS")).alias("order_status"),
    col("last_updated")
)

# --- Reimplementing "keep only the latest row per order_id" ---
# In dbt this was: delete+insert incremental strategy with unique_key='order_id',
# keeping whichever row has the max last_updated per order_id.

window_spec = Window.partitionBy("order_id").orderBy(col("last_updated").desc())

deduped = stg_orders.withColumn("row_num", row_number().over(window_spec)) \
                     .filter(col("row_num") == 1) \
                     .drop("row_num")

print("=== Deduped: latest row per order_id (mirrors incremental delete+insert) ===")
deduped.orderBy("order_id").show()

# --- SCD Type 2 style: track status changes per customer over time using lag/lead ---
# Partition by customer_id, order by last_updated, and compare each order's status
# to the customer's previous order status — flags a "change" the same way SCD2
# detects a new version of a record.

customer_window = Window.partitionBy("customer_id").orderBy("last_updated")

scd_style = stg_orders.withColumn("prev_status", lag("order_status").over(customer_window)) \
                       .withColumn("next_status", lead("order_status").over(customer_window)) \
                       .withColumn(
                           "status_changed",
                           (col("prev_status").isNotNull()) & (col("prev_status") != col("order_status"))
                       )

print("=== SCD2-style: status change detection per customer using lag/lead ===")
scd_style.orderBy("customer_id", "last_updated").show()