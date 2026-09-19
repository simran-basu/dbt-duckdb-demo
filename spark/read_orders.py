from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Week2SparkRead") \
    .getOrCreate()

# Point directly at the same raw CSVs used in the Week 1 dbt project
customers_path = "../seeds/customers.csv"
orders_path = "../seeds/orders.csv"

customers_df = spark.read.csv(customers_path, header=True, inferSchema=True)
orders_df = spark.read.csv(orders_path, header=True, inferSchema=True)

print("=== Customers ===")
customers_df.show()

print("=== Orders ===")
orders_df.show()

customers_df.printSchema()
orders_df.printSchema()