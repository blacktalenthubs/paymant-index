from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, to_date, current_date
import random
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

# S3 bucket for output
s3_bucket = "inputs/"  #local for now

# Initialize Spark session
spark = SparkSession.builder \
    .appName("MySparkApp") \
    .config("spark.executor.memory", "10g") \
    .config("spark.driver.memory", "10") \
    .config("spark.sql.shuffle.partitions", "50") \
    .getOrCreate()



def generate_products(num_records):
    return [
        {"product_id": i + 1,
         "name": fake.word().capitalize(),
         "category": random.choice(["Electronics", "Clothing", "Food", "Furniture", "Toys"])}
        for i in range(num_records)
    ]


def generate_vendors(num_records):
    return [
        {"vendor_id": i + 1,
         "name": fake.company(),
         "region": fake.city()}
        for i in range(num_records)
    ]


def generate_marketplaces(num_records):
    return [
        {"market_id": i + 1,
         "name": fake.company(),
         "type": random.choice(["Online", "Offline"])}
        for i in range(num_records)
    ]


def generate_calendar(num_records):
    start_date = datetime(2020, 1, 1)
    return [
        {"date": start_date + timedelta(days=i),
         "year": (start_date + timedelta(days=i)).year,
         "month": (start_date + timedelta(days=i)).month,
         "day": (start_date + timedelta(days=i)).day,
         "weekday": (start_date + timedelta(days=i)).strftime("%A"),
         "is_weekend": (start_date + timedelta(days=i)).weekday() in [5, 6]}
        for i in range(num_records)
    ]


def generate_orders_fact(num_records, products, vendors, marketplaces, calendar):
    return [
        {"order_id": i + 1,
         "product_id": random.choice(products)["product_id"],
         "vendor_id": random.choice(vendors)["vendor_id"],
         "market_id": random.choice(marketplaces)["market_id"],
         "order_date": random.choice(calendar)["date"],
         "order_quantity": random.randint(1, 100),
         "order_amount": round(random.uniform(10, 100) * random.randint(1, 100), 2)}
        for i in range(num_records)
    ]


def enrichment_output(product_df, vendor_df, marketplace_df, orders_df, location):
    enriched_output = orders_df \
        .join(product_df, "product_id", "left") \
        .join(vendor_df, "vendor_id", "left") \
        .join(marketplace_df, "market_id", "left") \
        .select(
            orders_df["order_id"],
            orders_df["product_id"],
            product_df["name"].alias("product_name"),
            product_df["category"],
            vendor_df["vendor_id"],
            vendor_df["name"].alias("vendor_name"),
            vendor_df["region"],
            orders_df["market_id"],
            marketplace_df["name"].alias("market_name"),
            marketplace_df["type"].alias("market_type"),
            orders_df["order_date"],
            orders_df["order_quantity"],
            orders_df["order_amount"]
        )
    (enriched_output
        .write
        .mode("overwrite")
        .option("header", "true")
        .csv(location))
    return enriched_output



def ingest_data():
    # Generate data
    num_products = 50
    num_vendors = 20
    num_marketplaces = 10
    num_calendar_days = 365 * 3
    num_orders = 1000

    products = generate_products(num_products)
    vendors = generate_vendors(num_vendors)
    marketplaces = generate_marketplaces(num_marketplaces)
    calendar = generate_calendar(num_calendar_days)
    orders_fact = generate_orders_fact(num_orders, products, vendors, marketplaces, calendar)

    # Convert datasets to DataFrames
    products_df = spark.createDataFrame(products).withColumn("dt", current_date())
    vendors_df = spark.createDataFrame(vendors).withColumn("dt", current_date())
    marketplaces_df = spark.createDataFrame(marketplaces).withColumn("dt", current_date())
    orders_fact_df = spark.createDataFrame(orders_fact) \
        .withColumn("year", year("order_date")) \
        .withColumn("month", month("order_date"))

    # Write data to S3 with partitioning
    products_df.coalesce(1).write.partitionBy("dt").mode("overwrite").parquet(s3_bucket + "product/")
    vendors_df.coalesce(1).write.partitionBy("dt").mode("overwrite").parquet(s3_bucket + "vendor/")
    marketplaces_df.coalesce(1).write.partitionBy("dt").mode("overwrite").parquet(s3_bucket + "marketplace/")
    orders_fact_df.coalesce(1).write.partitionBy("year", "month").mode("overwrite").parquet(s3_bucket + "order_fact/")

    # Create and write enriched output
    enriched_df = enrichment_output(products_df, vendors_df, marketplaces_df, orders_fact_df, s3_bucket + "enriched/")
    enriched_df.show()
    print(f"Enriched record count: {enriched_df.count()}")



if __name__ == "__main__":
    ingest_data()
    spark.stop()
