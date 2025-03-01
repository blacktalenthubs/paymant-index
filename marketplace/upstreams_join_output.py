
from pyspark.sql import SparkSession

from faker import Faker
fake = Faker()

s3_bucket = "s3a://mentor-hub-networks-dev/risk_control_project/"

def get_spark_session():
    return SparkSession.builder \
        .appName("AirflowSparkPipeline") \
        .config("spark.executor.memory", "10g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "50") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
        .config("fs.s3a.access.key", "YOUR_ACCESS_KEY") \
        .config("fs.s3a.secret.key", "YOUR_SECRET_KEY") \
        .config("fs.s3a.endpoint", "s3.amazonaws.com") \
        .getOrCreate()


def enrich_data():
    spark = get_spark_session()

    products_df = spark.read.parquet(s3_bucket + "product/")
    products_df.printSchema()
    vendors_df = spark.read.parquet(s3_bucket + "vendor/")
    vendors_df.printSchema()
    marketplaces_df = spark.read.parquet(s3_bucket + "marketplace/")
    marketplaces_df.printSchema()
    calendar_df = spark.read.parquet(s3_bucket + "calendar/")
    calendar_df.printSchema()

    orders_fact_df = spark.read.parquet(s3_bucket + "orders/")
    orders_fact_df.printSchema()




    enriched_df = orders_fact_df \
        .join(products_df, "product_id", "left") \
        .join(vendors_df, "vendor_id", "left") \
        .join(marketplaces_df, "market_id", "left") \
        .join(
            calendar_df,
            (orders_fact_df["year"] == calendar_df["year"]) &
            (orders_fact_df["month"] == calendar_df["month"]) &
            (orders_fact_df["day"] == calendar_df["day"]),
            "left"
        ) \
        .select(
            orders_fact_df["order_id"],
            orders_fact_df["product_id"],
            products_df["name"].alias("product_name"),
            products_df["category"],
            orders_fact_df["vendor_id"],
            vendors_df["name"].alias("vendor_name"),
            vendors_df["region"],
            orders_fact_df["market_id"],
            marketplaces_df["name"].alias("market_name"),
            marketplaces_df["type"].alias("market_type"),
            orders_fact_df["order_date"],
            orders_fact_df["order_quantity"],
            orders_fact_df["order_amount"],
            calendar_df["date"].alias("calendar_date"),
            calendar_df["weekday"],
            calendar_df["is_weekend"]
        )

    # Save Enriched DataFrame to S3
    enriched_df.coalesce(1).write.mode("overwrite").parquet(s3_bucket + "enriched_data/")
