from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, current_date
import random
import uuid
from faker import Faker
from datetime import datetime as dt, timedelta as td

# Initialize Faker
fake = Faker()

# S3 Bucket
s3_bucket = "s3a://mentor-hub-networks-dev/risk_control_project/"

# Spark Session Initialization
def get_spark_session():
    return SparkSession.builder \
        .appName("AirflowSparkPipeline") \
        .config("spark.executor.memory", "10g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "50") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
        .config("fs.s3a.endpoint", "s3.amazonaws.com") \
        .getOrCreate()

# Generate Data Functions for Each Schema
def generate_products():
    spark = get_spark_session()
    num_products = 50
    products = [
        {"product_id": str(uuid.uuid4()),
         "name": fake.word().capitalize(),
         "category": random.choice(["Electronics", "Clothing", "Food", "Furniture", "Toys"])}
        for _ in range(num_products)
    ]
    spark.createDataFrame(products).withColumn("dt", current_date()) \
        .coalesce(1).write.mode("overwrite").parquet(s3_bucket + "product/")

def generate_vendors():
    spark = get_spark_session()
    num_vendors = 20
    vendors = [
        {"vendor_id": str(uuid.uuid4()),
         "name": fake.company(),
         "region": fake.city()}
        for _ in range(num_vendors)
    ]
    spark.createDataFrame(vendors).withColumn("dt", current_date()) \
        .coalesce(1).write.mode("overwrite").parquet(s3_bucket + "vendor/")

def generate_marketplaces():
    spark = get_spark_session()
    num_marketplaces = 10
    marketplaces = [
        {"market_id": str(uuid.uuid4()),
         "name": fake.company(),
         "type": random.choice(["Online", "Offline"])}
        for _ in range(num_marketplaces)
    ]
    spark.createDataFrame(marketplaces).withColumn("dt", current_date()) \
        .coalesce(1).write.mode("overwrite").parquet(s3_bucket + "marketplace/")

def generate_calendar():
    spark = get_spark_session()
    num_calendar_days = 365 * 3
    start_date = dt(2020, 1, 1)
    calendar = [
        {"date": start_date + td(days=i),
         "year": (start_date + td(days=i)).year,
         "month": (start_date + td(days=i)).month,
         "day": (start_date + td(days=i)).day,
         "weekday": (start_date + td(days=i)).strftime("%A"),
         "is_weekend": (start_date + td(days=i)).weekday() in [5, 6]}
        for i in range(num_calendar_days)
    ]
    spark.createDataFrame(calendar).withColumn("year", year("date")) \
        .withColumn("month", month("date")) \
        .withColumn("day", dayofmonth("date")) \
        .coalesce(1).write.mode("overwrite").parquet(s3_bucket + "calendar/")

def generate_orders():
    spark = get_spark_session()
    num_orders = 1000
    products = spark.read.parquet(s3_bucket + "product/").collect()
    vendors = spark.read.parquet(s3_bucket + "vendor/").collect()
    marketplaces = spark.read.parquet(s3_bucket + "marketplace/").collect()
    calendar = spark.read.parquet(s3_bucket + "calendar/").collect()

    orders = [
        {"order_id": str(uuid.uuid4()),
         "product_id": random.choice(products)["product_id"],
         "vendor_id": random.choice(vendors)["vendor_id"],
         "market_id": random.choice(marketplaces)["market_id"],
         "order_date": random.choice(calendar)["date"],
         "order_quantity": random.randint(1, 100),
         "order_amount": round(random.uniform(10, 100) * random.randint(1, 100), 2)}
        for _ in range(num_orders)
    ]
    spark.createDataFrame(orders).withColumn("year", year("order_date")) \
        .withColumn("month", month("order_date")) \
        .withColumn("day", dayofmonth("order_date")) \
        .coalesce(1).write.mode("overwrite").parquet(s3_bucket + "orders/")

# Enrich Data
def enrich_data():
    spark = get_spark_session()

    # Read Datasets from S3
    products_df = spark.read.parquet(s3_bucket + "product/")
    vendors_df = spark.read.parquet(s3_bucket + "vendor/")
    marketplaces_df = spark.read.parquet(s3_bucket + "marketplace/")
    calendar_df = spark.read.parquet(s3_bucket + "calendar/")
    orders_fact_df = spark.read.parquet(s3_bucket + "orders/")

    # Perform Enrichment
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

# DAG Definition
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "data_enrichment_pipeline",
    default_args=default_args,
    description="A DAG to enrich data from S3 and save enriched datasets",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 1),
    catchup=False,
)

# Define Tasks
generate_products_task = PythonOperator(
    task_id="generate_products",
    python_callable=generate_products,
    dag=dag,
)

generate_vendors_task = PythonOperator(
    task_id="generate_vendors",
    python_callable=generate_vendors,
    dag=dag,
)

generate_marketplaces_task = PythonOperator(
    task_id="generate_marketplaces",
    python_callable=generate_marketplaces,
    dag=dag,
)

generate_calendar_task = PythonOperator(
    task_id="generate_calendar",
    python_callable=generate_calendar,
    dag=dag,
)

generate_orders_task = PythonOperator(
    task_id="generate_orders",
    python_callable=generate_orders,
    dag=dag,
)

enrich_task = PythonOperator(
    task_id="enrich_data",
    python_callable=enrich_data,
    dag=dag,
)

# Task Dependencies
[generate_products_task, generate_vendors_task, generate_marketplaces_task, generate_calendar_task] >> generate_orders_task >> enrich_task
