#!/usr/bin/env python3
"""
Upstream Data Ingestion for AML & Fraud Detection Pipeline
-----------------------------------------------------------
This script uses PySpark and the Faker library to generate synthetic data for
upstream feeds and writes the data as partitioned Parquet files to a local directory.
It produces datasets for eight upstream feeds: customer_profile, account_rel,
transactions, merchant_data, watchlist, device_channel, kyc_questionnaire, and complaints.

Each function includes a docstring explaining its purpose and output schema.
Note: The 'account_rel' table uses a DecimalType(15,2) for the balance field.
We convert float values to Python's Decimal type to match the schema.

Output:
  Data is written to the 'data_upstreams' directory.
  A _SUCCESS file is created upon successful ingestion.

Usage:
  Ensure PySpark and Faker are installed:
    pip install pyspark faker
  Run the script:
    python generate_upstreams.py
"""

import os
import random
from datetime import datetime
from decimal import Decimal

from faker import Faker
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, ArrayType, MapType,
    TimestampType, DateType, DecimalType, IntegerType
)
from pyspark.sql.functions import col, to_timestamp


# -------------------------
# Data Generation Functions
# -------------------------

def generate_dates(num_days=3):
    """
    Generate a list of date strings for partitioning.
    Returns: List of date strings in 'YYYY-MM-DD' format.
    """
    base_date = datetime(2025, 1, 1)
    return [(base_date.replace(day=base_date.day + i)).strftime("%Y-%m-%d") for i in range(num_days)]


def generate_customer_profile(spark, rowcount=50):
    """
    Generate a DataFrame for the 'customer_profile' table.
    Partitioned by the 'dt' field.
    """
    fake = Faker()
    date_list = generate_dates()
    rows = []
    for _ in range(rowcount):
        customer_id = "CUST-" + str(fake.random_number(digits=5))
        dt_choice = random.choice(date_list)
        row = Row(
            customer_id=customer_id,
            full_name=fake.name(),
            date_of_birth=fake.date_of_birth(minimum_age=18, maximum_age=90),
            nationality=fake.country_code(),
            primary_address=Row(
                street=fake.street_address(),
                city=fake.city(),
                postal_code=fake.postcode(),
                country=fake.country()
            ),
            contact_info=Row(
                email=fake.email(),
                phone=fake.phone_number()
            ),
            kyc_status=random.choice(["VERIFIED", "PENDING", "REJECTED"]),
            risk_score=round(random.uniform(0, 10), 2),
            created_at=fake.date_time_this_year(),
            updated_at=fake.date_time_this_year(),
            dt=dt_choice
        )
        rows.append(row)

    schema = StructType([
        StructField("customer_id", StringType(), nullable=False),
        StructField("full_name", StringType(), nullable=True),
        StructField("date_of_birth", DateType(), nullable=True),
        StructField("nationality", StringType(), nullable=True),
        StructField("primary_address",
                    StructType([
                        StructField("street", StringType(), True),
                        StructField("city", StringType(), True),
                        StructField("postal_code", StringType(), True),
                        StructField("country", StringType(), True)
                    ]), nullable=True),
        StructField("contact_info",
                    StructType([
                        StructField("email", StringType(), True),
                        StructField("phone", StringType(), True)
                    ]), nullable=True),
        StructField("kyc_status", StringType(), nullable=True),
        StructField("risk_score", DoubleType(), nullable=True),
        StructField("created_at", TimestampType(), nullable=True),
        StructField("updated_at", TimestampType(), nullable=True),
        StructField("dt", StringType(), nullable=True)
    ])
    return spark.createDataFrame(rows, schema)


def generate_account_rel(spark, rowcount=70):
    """
    Generate a DataFrame for the 'account_rel' table.
    Partitioned by 'dt'. The balance field is stored as Decimal.
    """
    fake = Faker()
    date_list = generate_dates()
    rows = []
    for _ in range(rowcount):
        account_id = "ACCT-" + str(fake.random_number(digits=5))
        dt_choice = random.choice(date_list)
        # Convert float to Decimal using string conversion to ensure correct precision
        balance_val = Decimal(str(round(random.uniform(100, 100000), 2)))
        row = Row(
            account_id=account_id,
            customer_id="CUST-" + str(random.randint(10000, 99999)),
            account_type=random.choice(["CHECKING", "SAVINGS", "BROKERAGE"]),
            open_date=fake.date_between(start_date="-5y", end_date="today"),
            status=random.choice(["ACTIVE", "CLOSED", "FROZEN"]),
            balance=balance_val,
            currency_code=random.choice(["USD", "EUR", "GBP"]),
            branch_id="BR-" + str(fake.random_number(digits=3)),
            last_updated=fake.date_time_this_year(),
            dt=dt_choice
        )
        rows.append(row)

    schema = StructType([
        StructField("account_id", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=False),
        StructField("account_type", StringType(), nullable=True),
        StructField("open_date", DateType(), nullable=True),
        StructField("status", StringType(), nullable=True),
        StructField("balance", DecimalType(15, 2), nullable=True),
        StructField("currency_code", StringType(), nullable=True),
        StructField("branch_id", StringType(), nullable=True),
        StructField("last_updated", TimestampType(), nullable=True),
        StructField("dt", StringType(), nullable=True)
    ])
    return spark.createDataFrame(rows, schema)


def generate_merchant_data(spark, rowcount=30):
    """
    Generate a DataFrame for the 'merchant_data' table.
    Partitioned by 'region'.
    """
    fake = Faker()
    regions = ["US", "EU", "APAC"]
    rows = []
    for _ in range(rowcount):
        merchant_id = "MER-" + str(fake.random_number(digits=5))
        region_choice = random.choice(regions)
        row = Row(
            merchant_id=merchant_id,
            legal_name=fake.company(),
            categories=[fake.word() for _ in range(random.randint(1, 3))],
            risk_level=random.choice(["LOW", "MEDIUM", "HIGH"]),
            contact=Row(
                phone=fake.phone_number(),
                email=fake.company_email()
            ),
            address={"street": fake.street_address(), "city": fake.city()},
            created_at=fake.date_time_this_year(),
            updated_at=fake.date_time_this_year(),
            region=region_choice
        )
        rows.append(row)

    schema = StructType([
        StructField("merchant_id", StringType(), nullable=False),
        StructField("legal_name", StringType(), nullable=True),
        StructField("categories", ArrayType(StringType()), nullable=True),
        StructField("risk_level", StringType(), nullable=True),
        StructField("contact",
                    StructType([
                        StructField("phone", StringType(), True),
                        StructField("email", StringType(), True)
                    ]), nullable=True),
        StructField("address", MapType(StringType(), StringType()), nullable=True),
        StructField("created_at", TimestampType(), nullable=True),
        StructField("updated_at", TimestampType(), nullable=True),
        StructField("region", StringType(), nullable=True)
    ])
    return spark.createDataFrame(rows, schema)


def generate_transactions(spark, rowcount=200):
    """
    Generate a DataFrame for the 'transactions' table.
    Partitioned by 'dt' and 'region'.
    """
    fake = Faker()
    date_list = generate_dates()
    regions = ["US", "EU", "APAC"]
    rows = []
    for _ in range(rowcount):
        transaction_id = "TX-" + str(fake.random_number(digits=6))
        dt_choice = random.choice(date_list)
        region_choice = random.choice(regions)
        row = Row(
            transaction_id=transaction_id,
            account_id="ACCT-" + str(random.randint(10000, 99999)),
            customer_id="CUST-" + str(random.randint(10000, 99999)),
            merchant_id="MER-" + str(random.randint(10000, 99999)),
            txn_timestamp=fake.date_time_this_year(),
            amount=round(random.uniform(5, 5000), 2),
            currency_code=random.choice(["USD", "EUR", "GBP", "JPY"]),
            channel=random.choice(["ONLINE", "ATM", "BRANCH"]),
            geo_location=Row(
                lat=float(round(fake.latitude(), 5)),
                lon=float(round(fake.longitude(), 5))
            ),
            device_info={"ip": fake.ipv4(), "os": fake.user_agent()},
            status=random.choice(["APPROVED", "DECLINED", "PENDING"]),
            dt=dt_choice,
            region=region_choice
        )
        rows.append(row)

    schema = StructType([
        StructField("transaction_id", StringType(), nullable=False),
        StructField("account_id", StringType(), nullable=True),
        StructField("customer_id", StringType(), nullable=True),
        StructField("merchant_id", StringType(), nullable=True),
        StructField("txn_timestamp", TimestampType(), nullable=True),
        StructField("amount", DoubleType(), nullable=True),
        StructField("currency_code", StringType(), nullable=True),
        StructField("channel", StringType(), nullable=True),
        StructField("geo_location",
                    StructType([
                        StructField("lat", DoubleType(), True),
                        StructField("lon", DoubleType(), True)
                    ]), nullable=True),
        StructField("device_info", MapType(StringType(), StringType()), nullable=True),
        StructField("status", StringType(), nullable=True),
        StructField("dt", StringType(), nullable=True),
        StructField("region", StringType(), nullable=True)
    ])
    return spark.createDataFrame(rows, schema)


def generate_watchlist(spark, rowcount=20):
    """
    Generate a DataFrame for the 'watchlist' table.
    Partitioned by 'dt'.
    """
    fake = Faker()
    date_list = generate_dates()
    rows = []
    for _ in range(rowcount):
        dt_choice = random.choice(date_list)
        row = Row(
            entity_name=fake.name(),
            entity_type=random.choice(["INDIVIDUAL", "COMPANY"]),
            risk_category=random.choice(["OFAC", "PEP", "INTERPOL"]),
            listing_source=random.choice(["OFAC", "UN", "LocalRegulator"]),
            watchlist_date=fake.date_this_year(),
            notes=fake.sentence(nb_words=5),
            last_updated=fake.date_time_this_year(),
            dt=dt_choice
        )
        rows.append(row)

    schema = StructType([
        StructField("entity_name", StringType(), True),
        StructField("entity_type", StringType(), True),
        StructField("risk_category", StringType(), True),
        StructField("listing_source", StringType(), True),
        StructField("watchlist_date", DateType(), True),
        StructField("notes", StringType(), True),
        StructField("last_updated", TimestampType(), True),
        StructField("dt", StringType(), True)
    ])
    return spark.createDataFrame(rows, schema)


def generate_device_channel(spark, rowcount=30):
    """
    Generate a DataFrame for the 'device_channel' table.
    Partitioned by 'environment'.
    """
    fake = Faker()
    environments = ["DEV", "PROD"]
    rows = []
    for _ in range(rowcount):
        env_choice = random.choice(environments)
        row = Row(
            device_id="DEV-" + str(fake.random_number(digits=6)),
            customer_id="CUST-" + str(random.randint(10000, 99999)),
            device_type=random.choice(["MOBILE", "WEB", "POS"]),
            ip_address=fake.ipv4(),
            os_version=fake.user_agent(),
            login_time=fake.date_time_this_year(),
            location=Row(
                lat=float(round(fake.latitude(), 5)),
                lon=float(round(fake.longitude(), 5))
            ),
            suspicious_flags=[fake.word() for _ in range(random.randint(0, 2))],
            environment=env_choice
        )
        rows.append(row)

    schema = StructType([
        StructField("device_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("os_version", StringType(), True),
        StructField("login_time", TimestampType(), True),
        StructField("location",
                    StructType([
                        StructField("lat", DoubleType(), True),
                        StructField("lon", DoubleType(), True)
                    ]), True),
        StructField("suspicious_flags", ArrayType(StringType()), True),
        StructField("environment", StringType(), True)
    ])
    return spark.createDataFrame(rows, schema)


def generate_kyc_questionnaire(spark, rowcount=25):
    """
    Generate a DataFrame for the 'kyc_questionnaire' table.
    Partitioned by 'dt'.
    """
    fake = Faker()
    date_list = generate_dates()
    rows = []
    for _ in range(rowcount):
        dt_choice = random.choice(date_list)
        question_map = {"Q" + str(i): fake.sentence(nb_words=3) for i in range(1, 4)}
        row = Row(
            form_id="FORM-" + str(fake.random_number(digits=4)),
            customer_id="CUST-" + str(random.randint(10000, 99999)),
            question_responses=question_map,
            review_status=random.choice(["PENDING", "APPROVED", "REJECTED"]),
            last_reviewed_by=fake.name(),
            review_date=fake.date_this_year(),
            updated_at=fake.date_time_this_year(),
            dt=dt_choice
        )
        rows.append(row)

    schema = StructType([
        StructField("form_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("question_responses", MapType(StringType(), StringType()), True),
        StructField("review_status", StringType(), True),
        StructField("last_reviewed_by", StringType(), True),
        StructField("review_date", DateType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("dt", StringType(), True)
    ])
    return spark.createDataFrame(rows, schema)


def generate_complaints(spark, rowcount=20):
    """
    Generate a DataFrame for the 'complaints' table.
    Partitioned by 'dt'.
    """
    fake = Faker()
    date_list = generate_dates()
    rows = []
    for _ in range(rowcount):
        dt_choice = random.choice(date_list)
        row = Row(
            complaint_id="CMP-" + str(fake.random_number(digits=5)),
            customer_id="CUST-" + str(random.randint(10000, 99999)),
            account_id="ACCT-" + str(random.randint(10000, 99999)),
            txn_id="TX-" + str(random.randint(100000, 999999)) if random.random() > 0.3 else None,
            opened_date=fake.date_time_this_year(),
            complaint_type=random.choice(["FRAUD_DISPUTE", "SERVICE_ISSUE"]),
            status=random.choice(["OPEN", "RESOLVED", "ESCALATED"]),
            resolution_notes=[fake.sentence(nb_words=5) for _ in range(random.randint(0, 2))],
            last_updated=fake.date_time_this_year(),
            dt=dt_choice
        )
        rows.append(row)

    schema = StructType([
        StructField("complaint_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("txn_id", StringType(), True),
        StructField("opened_date", TimestampType(), True),
        StructField("complaint_type", StringType(), True),
        StructField("status", StringType(), True),
        StructField("resolution_notes", ArrayType(StringType()), True),
        StructField("last_updated", TimestampType(), True),
        StructField("dt", StringType(), True)
    ])
    return spark.createDataFrame(rows, schema)


# -------------------------
# Main Function: Generate and Write Data
# -------------------------

def main():
    """
    Main function:
      1. Initializes a SparkSession.
      2. Generates upstream DataFrames for all eight feeds.
      3. Writes each DataFrame as partitioned Parquet files to the local directory 'data_upstreams'.
      4. Writes a _SUCCESS flag file upon completion.
    """
    from pyspark.sql import SparkSession

    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .appName("QualityPipeline") \
        .config("spark.executor.instances", "2") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "1g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    base_dir = "data_upstreams"
    os.makedirs(base_dir, exist_ok=True)

    # Generate upstream data
    df_customer = generate_customer_profile(spark, 50)
    df_account = generate_account_rel(spark, 70)
    df_merchant = generate_merchant_data(spark, 30)
    df_txn = generate_transactions(spark, 200)
    df_watchlist = generate_watchlist(spark, 20)
    df_device = generate_device_channel(spark, 30)
    df_kyc = generate_kyc_questionnaire(spark, 25)
    df_complaints = generate_complaints(spark, 20)

    # Write each dataset as partitioned Parquet files
    df_customer.write.partitionBy("dt").mode("overwrite") \
        .parquet(os.path.join(base_dir, "customer_profile"))
    df_account.write.partitionBy("dt").mode("overwrite") \
        .parquet(os.path.join(base_dir, "account_rel"))
    df_merchant.write.partitionBy("region").mode("overwrite") \
        .parquet(os.path.join(base_dir, "merchant_data"))
    df_txn.write.partitionBy("dt", "region").mode("overwrite") \
        .parquet(os.path.join(base_dir, "transactions"))
    df_watchlist.write.partitionBy("dt").mode("overwrite") \
        .parquet(os.path.join(base_dir, "watchlist"))
    df_device.write.partitionBy("environment").mode("overwrite") \
        .parquet(os.path.join(base_dir, "device_channel"))
    df_kyc.write.partitionBy("dt").mode("overwrite") \
        .parquet(os.path.join(base_dir, "kyc_questionnaire"))
    df_complaints.write.partitionBy("dt").mode("overwrite") \
        .parquet(os.path.join(base_dir, "complaints"))

    # Write a _SUCCESS flag file
    success_flag = os.path.join(base_dir, "_SUCCESS")
    with open(success_flag, "w") as f:
        f.write("SUCCESS")

    print(f"Data generation complete. Parquet files are stored in '{base_dir}'.")

    spark.stop()


if __name__ == "__main__":
    main()
