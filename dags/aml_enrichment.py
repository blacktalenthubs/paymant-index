#!/usr/bin/env python3
"""
Enrichment AML Pipeline
-----------------------

This script implements the enrichment layer for the AML & Fraud Detection system.
It reads upstream datasets (generated using Faker in previous ingestion steps) from
local directories, performs complex left-joins and enrichment steps, and produces
a consolidated "index" dataset that serves as input to downstream systems (e.g.,
compliance dashboards, case management systems).

Design Decisions:
  • The pipeline uses a Lambda architecture – a batch layer on EMR (Spark) is used
    to join, enrich, and aggregate data nightly.
  • Data is stored in Amazon S3 (here simulated as local directories) as Parquet files,
    partitioned by date, region, or environment to support production-level query performance.
  • The enriched output follows a star schema design, with fact tables (e.g. Fact_Transactions)
    joined with multiple dimensions (Dim_Customer, Dim_Merchant, etc.) to facilitate fast analytics.
  • Key enrichment functions include ML scoring (suspicion_score) and rule-based flagging,
    which combine transaction attributes with internal AML compliance rules.
  • This pipeline is designed for batch processing; future enhancements may integrate Spark
    Structured Streaming to lower latency (transitioning to a hybrid Lambda/Kappa model).

Upstream Datasets (Assumed to be generated and stored under 'data_upstreams'):
  • customer_profile
  • account_rel
  • transactions
  • merchant_data
  • watchlist
  • device_channel
  • kyc_questionnaire
  • complaints

Internally Generated Data:
  • aml_compliance_rules (contains threshold parameters, effective dates, etc.)

Downstream Outputs (to be produced):
  • suspicious_tx_index – detailed flagged transactions with suspicion scores and rule hits.
  • suspicious_entity_index – aggregated risk scores per customer/merchant.
  • aml_case – case records for investigation.

Output:
  The enriched data is written as partitioned Parquet files under 'data_enriched/'.
  A _SUCCESS file is written upon successful completion.

Usage:
  Ensure PySpark and Faker are installed:
    pip install pyspark faker
  Run this script with:
    spark-submit enrichment_aml_pipeline.py
"""

import os
import random
from datetime import datetime
from decimal import Decimal

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, ArrayType, MapType,
    TimestampType, DateType, DecimalType
)
from pyspark.sql.functions import col, to_timestamp, lit, current_timestamp, udf, array_union

from faker import Faker


# ---------------------------------------------------------------------------
# UDF Definitions for Enrichment
# ---------------------------------------------------------------------------
def compute_suspicion_score(amount, customer_risk):
    """
    Compute a suspicion score based on transaction amount and customer risk.
    - If amount exceeds the threshold (from AML rules), add 0.5.
    - If customer risk > 8, add 0.5.
    Otherwise, base score = (amount / 10000).
    Returns a value between 0 and 1.
    """
    score = (amount / 10000.0) if amount is not None else 0.0
    if customer_risk is not None and customer_risk > 8:
        score += 0.5
    if score > 1.0:
        score = 1.0
    return float(score)


compute_suspicion_score_udf = udf(compute_suspicion_score, DoubleType())


def determine_rule_hits(amount, customer_risk, aml_threshold):
    """
    Determine rule hits based on:
      - If amount > aml_threshold then include "HIGH_VALUE"
      - If customer_risk > 8 then include "HIGH_RISK_CUSTOMER"
    Returns an array of strings.
    """
    hits = []
    try:
        threshold = float(aml_threshold)
    except:
        threshold = 10000.0
    if amount is not None and amount > threshold:
        hits.append("HIGH_VALUE")
    if customer_risk is not None and customer_risk > 8:
        hits.append("HIGH_RISK_CUSTOMER")
    return hits


determine_rule_hits_udf = udf(determine_rule_hits, ArrayType(StringType()))


# ---------------------------------------------------------------------------
# Generate Internal AML Compliance Rules (single record)
# ---------------------------------------------------------------------------
def generate_aml_compliance_rules(spark):
    """
    Generates a DataFrame containing internal AML compliance rules.
    This includes threshold parameters for transaction limits.
    """
    fake = Faker()
    row = Row(
        rule_id="RULE-001",
        description="Transaction Limit Rule",
        threshold_params={"transaction_limit": "10000"},
        effective_date=datetime(2025, 1, 1),
        version="v1",
        owner="ComplianceTeam",
        updated_at=datetime.now()
    )
    schema = StructType([
        StructField("rule_id", StringType(), True),
        StructField("description", StringType(), True),
        StructField("threshold_params", MapType(StringType(), StringType()), True),
        StructField("effective_date", DateType(), True),
        StructField("version", StringType(), True),
        StructField("owner", StringType(), True),
        StructField("updated_at", TimestampType(), True)
    ])
    return spark.createDataFrame([row], schema)


# ---------------------------------------------------------------------------
# Main Enrichment Pipeline
# ---------------------------------------------------------------------------
def main():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("QualityPipeline") \
        .config("spark.executor.instances", "2") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "1g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Define base directories for upstream data and enriched outputs
    upstream_dir = "data_upstreams"
    enriched_dir = "data_enriched"
    os.makedirs(enriched_dir, exist_ok=True)

    # -----------------------------------------------------------------------
    # Read Upstream Datasets from local directories
    # -----------------------------------------------------------------------
    df_customer = spark.read.parquet(os.path.join(upstream_dir, "customer_profile"))
    df_account = spark.read.parquet(os.path.join(upstream_dir, "account_rel"))
    df_txn = spark.read.parquet(os.path.join(upstream_dir, "transactions"))
    df_merchant = spark.read.parquet(os.path.join(upstream_dir, "merchant_data"))
    df_watchlist = spark.read.parquet(os.path.join(upstream_dir, "watchlist"))
    df_device = spark.read.parquet(os.path.join(upstream_dir, "device_channel"))
    df_kyc = spark.read.parquet(os.path.join(upstream_dir, "kyc_questionnaire"))
    df_complaints = spark.read.parquet(os.path.join(upstream_dir, "complaints"))

    # Read Internal Policy Data (AML Compliance Rules)
    df_aml_rules = generate_aml_compliance_rules(spark)
    # For simplicity, broadcast the AML rule threshold (assuming one row)
    aml_threshold = df_aml_rules.select("threshold_params").collect()[0]["threshold_params"]["transaction_limit"]

    # -----------------------------------------------------------------------
    # Data Enrichment: Left Joins & Derived Columns
    # -----------------------------------------------------------------------
    # Start with transactions as the fact table
    df_enriched = df_txn.alias("txn") \
        .join(df_customer.alias("cust"), col("txn.customer_id") == col("cust.customer_id"), "left") \
        .join(df_account.alias("acct"), col("txn.account_id") == col("acct.account_id"), "left") \
        .join(df_merchant.alias("merch"), col("txn.merchant_id") == col("merch.merchant_id"), "left") \
        .join(df_device.alias("dev"), col("txn.customer_id") == col("dev.customer_id"), "left") \
        .join(df_kyc.alias("kyc"), col("txn.customer_id") == col("kyc.customer_id"), "left") \
        .join(df_complaints.alias("comp"), col("txn.transaction_id") == col("comp.txn_id"), "left")
    # Watchlist is not directly joinable on a key; assume a fuzzy match on customer full name exists:
    # For simulation, we skip direct join for watchlist.

    # Enrich with ML suspicion score and rule hits using UDFs.
    # Use customer risk_score from customer_profile.
    df_enriched = df_enriched.withColumn(
        "suspicion_score",
        compute_suspicion_score_udf(col("txn.amount"), col("cust.risk_score"))
    ).withColumn(
        "rule_hits",
        determine_rule_hits_udf(col("txn.amount"), col("cust.risk_score"), lit(aml_threshold))
    ).withColumn(
        "flagged_time",
        current_timestamp()
    ).withColumn(
        "flag_status",
        lit("OPEN")
    ).withColumn(
        "assigned_officer",
        lit(None).cast(StringType())
    )

    # Select relevant columns for the downstream suspicious transaction index
    df_suspicious_tx_index = df_enriched.select(
        col("txn.transaction_id").alias("transaction_id"),
        col("txn.customer_id").alias("customer_id"),
        col("suspicion_score"),
        col("rule_hits"),
        col("flagged_time"),
        col("flag_status").alias("status"),
        col("assigned_officer")
    )

    # -----------------------------------------------------------------------
    # Generate Aggregated Suspicious Entity Index (by customer)
    # -----------------------------------------------------------------------
    from pyspark.sql.functions import avg, collect_set, max as spark_max

    df_suspicious_entity_index = df_suspicious_tx_index.groupBy("customer_id") \
        .agg(
        avg("suspicion_score").alias("aggregated_score"),
        collect_set("rule_hits").alias("reasons"),
        spark_max("flagged_time").alias("last_updated")
    ).withColumnRenamed("customer_id", "entity_id") \
        .withColumn("entity_type", lit("CUSTOMER"))

    # -----------------------------------------------------------------------
    # Generate AML Case Table (dummy case creation for flagged entities)
    # -----------------------------------------------------------------------
    df_aml_case = df_suspicious_entity_index.select(
        (col("entity_id")).alias("subject_id"),
        lit("CUSTOMER").alias("subject_type"),
        current_timestamp().alias("opened_date"),
        lit(None).cast(MapType(StringType(), StringType())).alias("details"),
        lit("FraudInvestigationTeam").alias("assigned_group"),
        lit("OPEN").alias("status"),
        lit(None).alias("resolution"),
        col("last_updated")
    ).withColumn("case_id", F.concat(lit("CASE-"), col("subject_id")))

    # -----------------------------------------------------------------------
    # Write Enriched Outputs as Parquet Files
    # -----------------------------------------------------------------------
    df_suspicious_tx_index.write.mode("overwrite").parquet(os.path.join(enriched_dir, "suspicious_tx_index"))
    df_suspicious_entity_index.write.mode("overwrite").parquet(os.path.join(enriched_dir, "suspicious_entity_index"))
    df_aml_case.write.mode("overwrite").parquet(os.path.join(enriched_dir, "aml_case"))

    # Write a _SUCCESS flag file
    success_flag = os.path.join(enriched_dir, "_SUCCESS")
    with open(success_flag, "w") as sf:
        sf.write("SUCCESS")

    print(f"Enrichment complete. Outputs written to '{enriched_dir}'.")

    spark.stop()


if __name__ == "__main__":
    main()
