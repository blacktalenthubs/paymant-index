#!/usr/bin/env python3
"""
Indexing Pipeline using Apache Spark.

Production Issues Addressed:
  - Joining a large enriched transactions table with several small dimension tables (users, merchants, cards)
    can lead to heavy shuffling.
  - Broadcasting the small dimension tables minimizes shuffling and improves join performance.
  - The ml_signals table is joined normally as it may be large.
  - Data is partitioned by transaction_date for optimized querying.
Spark configurations are tuned for production performance.
A _SUCCESS flag is written after processing to avoid duplicate work.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, broadcast
from pyspark.sql.types import StringType

def compute_risk_flag(amount, fraud_score):
    if amount is None or fraud_score is None:
        return "UNKNOWN"
    if fraud_score > 0.7 or amount > 800:
        return "HIGH"
    elif fraud_score > 0.4 or amount > 500:
        return "MEDIUM"
    else:
        return "LOW"

compute_risk_flag_udf = udf(compute_risk_flag, StringType())

def index_data():
    spark = SparkSession.builder \
        .appName("IndexingPipeline") \
        .config("spark.executor.instances", "4") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    # Define directories (relative to /opt/airflow)
    enriched_dir = 'data/enriched'
    ingestion_dir = 'data/ingestion'
    indexed_dir = 'data/indexed'
    os.makedirs(indexed_dir, exist_ok=True)
    success_flag = os.path.join(indexed_dir, "_SUCCESS")
    if os.path.exists(success_flag):
        print("Indexed data already processed. Skipping.")
        spark.stop()
        return

    # Read enriched transactions and dimension tables
    enriched_transactions = spark.read.parquet(os.path.join(enriched_dir, 'enriched_transactions.parquet'))
    users = spark.read.parquet(os.path.join(ingestion_dir, 'users.parquet'))
    merchants = spark.read.parquet(os.path.join(ingestion_dir, 'merchants.parquet'))
    cards = spark.read.parquet(os.path.join(ingestion_dir, 'cards.parquet'))
    ml_signals = spark.read.parquet(os.path.join(ingestion_dir, 'ml_signals.parquet'))

    # Broadcast the small dimension tables to reduce shuffling // todo read more about data shuffling and data skew
    final_index = enriched_transactions.join(broadcast(users), on="user_id", how="left") \
        .join(broadcast(merchants), on="merchant_id", how="left") \
        .join(broadcast(cards), on="card_id", how="left") \
        .join(ml_signals, on="transaction_id", how="left")

    final_index = final_index.withColumn("risk_flag", compute_risk_flag_udf(col("amount"), col("fraud_score")))

    # Write the final index partitioned by transaction_date
    final_index.write.mode("overwrite").partitionBy("transaction_date").parquet(
        os.path.join(indexed_dir, 'final_index.parquet'))

    with open(success_flag, 'w') as f:
        f.write("SUCCESS")
    print("Indexing complete. Success flag written.")
    spark.stop()

if __name__ == "__main__":
    index_data()
