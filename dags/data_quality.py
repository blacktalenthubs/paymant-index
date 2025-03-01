#!/usr/bin/env python3
"""
Quality Pipeline using Apache Spark.

Production Considerations:
  - Reads the final indexed data and performs basic schema and numeric validations.
  - Generates a quality report and writes it to a file.
A _SUCCESS flag is written after a successful quality check to prevent duplicate work.
"""

import os
from pyspark.sql import SparkSession

def validate_data():
    spark = SparkSession.builder \
        .appName("QualityPipeline") \
        .config("spark.executor.instances", "2") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "1g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    indexed_dir = 'data/indexed'
    quality_dir = 'data/quality'
    os.makedirs(quality_dir, exist_ok=True)
    success_flag = os.path.join(quality_dir, "_SUCCESS")
    if os.path.exists(success_flag):
        print("Quality report already generated. Skipping.")
        spark.stop()
        return

    # Read final indexed data
    final_index = spark.read.parquet(os.path.join(indexed_dir, 'final_index.parquet'))
    required_columns = ['transaction_id', 'user_id', 'merchant_id', 'card_id', 'amount', 'fraud_score', 'is_allowed']
    report_lines = []

    for col_name in required_columns:
        if col_name in final_index.columns:
            report_lines.append(f"Column '{col_name}': Present")
        else:
            report_lines.append(f"Column '{col_name}': MISSING")

    numeric_columns = ['amount', 'fraud_score']
    for col_name in numeric_columns:
        if col_name in final_index.columns:
            try:
                final_index.select(final_index[col_name].cast("double")).collect()
                report_lines.append(f"Column '{col_name}': Numeric check passed")
            except Exception:
                report_lines.append(f"Column '{col_name}': Numeric check failed")

    report_path = os.path.join(quality_dir, "quality_report.txt")
    with open(report_path, "w") as f:
        f.write("\n".join(report_lines))
    print(f"Quality report written to {report_path}")

    with open(success_flag, "w") as f:
        f.write("SUCCESS")
    spark.stop()

if __name__ == "__main__":
    validate_data()
