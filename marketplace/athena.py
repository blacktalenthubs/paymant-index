
athena_queries = """
CREATE EXTERNAL TABLE IF NOT EXISTS products (
    product_id STRING,
    name STRING
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION 's3://mentor-hub-networks-dev/risk_control_project/product/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS vendors (
    vendor_id STRING,
    name STRING
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION 's3://mentor-hub-networks-dev/risk_control_project/vendor/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS marketplaces (
    market_id STRING,
    name STRING
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION 's3://mentor-hub-networks-dev/risk_control_project/marketplace/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS calendar (
    date DATE,
    weekday STRING,
    is_weekend BOOLEAN
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
LOCATION 's3://mentor-hub-networks-dev/risk_control_project/calendar/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

CREATE EXTERNAL TABLE IF NOT EXISTS orders_fact (
    order_id STRING,
    product_id STRING,
    vendor_id STRING,
    market_id STRING,
    order_quantity INT,
    order_amount DOUBLE
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
LOCATION 's3://mentor-hub-networks-dev/risk_control_project/orders/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');
"""
#print(athena_queries)
