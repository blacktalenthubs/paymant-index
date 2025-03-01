import random
import uuid
from faker import Faker
from datetime import datetime, timedelta

from pyspark.python.pyspark.shell import spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, current_timestamp
from starlette.routing import request_response
import requests


class UpstreamsDataProducer:
    def __init__(self,spark):
        self.fake = Faker()
        self.spark = spark

    def generate_users_data(self, num_of_users):

        users = []

        for i in range(num_of_users):
            user_id = str(uuid.uuid4())
            name = self.fake.name()
            email = self.fake.email()
            country = self.fake.country()
            dob = self.fake.date_of_birth(minimum_age=18, maximum_age=100)
            user = {
                "user_id": user_id,
                "name": name,
                "email": email,
                "country": country,
                "dob": dob
            }
            users.append(user)

        return users

    def generate_merchants(self, num_merchants):
        merchants = []
        for _ in range(num_merchants):
            merchant_id = str(uuid.uuid4())
            merchant_name = self.fake.company()
            category = random.choice(["Retail", "Food & Beverage", "Entertainment", "Healthcare", "Technology"])
            location = self.fake.city()
            merchants.append({
                "merchant_id": merchant_id,
                "merchant_name": merchant_name,
                "category": category,
                "location": location
            })
        return merchants

    def generate_devices(self, num_devices):
        devices = []
        for _ in range(num_devices):
            device_id = str(uuid.uuid4())
            device_type = random.choice(["Mobile", "Laptop", "Desktop", "Tablet"])
            os = random.choice(["Android", "iOS", "Windows", "MacOS", "Linux"])
            ip_address = self.fake.ipv4()
            devices.append({
                "device_id": device_id,
                "device_type": device_type,
                "os": os,
                "ip_address": ip_address
            })
        return devices


    def generate_transactions(self,number_of_transactions, users, merchants, devices):
        transactions = []
        for _ in range(number_of_transactions):
            transaction_id = str(uuid.uuid4())

            user = random.choice(users) if random.random() < 0.9 else None
            merchant = random.choice(merchants) if random.random() < 0.8 else None
            device = random.choice(devices) if random.random() < 0.9 else None

            amount = round(random.uniform(1, 1000), 2)
            currency = random.choice(["USD", "EUR", "GBP", "INR"])
            transaction_date = datetime.now() - timedelta(days=random.randint(1, 365))
            transaction_type = random.choice(["purchase", "refund", "chargeback"])


            transaction = {
                "transaction_id": transaction_id,
                "user_id": user["user_id"] if user else None,
                "merchant_id": merchant["merchant_id"] if merchant else None,

                "device_id": device["device_id"] if device else None,
                "amount": amount,
                "currency": currency,
                "transaction_time": transaction_date,
                "transaction_type": transaction_type
            }

            transactions.append(transaction)

        return transactions

    def generate_exchange_rates(self, currencies, num_days):
        exchange_rates = []
        today = datetime.today()
        for currency in currencies:
            for i in range(num_days):
                rate_to_usd = round(random.uniform(0.5, 2.0), 2)  # Simulate exchange rate
                date = today - timedelta(days=i)
                exchange_rates.append({
                    "currency": currency,
                    "rate_to_usd": rate_to_usd,
                    "date": date.date()
                })
        return exchange_rates

    def generate_geolocation(self, ip_addresses, num_entries):
        geolocations = []
        for ip_address in ip_addresses:
            country = self.fake.country()
            region = self.fake.state()
            city = self.fake.city()
            latitude = random.uniform(-90.0, 90.0)
            longitude = random.uniform(-180.0, 180.0)
            geolocations.append({
                "ip_address": ip_address,
                "country": country,
                "region": region,
                "city": city,
                "latitude": latitude,
                "longitude": longitude
            })
        return geolocations


    def join_upstreams_data(self,Nusers, Nmerchants, Ndevices, Ntransactions, Nexchange_rates, Ngeolocations):
        users = self.generate_users_data(Nusers)
        merchants = self.generate_merchants(Nmerchants)
        devices = self.generate_devices(Ndevices)
        transactions = self.generate_transactions(Ntransactions, Nusers, merchants, devices)
        geolocations = self.generate_geolocation(Ngeolocations, Ntransactions)
        exchange_rates = self.generate_exchange_rates(Nexchange_rates, Ntransactions)
        pass

    def get_api_data(self):
        response = requests.get("https://api.example.com/users")
        print(response.status_code)
        print(response.status_code)






# testing
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("MySparkApp") \
        .config("spark.executor.memory", "10g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "50") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
        .config("fs.s3a.endpoint", "s3.amazonaws.com") \
        .getOrCreate()



    upstreams_data_producer = UpstreamsDataProducer(spark)
    users = upstreams_data_producer.generate_users_data(10)
    merchants = upstreams_data_producer.generate_merchants(5)
    devices = upstreams_data_producer.generate_devices(5)
    transactions = upstreams_data_producer.generate_transactions(10, users, merchants, devices)
    exchange_rates = upstreams_data_producer.generate_exchange_rates(["USD", "EUR", "GBP", "INR"], 10)
    geolocations = upstreams_data_producer.generate_geolocation([device["ip_address"] for device in devices], 5)
    print(users)

    # can we use spark instead of pandas  to convert these data to dataframes
    users_df = spark.createDataFrame(users)
    users_df.show()
    trans_df = spark.createDataFrame(transactions)

    users_df = users_df.withColumn("timestamp", current_timestamp())
    trans_df = trans_df.withColumn("timestamp", current_timestamp())
    users_df.printSchema()
    trans_df.printSchema()
    users_df = users_df.withColumn("dt", date_format("timestamp", "yyyy-MM-dd"))
    trans_df = trans_df.withColumn("dt", date_format("timestamp", "yyyy-MM-dd"))

    users_df.coalesce(1).write.partitionBy("dt").mode("overwrite").parquet(
        "s3a://mentor-hub-networks-dev/risk_control_project/users")
    trans_df.coalesce(1).write.partitionBy("dt").mode("overwrite").parquet(
        "s3a://mentor-hub-networks-dev/risk_control_project/transactions")

    trans_df = spark.createDataFrame(transactions)
    join_user_transactions_df = users_df.join(trans_df,on='user_id',how='left')
    join_user_transactions_df.show()
    print(join_user_transactions_df.count())

    trans_df.write \
        .partitionBy("transaction_date") \
        .mode("overwrite") \
        .parquet("data/transactions")

    data = UpstreamsDataProducer(spark)
    users =  data.generate_users_data(100000)
    user_dataframe = spark.createDataFrame(users)
    user_dataframe.show()

#spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.1 your_script.py






