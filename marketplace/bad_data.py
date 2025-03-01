import random
import uuid
from faker import Faker
from datetime import datetime, timedelta
from pyspark.sql import SparkSession


class DataGenerator:

    def __init__(self, spark):
        self.fake = Faker()
        self.spark = spark

    def generate_users_with_issues(self, num_users):
        users = []
        emails = set()
        for _ in range(num_users):
            user_id = str(uuid.uuid4())
            name = self.fake.name()
            if random.random() < 0.1 and emails:
                email = random.choice(list(emails))
            else:
                email = self.fake.email() if random.random() > 0.05 else "invalid_email_format"
            emails.add(email)
            signup_date = self.fake.date_this_decade()
            country = self.fake.country()
            dob = self.fake.date_of_birth(minimum_age=18, maximum_age=70)

            users.append({
                "user_id": user_id,
                "name": name,
                "email": email,
                "signup_date": signup_date if random.random() > 0.05 else None,
                "country": country,
                "date_of_birth": dob
            })
        return users

    def generate_merchants_with_issues(self, num_merchants):
        merchants = []
        names = set()
        for _ in range(num_merchants):
            merchant_id = str(uuid.uuid4())
            merchant_name = self.fake.company()

            if random.random() < 0.15 and names:
                merchant_name = random.choice(list(names))
            names.add(merchant_name)

            category = random.choice(["Retail", "Food & Beverage", "Entertainment", "Healthcare", "Technology"])
            location = self.fake.city()

            merchants.append({
                "merchant_id": merchant_id,
                "merchant_name": merchant_name,
                "category": category,
                "location": location if random.random() > 0.1 else None
            })
        return merchants

    def generate_devices_with_issues(self, num_devices):
        devices = []
        ip_addresses = set()
        for _ in range(num_devices):
            device_id = str(uuid.uuid4())
            device_type = random.choice(["Mobile", "Laptop", "Desktop", "Tablet"])
            os = random.choice(["Android", "iOS", "Windows", "MacOS", "Linux"])

            if random.random() < 0.1 and ip_addresses:
                ip_address = random.choice(list(ip_addresses))
            else:
                ip_address = self.fake.ipv4()

            ip_addresses.add(ip_address)

            devices.append({
                "device_id": device_id,
                "device_type": device_type,
                "os": os if random.random() > 0.1 else None,
                "ip_address": ip_address
            })
        return devices

    def generate_transactions_with_issues(self, number_of_transactions, users, merchants, devices):
        transactions = []
        existing_transactions = set()
        for _ in range(number_of_transactions):
            transaction_id = str(uuid.uuid4())

            user = random.choice(users) if random.random() < 0.9 else None
            merchant = random.choice(merchants) if random.random() < 0.8 else None
            device = random.choice(devices) if random.random() < 0.9 else None

            amount = round(random.uniform(1, 1000), 2)
            if random.random() < 0.05:
                amount = round(random.uniform(10000, 100000), 2)

            currency = random.choice(["USD", "EUR", "GBP", "INR", "ABC"])
            transaction_date = datetime.now() - timedelta(days=random.randint(1, 365))
            transaction_type = random.choice(["purchase", "refund", "chargeback"])

            # Convert the user, merchant, and device to their IDs to make the combination hashable
            user_id = user["user_id"] if user else None
            merchant_id = merchant["merchant_id"] if merchant else None
            device_id = device["device_id"] if device else None

            # Create a unique combination of user_id, merchant_id, device_id, and transaction_date
            transaction_key = (user_id, merchant_id, device_id, transaction_date)

            if random.random() < 0.1 and transaction_key in existing_transactions:
                transaction_id = str(uuid.uuid4())  # New transaction ID for the duplicate
            else:
                existing_transactions.add(transaction_key)

            transactions.append({
                "transaction_id": transaction_id,
                "user_id": user_id,
                "merchant_id": merchant_id,
                "device_id": device_id,
                "amount": amount,
                "currency": currency if currency != "ABC" else None,
                "transaction_time": transaction_date,
                "transaction_type": transaction_type
            })

        return transactions

    def generate_exchange_rates_with_issues(self, currencies, num_days):
        exchange_rates = []
        seen_dates = set()
        today = datetime.today()
        for currency in currencies:
            for i in range(num_days):
                rate_to_usd = round(random.uniform(0.5, 2.0), 2)
                date = today - timedelta(days=i)

                if random.random() < 0.1 and date in seen_dates:
                    rate_to_usd = round(random.uniform(0.5, 2.0), 2)
                else:
                    seen_dates.add(date)

                if random.random() < 0.05:
                    rate_to_usd = 0

                exchange_rates.append({
                    "currency": currency,
                    "rate_to_usd": rate_to_usd,
                    "date": date.date()
                })
        return exchange_rates


if __name__ == '__main__':
    spark = SparkSession.builder.master("local").appName("DataGenerator").getOrCreate()
    data = DataGenerator(spark)
    num_users = 1000
    num_merchants = 100
    num_devices = 300
    num_transactions = 5000
    currencies = ["USD", "EUR", "GBP", "INR", "JPY"]
    num_days = 30

    users = data.generate_users_with_issues(num_users)
    merchants = data.generate_merchants_with_issues(num_merchants)
    devices = data.generate_devices_with_issues(num_devices)
    transactions = data.generate_transactions_with_issues(num_transactions, users, merchants, devices)
    exchange_rates = data.generate_exchange_rates_with_issues(currencies, num_days)
    users_df = spark.createDataFrame(users)
    merchants_df = spark.createDataFrame(merchants)
    devices_df = spark.createDataFrame(devices)
    transactions_df = spark.createDataFrame(transactions)
#    exchange_rates_df = spark.createDataFrame(exchange_rates)

    join_df = transactions_df.join(users_df, transactions_df.user_id == users_df.user_id, "inner")
    join_df.show()


