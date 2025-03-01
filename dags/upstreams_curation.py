#!/usr/bin/env python3
"""
Ingestion Pipeline using Pandas.
This script generates synthetic data for users, merchants, locations, cards,
transactions, and ML signals using Faker.
It writes the data as Parquet files into the directory 'data/ingestion' and
writes a _SUCCESS flag file when complete.
"""

import os
import random
from datetime import datetime, timedelta
import pandas as pd
from faker import Faker


def generate_ingestion_data():
    # Directory where files will be written
    ingestion_dir = "data/ingestion"
    os.makedirs(ingestion_dir, exist_ok=True)
    success_flag = os.path.join(ingestion_dir, "_SUCCESS")

    num_users = 400
    num_merchants = 50
    num_locations = 20
    num_cards = 100
    num_transactions = 200
    num_ml_signals = num_transactions

    fake = Faker()

    users = pd.DataFrame([{
        "user_id": i,
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": fake.email()
    } for i in range(1, num_users + 1)])

    merchants = pd.DataFrame([{
        "merchant_id": i,
        "merchant_name": fake.company(),
        "merchant_category": fake.bs()
    } for i in range(1, num_merchants + 1)])

    locations = pd.DataFrame([{
        "location_id": i,
        "country": fake.country(),
        "city": fake.city()
    } for i in range(1, num_locations + 1)])

    cards = pd.DataFrame([{
        "card_id": i,
        "masked_card_number": f"XXXX-XXXX-XXXX-{random.randint(1000, 9999)}"
    } for i in range(1, num_cards + 1)])

    start_date = datetime.now() - timedelta(days=30)
    transactions = pd.DataFrame([{
        "transaction_id": i,
        "user_id": random.randint(1, num_users),
        "merchant_id": random.randint(1, num_merchants),
        "location_id": random.randint(1, num_locations),
        "card_id": random.randint(1, num_cards),
        "amount": round(random.uniform(10.0, 1000.0), 2),
        "transaction_date": (start_date + timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%d')
    } for i in range(1, num_transactions + 1)])

    ml_signals = pd.DataFrame([{
        "ml_signal_id": i,
        "transaction_id": i,
        "fraud_score": round(random.uniform(0, 1), 4),
        "created_date": (start_date + timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%d')
    } for i in range(1, num_ml_signals + 1)])

    # Write data as Parquet files
    users.to_parquet(os.path.join(ingestion_dir, "users.parquet"), index=False)
    merchants.to_parquet(os.path.join(ingestion_dir, "merchants.parquet"), index=False)
    locations.to_parquet(os.path.join(ingestion_dir, "locations.parquet"), index=False)
    cards.to_parquet(os.path.join(ingestion_dir, "cards.parquet"), index=False)

    # For transactions and ml_signals, group by date (simulate partitioning)
    for date, group in transactions.groupby("transaction_date"):
        group.to_parquet(os.path.join(ingestion_dir, f"transactions_{date}.parquet"), index=False)
    for date, group in ml_signals.groupby("created_date"):
        group.to_parquet(os.path.join(ingestion_dir, f"ml_signals_{date}.parquet"), index=False)

    # Write a success flag
    with open(success_flag, "w") as f:
        f.write("SUCCESS")

    print("Ingestion complete and success flag written.")
