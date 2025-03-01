#!/usr/bin/env python3
"""
Enrichment Pipeline using Pandas.

This script reads ingestion data from 'data/ingestion', joins transactions with locations,
adds an "is_allowed" flag (based on restricted countries) and a normalized country name,
and writes the enriched data as a Parquet file to 'data/enriched/enriched_transactions.parquet'.
It then writes a success flag (_SUCCESS) in the same folder.
"""

import os
import pandas as pd


def normalize_country(country):
    # Return the country name in uppercase without leading/trailing spaces.
    return country.strip().upper() if isinstance(country, str) and country.strip() else None


def enrich_data():
    ingestion_dir = 'data/ingestion'
    enriched_dir = 'data/enriched'
    os.makedirs(enriched_dir, exist_ok=True)
    success_flag = os.path.join(enriched_dir, "_SUCCESS")

    # Define file paths for the required ingestion data.
    transactions_path = os.path.join(ingestion_dir, "transactions.parquet")
    locations_path = os.path.join(ingestion_dir, "locations.parquet")

    if not os.path.exists(transactions_path) or not os.path.exists(locations_path):
        print("Required ingestion files are missing. Exiting enrichment.")
        return

    # Read the ingestion data
    transactions = pd.read_parquet(transactions_path)
    locations = pd.read_parquet(locations_path)

    # Join transactions with locations on 'location_id'
    enriched = pd.merge(transactions, locations, on="location_id", how="left")

    # Apply enrichment: flag transactions and normalize country names.
    restricted_countries = ['North Korea', 'Iran']
    enriched["is_allowed"] = ~enriched["country"].isin(restricted_countries)
    enriched["norm_country"] = enriched["country"].apply(normalize_country)

    # Write the enriched data as a single Parquet file.
    output_file = os.path.join(enriched_dir, "enriched_transactions.parquet")
    enriched.to_parquet(output_file, index=False)

    # Write a success flag.
    with open(success_flag, "w") as f:
        f.write("SUCCESS")

    print(f"Enrichment complete. Enriched data written to {output_file} and flag written at {success_flag}.")
