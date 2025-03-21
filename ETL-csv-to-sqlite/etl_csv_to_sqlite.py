#!/usr/bin/env python3

import pandas as pd
import sqlite3
import argparse
import os
from datetime import datetime

def extract_csv(file_path):
    try:
        df = pd.read_csv(file_path)
        print(f"[INFO] Extracted {len(df)} rows from '{file_path}'")
        return df
    except Exception as e:
        raise Exception(f"Failed to read CSV: {e}")

def transform_data(df):
    print("[INFO] Starting data transformation...")

    # Drop rows with any nulls
    df = df.dropna()

    # Normalize column names
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

    # Capitalize name fields if present
    if 'name' in df.columns:
        df['name'] = df['name'].apply(lambda x: str(x).title())

    # Standardize date format
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df = df.dropna(subset=['date'])
        df['date'] = df['date'].dt.strftime('%Y-%m-%d')

    print(f"[INFO] Data cleaned. Remaining rows: {len(df)}")
    return df

def load_to_sqlite(df, db_name, table_name):
    conn = sqlite3.connect(db_name)
    try:
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        print(f"[INFO] Loaded {len(df)} rows into '{db_name}' (table: {table_name})")
    except Exception as e:
        raise Exception(f"Failed to load data to SQLite: {e}")
    finally:
        conn.close()

def parse_args():
    parser = argparse.ArgumentParser(description="CSV to SQLite ETL script")
    parser.add_argument('--csv', required=True, help="Path to input CSV file")
    parser.add_argument('--db', required=True, help="Output SQLite database file")
    parser.add_argument('--table', default="data", help="Table name (default: 'data')")
    return parser.parse_args()

def main():
    args = parse_args()

    if not os.path.exists(args.csv):
        print(f"[ERROR] CSV file '{args.csv}' does not exist.")
        return

    try:
        df_raw = extract_csv(args.csv)
        df_clean = transform_data(df_raw)
        load_to_sqlite(df_clean, args.db, args.table)
        print("[SUCCESS] ETL pipeline completed successfully.")
    except Exception as e:
        print(f"[FAILURE] {e}")

if __name__ == "__main__":
    main()
