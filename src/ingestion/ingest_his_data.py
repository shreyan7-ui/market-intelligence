import pandas as pd
import os

# ─── CONFIGURATION ───
INPUT_PATH = "data/raw/historical/analyst_ratings_processed.csv"
OUTPUT_PATH = "data/raw/historical/cleaned_historical_news.parquet"


def clean_historical_data():
    print("Reading historical Kaggle dataset (this might take a few seconds)...")

    if not os.path.exists(INPUT_PATH):
        print(
            f"Error: Could not find the Kaggle CSV file at {INPUT_PATH}. Please download it and place it there!"
        )
        return

    df = pd.read_csv(INPUT_PATH)
    print(f"Raw data loaded: {df.shape[0]:,} rows found.")

    # Let's see what columns exist and clean them up
    # Standardizing column names to lowercase is a best practice before loading to BigQuery
    df.columns = df.columns.str.strip().str.lower()

    print("Standardizing column schemas...")
    # Keep only the columns we actually care about for our platform
    # Adjust these names based on the exact columns in your downloaded Kaggle file
    columns_to_keep = ["title", "date", "stock"]
    df = df[columns_to_keep]

    # This prevents BigQuery from failing due to mixed date/time string formats
    df['date'] = pd.to_datetime(df['date'], errors='coerce', utc=True).dt.strftime('%Y-%m-%d')

    # Drop rows that don't have a valid date or stock ticker
    df = df.dropna(subset=["date", "stock"])

    print(f"Saving cleaned historical base as optimized Parquet to: {OUTPUT_PATH}")
    # Saving as Parquet instead of CSV makes cloud storage cheaper and queries 10x faster!
    df.to_parquet(OUTPUT_PATH, index=False)
    print("Historical base preparation complete!")


if __name__ == "__main__":
    clean_historical_data()
