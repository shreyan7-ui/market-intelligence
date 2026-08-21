import os
import json
from datetime import datetime

import yfinance as yf
import boto3


# ─── CONFIGURATION ───

TICKERS = ["NVDA", "AAPL", "MSFT", "GOOG", "RELIANCE.NS", "IBM"]

TODAY_STR = datetime.today().strftime("%Y-%m-%d")

BUCKET_NAME = "market-intelligence-platform"

# Local raw paths
NEWS_DIR = f"data/raw/news/date={TODAY_STR}"
STOCK_DIR = f"data/raw/stocks/date={TODAY_STR}"

os.makedirs(NEWS_DIR, exist_ok=True)
os.makedirs(STOCK_DIR, exist_ok=True)

RAW_NEWS_PATH = f"{NEWS_DIR}/market_news.json"
RAW_STOCK_PATH = f"{STOCK_DIR}/stock_prices.json"


# AWS client
s3 = boto3.client("s3")


def upload_to_s3(local_path, s3_key):
    """Upload a local file to the S3 raw layer."""

    try:
        s3.upload_file(
            local_path,
            BUCKET_NAME,
            s3_key
        )

        print(
            f"Uploaded to S3: "
            f"s3://{BUCKET_NAME}/{s3_key}"
        )

    except Exception as e:
        print(f"S3 upload failed for {local_path}: {e}")
        raise


def fetch_stock_prices():
    """Extract daily market metrics and upload them to S3."""

    print(f"[1/2] Extracting stock data for: {TICKERS}...")

    combined_stocks = []

    for ticker in TICKERS:

        try:

            stock = yf.Ticker(ticker)

            hist = stock.history(period="1d")

            if not hist.empty:

                metrics = hist.iloc[-1].to_dict()

                metrics["ticker"] = ticker

                metrics["extracted_date"] = str(
                    hist.index[-1].date()
                )

                combined_stocks.append(metrics)

        except Exception as e:

            print(
                f"Error extracting stock data "
                f"for {ticker}: {e}"
            )

    # Save locally
    with open(RAW_STOCK_PATH, "w") as f:
        json.dump(
            combined_stocks,
            f,
            indent=4,
            default=str
        )

    print(
        f"Stock data successfully landed locally at: "
        f"{RAW_STOCK_PATH}"
    )

    # Upload to S3
    s3_key = (
        f"raw/stocks/"
        f"date={TODAY_STR}/"
        f"stock_prices.json"
    )

    upload_to_s3(
        RAW_STOCK_PATH,
        s3_key
    )


def fetch_market_news():
    """Extract recent news articles and upload them to S3."""

    print(
        f"[2/2] Extracting live news feed "
        f"for: {TICKERS}..."
    )

    all_articles = []

    for ticker in TICKERS:

        try:

            stock = yf.Ticker(ticker)

            raw_articles = stock.news

            if not raw_articles:
                continue

            for article in raw_articles:

                content = article.get("content", {})

                record = {

                    "stock": ticker,

                    "news_id": article.get("id"),

                    "title": content.get("title"),

                    "summary": content.get("summary"),

                    "pub_date": content.get("pubDate"),

                    "provider": (
                        content.get("provider", {})
                        .get("displayName")
                    ),

                    "provider_publish_time": (
                        content.get("providerPublishTime")
                    ),

                    "canonical_url": (
                        content.get("canonicalUrl", {}).get("url")
                        if content.get("canonicalUrl")
                        else None
                    ),

                    "thumbnail": (
                        content.get("thumbnail", {})
                        .get("originalUrl")
                        if content.get("thumbnail")
                        else None
                    ),

                    "language": content.get("language"),

                    "region": content.get("region"),

                    "scraped_date": TODAY_STR
                }

                all_articles.append(record)

        except Exception as e:

            print(
                f"Error extracting news "
                f"for {ticker}: {e}"
            )

    # Save locally
    with open(RAW_NEWS_PATH, "w") as f:

        json.dump(
            all_articles,
            f,
            indent=4,
            default=str
        )

    print(
        f"Saved {len(all_articles)} news articles."
    )

    print(
        f"News feed successfully landed locally at: "
        f"{RAW_NEWS_PATH}"
    )

    # Upload to S3
    s3_key = (
        f"raw/news/"
        f"date={TODAY_STR}/"
        f"market_news.json"
    )

    upload_to_s3(
        RAW_NEWS_PATH,
        s3_key
    )


def run():

    try:

        print(
            f"Starting Ingestion Pipeline "
            f"[Run Date: {TODAY_STR}]"
        )

        fetch_stock_prices()

        fetch_market_news()

        print(
            "Ingestion Pipeline Completed Successfully."
        )

    except Exception as e:

        print(f"Ingestion Failed: {e}")

        raise


if __name__ == "__main__":
    run()