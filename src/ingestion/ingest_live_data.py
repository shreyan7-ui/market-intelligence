import os, json
from datetime import datetime
import yfinance as yf
import requests

# ─── CONFIGURATION ───
TICKERS = ["NVDA", "AAPL", "MSFT", "GOOG", "RELIANCE.NS", "IBM"]
TODAY_STR = datetime.today().strftime("%Y-%m-%d")

# create the directory once
NEWS_DIR = f"data/raw/news/date={TODAY_STR}"
STOCK_DIR = f"data/raw/stocks/date={TODAY_STR}"

os.makedirs(NEWS_DIR, exist_ok=True)
os.makedirs(STOCK_DIR, exist_ok=True)


RAW_NEWS_PATH = f"{NEWS_DIR}/market_news.json"
RAW_STOCK_PATH = f"{STOCK_DIR}/stock_prices.json"

def fetch_stock_prices():
    """Extracts daily makrte metrics for our target tickers using yfinance"""
    print(f"[1/2] Extracting stock data for: {TICKERS}...")
    combined_stocks = []
    
    for ticker in TICKERS:
        try:
            stock = yf.Ticker(ticker)
            # Fetch the most recent 1 day of historical pricing row
            hist = stock.history(period="1d")
            
            if not hist.empty:
                # Convert the pandas dataframe row to a dictionary format
                metrics = hist.iloc[-1].to_dict()
                # Explicitly record the exact date stamp of this market capture
                metrics['ticker'] = ticker
                metrics["extracted_date"] = str(hist.index[-1].date())
                combined_stocks.append(metrics)
        except Exception as e:
            print(f"Error extracting stock data for {ticker}: {e}")
            
    # Save the output to our raw zone
    with open(RAW_STOCK_PATH, 'w') as f:
        json.dump(combined_stocks, f, indent=4)
    print(f"Stock data successfully landing at: {RAW_STOCK_PATH}")
    

def fetch_market_news():
    """Extracts recent news articles and stores one article per record."""
    print(f"[2/2] Extracting live news feed for: {TICKERS}...")

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
                    "provider": content.get("provider", {}).get("displayName"),
                    "provider_publish_time": content.get("providerPublishTime"),
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
            print(f"Error extracting news for {ticker}: {e}")

    with open(RAW_NEWS_PATH, "w") as f:
        json.dump(all_articles, f, indent=4)

    print(f"Saved {len(all_articles)} news articles.")
    print(f"News feed successfully landed at: {RAW_NEWS_PATH}")
    
    

def run():
    try:
        print(f"Starting Ingestion Pipeline [Run Date: {TODAY_STR}]")
        fetch_stock_prices()
        fetch_market_news()
        print("Ingestion Pipeline Completed Successfully.")
        
    except Exception as e:
        print(f"Ingestion Failed: {e}")
        raise
    
if __name__ == "__main__":
    run()