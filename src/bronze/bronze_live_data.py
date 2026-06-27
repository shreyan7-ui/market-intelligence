import os, json
from datetime import datetime
import yfinance as yf
import requests

# ─── CONFIGURATION ───
TICKERS = ["NVDA", "AAPL", "MSFT", "GOOG", "RELIANCE.NS", "IBM"]
TODAY_STR = datetime.today().strftime("%Y-%m-%d")

# create the directory once
TODAY_DIR = f"data/raw/daily/date={TODAY_STR}"
os.makedirs(TODAY_DIR, exist_ok=True)

# Local Storage Landing Zone Paths
RAW_STOCK_PATH = f"{TODAY_DIR}/stock_prices.json"
RAW_NEWS_PATH = f"{TODAY_DIR}/market_news.json"

def fetch_stock_prices():
    """Extracts daily makrte metrics for our target tickers using yfinance"""
    print(f"[1/2] Extracting stock data for: {TICKERS}...")
    combined_stocks = {}
    
    for ticker in TICKERS:
        try:
            stock = yf.Ticker(ticker)
            # Fetch the most recent 1 day of historical pricing row
            hist = stock.history(period="1d")
            
            if not hist.empty:
                # Convert the pandas dataframe row to a dictionary format
                metrics = hist.iloc[-1].to_dict()
                # Explicitly record the exact date stamp of this market capture
                metrics['extracted_date'] = str(hist.index[-1].date())
                combined_stocks[ticker] = metrics
        except Exception as e:
            print(f"Error extracting stock data for {ticker}: {e}")
            
    # Save the output to our raw zone
    with open(RAW_STOCK_PATH, 'w') as f:
        json.dump(combined_stocks, f, indent=4)
    print(f"Stock data successfully landing at: {RAW_STOCK_PATH}")
    

def fetch_market_news():
    """Extracts recent text news articles directly linked to our tickers from Yahoo Finance."""
    print(f"[2/2] Extracting live news feed for: {TICKERS}...")
    combined_news = {}
    
    for ticker in TICKERS:
        try:
            stock = yf.Ticker(ticker)
            # Native yfinance endpoint that scrapes the latest headlines for this asset
            raw_articles = stock.news
            
            if raw_articles:
                # Store the list of articles under the company's ticker key
                combined_news[ticker] = raw_articles
        except Exception as e:
            print(f"Error extracting news for {ticker}: {e}")
            
    # Save the output to our raw zone
    with open(RAW_NEWS_PATH, 'w') as f:
        json.dump(combined_news, f, indent=4)
    print(f"News feed successfully landing at: {RAW_NEWS_PATH}")


if __name__ == "__main__":
    print(f"Starting Ingestion Execution Pipeline [Run Date: {TODAY_STR}]")
    fetch_stock_prices()
    fetch_market_news()
    print("Ingestion Sequence Finished Successfully!")