from src.config import spark
from pyspark.sql import DataFrame

historical_df = spark.read.parquet(
"s3a://bucket/raw/news/historical/cleaned_historical_news.parquet"
)

live_df = spark.read.json(
"s3a://bucket/raw/stocks/market_news_*.json"
)