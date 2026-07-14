from src.config import spark

historical_df=spark.read.parquet(
    "s3a://market-intelligence-platform/raw/bronze/cleaned_historical_news.parquet"
) 