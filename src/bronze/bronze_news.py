from src.config import spark
# from pyspark.sql import DataFrame

historical_df = spark.read.parquet(
"s3a://market-intelligence-platform/raw/historical/cleaned_historical_news.parquet"
)

live_df = (spark.read.option("multiline","true")
    .json("s3a://market-intelligence-platform/raw/news/date=*/market_news.json"))

historical_df.printSchema()
live_df.printSchema(1)

historical_df.show(2,truncate=False)
live_df.show(2,truncate=False)