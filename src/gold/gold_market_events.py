from src.config import spark
from pyspark.sql.functions import to_date

silver_news_df=spark.read.parquet("s3a://market-intelligence-platform/silver/news/")

silver_stocks_df=spark.read.parquet("s3a://market-intelligence-platform/silver/stocks/")

silver_news_df=silver_news_df.withColumn("event_date",to_date("event_time"))
silver_stocks_df=silver_stocks_df.withColumn("event_date",to_date("event_time"))

gold_market_events_df = (silver_stocks_df.alias("s")
    .join(silver_news_df.alias("n"),
        on=["ticker", "event_date"],
        how="left"
    )
)
# print("after join :",gold_market_events_df.count())

# gold_market_events_df.show(5,truncate=False)

gold_market_events_df.printSchema()

# silver_news_df.printSchema()
# silver_stocks_df.printSchema()