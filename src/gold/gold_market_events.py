from src.config import spark
from pyspark.sql.functions import to_date,col,count,year,month,round

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

gold_market_events_df=gold_market_events_df.select(
    col("s.ticker").alias("ticker"),
    col("s.event_date"),
    col("s.open_price"),
    col("s.close_price"),
    col("s.high_price"),
    col("s.low_price"),
    col("s.volume"),
    
    col("n.title"),
    col("n.summary"),
    col("n.provider"),
    col("n.news_id"),
    col("n.canonical_url")
)

gold_market_events_df = (gold_market_events_df
    .withColumn("year", year("event_date"))
    .withColumn("month", month("event_date"))
)

# business metrics
gold_market_events_df = (gold_market_events_df
    .withColumn("price_change",round(col("close_price") - col("open_price"),2))
)

gold_market_events_df = (gold_market_events_df
    .withColumn("price_change_percent",(round(
            ((col("close_price") - col("open_price"))/ col("open_price")) * 100,2)))
)
# checking metrics
gold_market_events_df.select(
    "ticker",
    "open_price",
    "close_price",
    "price_change",
    "price_change_percent"
).show(5, truncate=False)

# print("rows :",gold_market_events_df.count())
# print("silver stocks",silver_stocks_df.count())
# print("silver news",silver_news_df.count())

gold_market_events_df.groupBy("ticker").agg(count("*")).show()
# gold_market_events_df.show(5,truncate=False)

gold_market_events_df.write \
.mode("overwrite") \
.partitionBy("year", "month") \
.parquet(f"s3a://market-intelligence-platform/gold/market_events/")

gold_df = spark.read.parquet(
    "s3a://market-intelligence-platform/gold/market_events/"
)

gold_df.printSchema()
gold_df.show(5, truncate=False)

# gold_market_events_df.printSchema()

# silver_news_df.printSchema()
# silver_stocks_df.printSchema()