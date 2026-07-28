from src.config import spark
from pyspark.sql.functions import to_date,col,count,year,month,round

silver_stocks_df =spark.read.parquet("s3a://market-intelligence-platform/silver/stocks/")

silver_stocks_df =silver_stocks_df.withColumn("event_date",to_date("event_time")) 

gold_stocks_analytics_df=silver_stocks_df .select(
    col("ticker"),
    col("event_time"),
    col("open_price"),
    col("close_price"),
    col("high_price"),
    col("low_price"),
    col("volume")
)

gold_stocks_analytics_df=( gold_stocks_analytics_df
    .withColumn("price_change",round(col("close_price") - col("open_price"),2)
    ))

gold_stocks_analytics_df=( gold_stocks_analytics_df
    .withColumn("price_change_percent",(round(
            ((col("close_price") - col("open_price"))/ col("open_price")) * 100,2))
    ))

gold_stocks_analytics_df = (gold_stocks_analytics_df
    .withColumn("trading_range",round(col("high_price") - col("low_price"),2))
)

gold_stocks_analytics_df.select(
    "ticker",
    "high_price",
    "low_price",
    "trading_range",
    "price_change",
    "price_change_percent"
).show()

# gold_stocks_analytics_df.printSchema()