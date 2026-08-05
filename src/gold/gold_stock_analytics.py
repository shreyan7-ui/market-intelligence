from src.config import get_spark
from pyspark.sql.functions import to_date,col, when,year,month,round

def run():
    
    spark = get_spark()
    
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

    gold_stocks_analytics_df = (gold_stocks_analytics_df
        .withColumn("day_status",
            when(col("close_price") > col("open_price"), "GAIN")
            .when(col("close_price") < col("open_price"), "LOSS")
            .otherwise("NO_CHANGE")
        )
    )

    gold_stocks_analytics_df = (gold_stocks_analytics_df
        .withColumn("year", year("event_time"))
        .withColumn("month", month("event_time"))
    )


    gold_stocks_analytics_df.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(f"s3a://market-intelligence-platform/gold/stock_analytics/")

    # gold_stocks_analytics_df.show(5)
    # gold_stocks_analytics_df.printSchema()

if __name__ == "__main__":
    run()