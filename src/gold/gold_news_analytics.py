from src.config import get_spark
from pyspark.sql.functions import to_date, year,countDistinct,count,month,col

def run():
    
    spark = get_spark()
    
    # gold_market_events=spark.read.parquet(
    #     "s3a://market-intelligence-platform/gold/market_events/"
    # )

    # gold_stock_analytics=spark.read.parquet(
    #     "s3a://market-intelligence-platform/gold/stock_analytics/"
    # )

    # gold_stock_analytics.printSchema()
    # gold_market_events.printSchema()

    silver_news_df = spark.read.parquet("s3a://market-intelligence-platform/silver/news/")

    gold_news_analytics_df = (silver_news_df
        .withColumn("event_date", to_date("event_time"))
        .groupBy("ticker", "event_time")
        .agg(count("*").alias("news_count"),
            countDistinct("provider").alias("unique_providers")
        )
    )

    gold_news_analytics_df = (gold_news_analytics_df
        .withColumn("year", year("event_time"))
        .withColumn("month", month("event_time"))
    )
    # print("Silver rows:", silver_news_df.count())
    # print(gold_news_analytics_df.count())
    # print(gold_news_analytics_df.rdd.getNumPartitions())

    # This reduces the number of output files and concurrent writers.
    gold_news_analytics_df = gold_news_analytics_df.coalesce(4)


    # print(spark.sparkContext.defaultParallelism)
    # print(spark.conf.get("spark.sql.shuffle.partitions"))

    # reduces the number of output files uploaded to S3
    gold_news_analytics_df = gold_news_analytics_df.coalesce(4)

    # gold_news_analytics_df=spark.read.parquet(
    #     "s3a://market-intelligence-platform/gold/news_analytics/"
    # )

    gold_news_analytics_df.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(f"s3a://market-intelligence-platform/gold/news_analytics/")


    # silver_news_df.select("provider").show(20, truncate=False)
    # silver_news_df.filter(col("provider").isNotNull()).show(20, truncate=False)


    gold_news_analytics_df.printSchema()
    # gold_news_analytics_df.show(3,truncate=False)
    # print("Market Events:", gold_market_events.count())
    # print("Stock Analytics:", gold_stock_analytics.count())
    print("Gold News Analytics completed successfully.")


if __name__ == "__main__":
    run()