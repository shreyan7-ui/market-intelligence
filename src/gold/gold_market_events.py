from src.config import get_spark

from pyspark.sql.functions import (
    to_date,
    col,
    count,
    year,
    month,
    round
)


def run():

    spark = get_spark()

    # ============================================================
    # 1. READ SILVER
    # ============================================================

    silver_news_df = spark.read.parquet(
        "s3a://market-intelligence-platform/silver/news/"
    )

    silver_stocks_df = spark.read.parquet(
        "s3a://market-intelligence-platform/silver/stocks/"
    )

    print("SILVER NEWS ROWS:", silver_news_df.count())
    print("SILVER STOCK ROWS:", silver_stocks_df.count())


    # ============================================================
    # 2. CREATE EVENT DATE
    # ============================================================

    silver_news_df = silver_news_df.withColumn(
        "event_date",
        to_date("event_time")
    )

    silver_stocks_df = silver_stocks_df.withColumn(
        "event_date",
        to_date("event_time")
    )


    # ============================================================
    # 3. CHECK STOCK GRAIN
    # ============================================================

    # print("=== STOCK GRAIN CHECK ===")

    # silver_stocks_df.groupBy(
    #     "ticker",
    #     "event_date"
    # ).count().filter(
    #     col("count") > 1
    # ).show(20, truncate=False)


    # ============================================================
    # 4. CHECK NEWS DUPLICATES
    # ============================================================

    # print("=== NEWS ID DUPLICATE CHECK ===")

    # silver_news_df.groupBy(
    #     "news_id"
    # ).count().filter(
    #     col("news_id").isNotNull() &
    #     (col("count") > 1)
    # ).orderBy(
    #     col("count").desc()
    # ).show(20, truncate=False)


    # ============================================================
    # 5. JOIN STOCKS + NEWS
    # ============================================================

    gold_market_events_df = (
        silver_stocks_df.alias("s")
        .join(
            silver_news_df.alias("n"),
            on=[
                "ticker",
                "event_date"
            ],
            how="left"
        )

    )


    print(
        "GOLD ROWS AFTER JOIN:",
        gold_market_events_df.count()
    )


    # ============================================================
    # 6. SELECT GOLD COLUMNS
    # ============================================================

    gold_market_events_df = gold_market_events_df.select(
        col("s.ticker").alias("ticker"),
        col("s.event_time").alias("event_time"),
        col("s.open_price").alias("open_price"),
        col("s.close_price").alias("close_price"),
        col("s.high_price").alias("high_price"),
        col("s.low_price").alias("low_price"),
        col("s.volume").alias("volume"),
        col("n.title").alias("title"),
        col("n.summary").alias("summary"),
        col("n.provider").alias("provider"),
        col("n.news_id").alias("news_id"),
        col("n.canonical_url").alias("canonical_url")

    )


    # ============================================================
    # 7. ADD PARTITION COLUMNS
    # ============================================================

    gold_market_events_df = (
        gold_market_events_df
        .withColumn(
            "year",
            year("event_time")
        )
        .withColumn(
            "month",
            month("event_time")
        )

    )


    # ============================================================
    # 8. BUSINESS METRICS
    # ============================================================

    gold_market_events_df = (
        gold_market_events_df
        .withColumn(
            "price_change",
            round(
                col("close_price") -
                col("open_price"),
                2
            )
        )
        .withColumn(
            "price_change_percent",
            round(
                (
                    (
                        col("close_price") -
                        col("open_price")
                    )
                    /
                    col("open_price")
                ) * 100,
                2
            )
        )

    )


    # ============================================================
    # 9. CHECK GOLD OUTPUT
    # ============================================================

    # print("=== GOLD SAMPLE ===")

    # gold_market_events_df.select(
    #     "ticker",
    #     "event_time",
    #     "open_price",
    #     "close_price",
    #     "price_change",
    #     "price_change_percent",
    #     "news_id",
    #     "title"
    # ).show(
    #     10,
    #     truncate=False
    # )


    # print("=== GOLD ROWS PER TICKER ===")

    # gold_market_events_df.groupBy(
    #     "ticker"
    # ).agg(
    #     count("*").alias("row_count")
    # ).show()


    # ============================================================
    # 10. WRITE GOLD
    # ============================================================

    gold_market_events_df.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(
            "s3a://market-intelligence-platform/gold/market_events/"
        )


    print("=== GOLD MARKET EVENTS COMPLETED ===")


if __name__ == "__main__":
    run()