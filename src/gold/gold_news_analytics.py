from src.config import get_spark

from pyspark.sql.functions import (
    to_date,
    year,
    month,
    col,
    count,
    countDistinct
)


def run():

    spark = get_spark()

    # ============================================================
    # 1. READ SILVER NEWS
    # ============================================================

    silver_news_df = spark.read.parquet(
        "s3a://market-intelligence-platform/silver/news/"
    )

    print("SILVER NEWS ROWS:", silver_news_df.count())


    # ============================================================
    # 2. CREATE EVENT DATE
    # ============================================================

    silver_news_df = silver_news_df.withColumn(
        "event_date",
        to_date("event_time")
    )


    # ============================================================
    # 3. GOLD NEWS ANALYTICS
    #
    # Grain:
    # ticker + event_date
    #
    # One row represents the news activity for a stock
    # on a particular day.
    # ============================================================

    gold_news_analytics_df = (
        silver_news_df
        .groupBy(
            "ticker",
            "event_date"
        )
        .agg(
            count("*").alias("news_count"),
            countDistinct(
                "provider"
            ).alias("unique_providers")
        )
    )


    # ============================================================
    # 4. ADD PARTITION COLUMNS
    # ============================================================

    gold_news_analytics_df = (
        gold_news_analytics_df
        .withColumn(
            "year",
            year("event_date")
        )
        .withColumn(
            "month",
            month("event_date")
        )
    )


    # ============================================================
    # 5. CHECK RESULTS
    # ============================================================

    # print("=== GOLD NEWS ANALYTICS ===")

    # gold_news_analytics_df.orderBy(
    #     col("event_date").desc(),
    #     col("ticker")
    # ).show(
    #     20,
    #     truncate=False
    # )


    # print(
    #     "GOLD NEWS ANALYTICS ROWS:",
    #     gold_news_analytics_df.count()
    # )


    # ============================================================
    # 6. CHECK GRAIN
    # ============================================================

    # print("=== DUPLICATE CHECK ===")

    # gold_news_analytics_df.groupBy(
    #     "ticker",
    #     "event_date"
    # ).count().filter(
    #     col("count") > 1
    # ).show(
    #     20,
    #     truncate=False
    # )


    # ============================================================
    # 7. REDUCE OUTPUT FILES
    # ============================================================

    gold_news_analytics_df = (
        gold_news_analytics_df.coalesce(4)
    )


    # ============================================================
    # 8. WRITE GOLD
    # ============================================================

    gold_news_analytics_df.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(
            "s3a://market-intelligence-platform/gold/news_analytics/"
        )


    print("=== GOLD NEWS ANALYTICS COMPLETED ===")


if __name__ == "__main__":
    run()