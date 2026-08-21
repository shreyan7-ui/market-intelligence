from src.config import get_spark

from pyspark.sql.functions import (
    col,
    when,
    year,
    month,
    round
)
def run():

    spark = get_spark()

    # ============================================================
    # 1. READ SILVER STOCKS
    # ============================================================

    silver_stocks_df = spark.read.parquet(
        "s3a://market-intelligence-platform/silver/stocks/"
    )

    print("SILVER STOCK ROWS:", silver_stocks_df.count())


    # ============================================================
    # 2. SELECT REQUIRED BUSINESS COLUMNS
    # ============================================================

    gold_stocks_analytics_df = silver_stocks_df.select(
        col("ticker"),
        col("event_time"),
        col("open_price"),
        col("close_price"),
        col("high_price"),
        col("low_price"),
        col("volume")
    )


    # ============================================================
    # 3. PRICE CHANGE
    # ============================================================

    gold_stocks_analytics_df = (
        gold_stocks_analytics_df
        .withColumn(
            "price_change",
            round(
                col("close_price") - col("open_price"),
                2
            )
        )
    )


    # ============================================================
    # 4. PRICE CHANGE PERCENT
    # ============================================================

    gold_stocks_analytics_df = (
        gold_stocks_analytics_df
        .withColumn(
            "price_change_percent",
            when(
                col("open_price") != 0,
                round(
                    (
                        (col("close_price") - col("open_price"))
                        / col("open_price")
                    ) * 100,
                    2
                )
            ).otherwise(None)
        )
    )


    # ============================================================
    # 5. TRADING RANGE
    # ============================================================

    gold_stocks_analytics_df = (
        gold_stocks_analytics_df
        .withColumn(
            "trading_range",
            round(
                col("high_price") - col("low_price"),
                2
            )
        )
    )


    # ============================================================
    # 6. DAY STATUS
    # ============================================================

    gold_stocks_analytics_df = (
        gold_stocks_analytics_df
        .withColumn(
            "day_status",
            when(
                col("close_price") > col("open_price"),
                "GAIN"
            )
            .when(
                col("close_price") < col("open_price"),
                "LOSS"
            )
            .otherwise("NO_CHANGE")
        )
    )


    # ============================================================
    # 7. PARTITION COLUMNS
    # ============================================================

    gold_stocks_analytics_df = (
        gold_stocks_analytics_df
        .withColumn("year", year("event_time"))
        .withColumn("month", month("event_time"))
    )


    # ============================================================
    # 8. CHECK BUSINESS METRICS
    # ============================================================

    # print("=== GOLD STOCK ANALYTICS SAMPLE ===")

    # gold_stocks_analytics_df.select(
    #     "ticker",
    #     "event_time",
    #     "open_price",
    #     "close_price",
    #     "price_change",
    #     "price_change_percent",
    #     "trading_range",
    #     "day_status"
    # ).show(20, truncate=False)


    # ============================================================
    # 9. WRITE GOLD
    # ============================================================

    (
        gold_stocks_analytics_df
        .write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet(
            "s3a://market-intelligence-platform/gold/stock_analytics/"
        )
    )


    # ============================================================
    # 10. VALIDATION
    # ============================================================

    # gold_df = spark.read.parquet(
    #     "s3a://market-intelligence-platform/gold/stock_analytics/"
    # )

    # print("GOLD STOCK ANALYTICS ROWS:", gold_df.count())

    # print("=== GOLD STOCK GRAIN CHECK ===")

    # gold_df.groupBy(
    #     "ticker",
    #     "event_time"
    # ).count().filter(
    #     col("count") > 1
    # ).show(20, truncate=False)


    # print("=== GOLD STOCK ANALYTICS SCHEMA ===")

    # gold_df.printSchema()

    print("=== GOLD STOCK ANALYTICS COMPLETED ===")


if __name__ == "__main__":
    run()