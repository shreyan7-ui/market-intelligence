from src.config import get_spark

from pyspark.sql.functions import (
    to_timestamp,
    to_date,
    col,
    count,
    upper,
    trim,
    dayofmonth,
    month,
    year,
    row_number,
)

from pyspark.sql.window import Window


def run():

    spark = get_spark()

    # ============================================================
    # 1. READ BRONZE
    # ============================================================

    stocks_df = spark.read.parquet(
        "s3a://market-intelligence-platform/bronze/stocks/date=*/"
    )

    # DEBUG CHECK - expensive full dataset action
    # print("BRONZE ROW COUNT:", stocks_df.count())


    # ============================================================
    # 2. STANDARDIZE COLUMN NAMES
    # ============================================================

    stocks_df = (
        stocks_df
        .withColumnRenamed("extracted_date", "event_time")
        .withColumnRenamed("Close", "close_price")
        .withColumnRenamed("Open", "open_price")
        .withColumnRenamed("High", "high_price")
        .withColumnRenamed("Low", "low_price")
        .withColumnRenamed("Volume", "volume")
        .withColumnRenamed("Dividends", "dividends")
        .withColumnRenamed("Stock Splits", "stock_splits")
    )


    # ============================================================
    # 3. CONVERT EVENT TIME TO TIMESTAMP
    # ============================================================

    stocks_df = stocks_df.withColumn(
        "event_time",
        to_timestamp("event_time")
    )


    # ============================================================
    # 4. STANDARDIZE TICKER
    # ============================================================

    stocks_df = stocks_df.withColumn(
        "ticker",
        upper(trim(col("ticker")))
    )


    # ============================================================
    # 5. NULL CHECK
    # ============================================================

    # DEBUG / DATA-QUALITY CHECK
    # Expensive because it scans the dataset.

    # print("=== NULL CHECK ===")
    #
    # stocks_df.select([
    #     count(col(c)).alias(f"{c}_non_null")
    #     for c in stocks_df.columns
    # ]).show(truncate=False)


    # ============================================================
    # 6. REMOVE INVALID RECORDS
    # ============================================================

    stocks_df = stocks_df.filter(
        col("ticker").isNotNull()
        & col("event_time").isNotNull()
        & col("open_price").isNotNull()
        & col("high_price").isNotNull()
        & col("low_price").isNotNull()
        & col("close_price").isNotNull()
        & col("volume").isNotNull()
    )


    # ============================================================
    # 7. CREATE EVENT DATE
    # ============================================================

    stocks_df = stocks_df.withColumn(
        "event_date",
        to_date("event_time")
    )


    # ============================================================
    # 8. CHECK EXACT DUPLICATES
    # ============================================================

    # DEBUG CHECK
    # Keep this for development / troubleshooting.
    # Don't run on every Airflow execution.

    # print("=== EXACT STOCK DUPLICATES ===")
    #
    # stocks_df.groupBy(
    #     "ticker",
    #     "event_time",
    #     "open_price",
    #     "close_price",
    #     "high_price",
    #     "low_price",
    #     "volume"
    # ).count().filter(
    #     col("count") > 1
    # ).orderBy(
    #     col("count").desc()
    # ).show(20, truncate=False)


    # ============================================================
    # 9. CHECK MULTIPLE INGESTIONS
    # ============================================================

    # DEBUG CHECK
    # This is expensive because it performs a groupBy over the data.

    # print("=== STOCK ROWS PER TICKER + DATE BEFORE DEDUP ===")
    #
    # stocks_df.groupBy(
    #     "ticker",
    #     "event_date"
    # ).count().filter(
    #     col("count") > 1
    # ).orderBy(
    #     col("count").desc()
    # ).show(20, truncate=False)


    # ============================================================
    # 10. DEDUPLICATE
    #
    # Business key:
    #     ticker + event_date
    #
    # If the same stock/date was ingested multiple times,
    # keep the latest ingestion.
    # ============================================================

    window_spec = Window.partitionBy(
        "ticker",
        "event_date"
    ).orderBy(
        col("ingestion_timestamp").desc()
    )

    stocks_df = (
        stocks_df
        .withColumn(
            "row_num",
            row_number().over(window_spec)
        )
        .filter(
            col("row_num") == 1
        )
        .drop("row_num")
    )


    # ============================================================
    # 11. VERIFY DEDUPLICATION
    # ============================================================

    # DEBUG CHECK
    # Keep for troubleshooting but don't execute every DAG run.

    # print("=== STOCK ROWS PER TICKER + DATE AFTER DEDUP ===")
    #
    # stocks_df.groupBy(
    #     "ticker",
    #     "event_date"
    # ).count().filter(
    #     col("count") > 1
    # ).show(20, truncate=False)

    # DEBUG CHECK
    # print("SILVER ROW COUNT:", stocks_df.count())


    # ============================================================
    # 12. CREATE PARTITION COLUMNS
    # ============================================================

    stocks_df = (
        stocks_df
        .withColumn("year", year("event_time"))
        .withColumn("month", month("event_time"))
        .withColumn("day", dayofmonth("event_time"))
    )


    # ============================================================
    # 13. SHOW FINAL SILVER DATA
    # ============================================================

    # DEBUG CHECK
    # Avoid show() on every Airflow run.

    # print("=== SILVER STOCK SAMPLE ===")
    #
    # stocks_df.orderBy(
    #     "event_date",
    #     "ticker"
    # ).show(
    #     20,
    #     truncate=False
    # )


    # ============================================================
    # 14. WRITE SILVER
    # ============================================================

    stocks_df.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(
            "s3a://market-intelligence-platform/silver/stocks/"
        )

    print("=== SILVER STOCKS COMPLETED ===")


if __name__ == "__main__":
    run()