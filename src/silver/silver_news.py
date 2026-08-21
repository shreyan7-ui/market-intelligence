from pyspark.sql.functions import (
    lit,
    to_timestamp,
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

from src.config import get_spark


def run():

    spark = get_spark()

    historical_cols = [
        "summary",
        "provider",
        "news_id",
        "canonical_url",
        "thumbnail",
    ]

    # ============================================================
    # 1. READ BRONZE
    # ============================================================

    historical_df = spark.read.parquet(
        "s3a://market-intelligence-platform/bronze/historical/"
    )

    news_df = spark.read.parquet(
        "s3a://market-intelligence-platform/bronze/news/date=*/"
    )

    # ============================================================
    # 2. RENAME COLUMNS
    # ============================================================

    historical_df = (
        historical_df
        .withColumnRenamed("stock", "ticker")
        .withColumnRenamed("date", "event_time")
    )

    news_df = (
        news_df
        .withColumnRenamed("stock", "ticker")
        .withColumnRenamed("pub_date", "event_time")
    )

    # ============================================================
    # 3. ADD MISSING COLUMNS TO HISTORICAL DATA
    # ============================================================

    historical_df = (
        historical_df
        .withColumn("summary", lit(None).cast("string"))
        .withColumn("provider", lit(None).cast("string"))
        .withColumn("news_id", lit(None).cast("string"))
        .withColumn("canonical_url", lit(None).cast("string"))
        .withColumn("thumbnail", lit(None).cast("string"))
    )

    # ============================================================
    # 4. SELECT SAME SCHEMA
    # ============================================================

    historical_df = historical_df.select(
        "ticker",
        "title",
        "summary",
        "event_time",
        "provider",
        "news_id",
        "canonical_url",
        "thumbnail",
        "source_system",
        "ingestion_timestamp",
    )

    news_df = news_df.select(
        "ticker",
        "title",
        "summary",
        "event_time",
        "provider",
        "news_id",
        "canonical_url",
        "thumbnail",
        "source_system",
        "ingestion_timestamp",
    )

    # ============================================================
    # 5. CONVERT EVENT TIME
    # ============================================================

    historical_df = historical_df.withColumn(
        "event_time",
        to_timestamp("event_time", "yyyy-MM-dd")
    )

    news_df = news_df.withColumn(
        "event_time",
        to_timestamp("event_time")
    )

    # ============================================================
    # 6. ENSURE SAME DATA TYPES
    # ============================================================

    for column in historical_cols:
        historical_df = historical_df.withColumn(
            column,
            col(column).cast("string")
        )

    news_df = news_df.withColumn(
        "news_id",
        col("news_id").cast("string")
    )

    # ============================================================
    # 7. UNION HISTORICAL + LIVE NEWS
    # ============================================================

    silver_news_df = historical_df.unionByName(news_df)

    # ============================================================
    # 8. STANDARDIZE VALUES
    # ============================================================

    silver_news_df = (
        silver_news_df
        .withColumn("ticker", upper(trim(col("ticker"))))
        .withColumn("title", trim(col("title")))
        .withColumn("summary", trim(col("summary")))
        .withColumn("provider", trim(col("provider")))
        .withColumn("news_id", trim(col("news_id")))
        .withColumn("canonical_url", trim(col("canonical_url")))
        .withColumn("thumbnail", trim(col("thumbnail")))
    )

    # ============================================================
    # 9. BUSINESS RULES
    # ============================================================

    silver_news_df = (
        silver_news_df
        .filter(col("ticker").isNotNull())
        .filter(col("title").isNotNull())
        .filter(col("event_time").isNotNull())
    )

    # ============================================================
    # 10. CHECK DUPLICATE NEWS IDS BEFORE REMOVING THEM
    # ============================================================
    # DEVELOPMENT / DEBUG CHECK
    # Expensive aggregation. Not required for every Airflow run.

    # print("=== DUPLICATE NEWS IDS BEFORE DEDUPLICATION ===")

    # silver_news_df.groupBy("news_id") \
    #     .count() \
    #     .filter(
    #         col("news_id").isNotNull() &
    #         (col("count") > 1)
    #     ) \
    #     .orderBy(
    #         col("count").desc()
    #     ) \
    #     .show(50, truncate=False)

    # ============================================================
    # 11. REMOVE GENUINE DUPLICATE NEWS RECORDS
    # ============================================================
    # KEEP THIS ACTIVE.
    # This is an actual Silver transformation, not just a check.

    # Historical records have NULL news_id.
    # Therefore only non-null news_ids are deduplicated.

    window_spec = Window.partitionBy("news_id").orderBy(
        col("ingestion_timestamp").desc()
    )

    silver_news_df = (
        silver_news_df
        .withColumn(
            "row_num",
            row_number().over(window_spec)
        )
        .filter(
            col("news_id").isNull() |
            (col("row_num") == 1)
        )
        .drop("row_num")
    )

    # ============================================================
    # 12. VERIFY DUPLICATES AFTER DEDUPLICATION
    # ============================================================
    # DEVELOPMENT / DEBUG CHECK
    # Not required during every Airflow run.

    # print("=== DUPLICATE NEWS IDS AFTER DEDUPLICATION ===")

    # silver_news_df.groupBy("news_id") \
    #     .count() \
    #     .filter(
    #         col("news_id").isNotNull() &
    #         (col("count") > 1)
    #     ) \
    #     .orderBy(
    #         col("count").desc()
    #     ) \
    #     .show(50, truncate=False)

    # ============================================================
    # 13. ADD DATE PARTITIONS
    # ============================================================

    silver_news_df = (
        silver_news_df
        .withColumn("year", year("event_time"))
        .withColumn("month", month("event_time"))
        .withColumn("day", dayofmonth("event_time"))
    )

    # ============================================================
    # 14. SOURCE COUNT
    # ============================================================
    # DEVELOPMENT / DEBUG CHECK
    # Not required for every Airflow run.

    # print("=== ROW COUNT BY SOURCE ===")

    # silver_news_df.groupBy(
    #     "source_system"
    # ).count().show()

    # ============================================================
    # 15. FINAL SILVER ROW COUNT
    # ============================================================
    # DEVELOPMENT / DEBUG CHECK
    # Full count can be expensive on 1.4M+ rows.
    # The write below already executes the Spark job.

    # print(
    #     "SILVER NEWS ROW COUNT:",
    #     silver_news_df.count()
    # )

    # ============================================================
    # 16. WRITE SILVER
    # ============================================================

    silver_news_df.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(
            "s3a://market-intelligence-platform/silver/news/"
        )

    print("Silver news written successfully.")


if __name__ == "__main__":
    run()