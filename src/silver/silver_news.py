from pyspark.sql.functions import lit,to_timestamp,col,count,when,upper,trim,dayofmonth,month,year
from src.config import get_spark

def run():
    

    
    spark = get_spark()
    
    historical_cols=[
        "summary",
        "provider",
        "news_id",
        "canonical_url",
        "thumbnail"
    ]
    historical_df=spark.read.parquet(
        "s3a://market-intelligence-platform/bronze/historical/"
    ) 

    news_df=spark.read.parquet(
        "s3a://market-intelligence-platform/bronze/news/date=*/"
    ) 

    # rename cols
    historical_df=(historical_df
            .withColumnRenamed("stock","ticker")
            .withColumnRenamed("date","event_time")
    )

    news_df = (news_df
        .withColumnRenamed("stock", "ticker")
        .withColumnRenamed("pub_date", "event_time")
    )
    # add col
    historical_df=(historical_df
        .withColumn("summary", lit(None))
        .withColumn("provider", lit(None))
        .withColumn("news_id", lit(None))
        .withColumn("canonical_url", lit(None))
        .withColumn("thumbnail", lit(None))        
    )
    # reorder his col
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
        "ingestion_timestamp"
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
        "ingestion_timestamp"
    )
    # change date from string to timestamp
    # almost all raw data comes as string format
    historical_df = historical_df.withColumn(
        "event_time",
        to_timestamp("event_time","yyyy-MM-dd")
    )
    news_df = news_df.withColumn(
        "event_time",
        to_timestamp("event_time")
    )

    for column in historical_cols:
        historical_df=historical_df.withColumn(column,col(column).cast("string"))

    silver_news_df = historical_df.unionByName(news_df)

    # count nulls
    silver_news_df.select([
        count(when(col(c).isNull(), c)).alias(c)
        for c in news_df.columns
    ]).show()

    # duplicate check
    silver_news_df.groupBy("news_id").count().filter("count >1").show()

    # now investigate the duplicates found\
    ids = [
        "3c8a1d2e-a15c-3a3",
        "bcaad1bf-7c68-41d",
        "1fdf89b7-660b-3ec",
        "1d9c975c-569a-395"
    ]
    condition = None

    for i in ids:
        if condition is None:
            condition = col("news_id").contains(i)
        else:
            condition = condition | col("news_id").contains(i)

    silver_news_df.filter(condition).show(truncate=False)

    # chcek if duplicate rows have duplicate data too
    silver_news_df.filter(col("news_id").isNotNull()) \
        .groupBy("news_id", "ticker") \
        .agg(count("*").alias("cnt")) \
        .filter(col("cnt") > 1) \
        # .show(truncate=False)

    # remove trailing spaces and convert ticker to upper case
    silver_news_df = (silver_news_df
        .withColumn("ticker", upper(trim(col("ticker"))))
        .withColumn("title", trim(col("title")))
        .withColumn("summary", trim(col("summary")))
        .withColumn("provider", trim(col("provider")))
    )

    # checks other business rules and filters out the rows that don't meet the criteria
    silver_news_df = (silver_news_df
        .filter(col("ticker").isNotNull())
        .filter(col("title").isNotNull())
        .filter(col("event_time").isNotNull())
    )

    silver_news_df=(silver_news_df
            .withColumn("year", year("event_time"))
            .withColumn("month", month("event_time"))
            .withColumn("day", dayofmonth("event_time")))

    silver_news_df.groupBy("source_system").count()

    # save to aws
    silver_news_df.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet("s3a://market-intelligence-platform/silver/news/")
    # when integrating airflow switch to append mode 


    # silver_news_df.show()
    # news_df.show()

    # silver_news_df.printSchema()
    # silver_news_df.show(5, truncate=False)

    # print("Rows:", silver_news_df.count())
    # historical_df.printSchema()
    # news_df.printSchema()

    # historical_df.show(10,truncate=False)
    # news_df.show(10,truncate=False)
    
if __name__ == "__main__":
    run()