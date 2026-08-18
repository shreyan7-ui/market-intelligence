from src.config import get_spark
from pyspark.sql.functions import lit,to_timestamp,col,count,when,upper,trim,dayofmonth,month,year,to_date

def run():
    
   
    
    spark = get_spark()
    
    stocks_df=spark.read.parquet(
        "s3a://market-intelligence-platform/bronze/stocks/date=*/"
    )

    stocks_df.printSchema()

    stocks_df = (stocks_df
        .withColumnRenamed("extracted_date", "event_time")
        .withColumnRenamed("Close", "close_price")
        .withColumnRenamed("Open", "open_price")
        .withColumnRenamed("High", "high_price")
        .withColumnRenamed("Low", "low_price")
        .withColumnRenamed("Volume", "volume")
        .withColumnRenamed("Dividends", "dividends")
        .withColumnRenamed("Stock Splits", "stock_splits")
    )

    stocks_df = stocks_df.withColumn("event_time",to_timestamp("event_time"))
    
    stocks_df \
    .filter(
        (col("ticker") == "GOOG") &
        (to_date(col("event_time")) == "2026-07-13")
    ) \
    .count()
    
    stocks_df=(stocks_df.withColumn("ticker", upper(trim(col("ticker"))))
                        # .withColumn("Close",trim(col("Close")))
                        # .withColumn("High",trim(col("High")))
                        # .withColumn("Low",trim(col("Low")))
                        # .withColumn("Open",trim(col("Open")))
                        # .withColumn("Volume",trim(col("Volume")))
    )


    stocks_df.select([
        count(when(col(c).isNull(), c)).alias(c)
        for c in stocks_df.columns
    ]).show()

    print("duplicate check : ",stocks_df.groupBy("ticker","event_time").count().filter("count>1").show())

    stocks_df.filter( col("ticker").isNotNull() &
        col("event_time").isNotNull() &
        col("open_price").isNotNull() &
        col("high_price").isNotNull() &
        col("low_price").isNotNull() &
        col("close_price").isNotNull() &
        col("volume").isNotNull()
    ).show(truncate=False)


    stocks_df = (stocks_df
        .withColumn("year", year("event_time"))
        .withColumn("month", month("event_time"))
        .withColumn("day", dayofmonth("event_time"))
    )

    stocks_df.write \
        .mode("overwrite") \
        .partitionBy("year","month") \
        .parquet(f"s3a://market-intelligence-platform/silver/stocks/")
    # when integrating airflow switch to append mode 


    stocks_df.show(truncate=False)

if __name__ == "__main__":
    run()