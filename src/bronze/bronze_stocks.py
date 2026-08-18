from datetime import datetime
from src.config import get_spark
from pyspark.sql.functions import current_timestamp, lit

def run():

    spark = get_spark()

    TODAY = datetime.today().strftime("%Y-%m-%d")

    stocks_df = (
        spark.read
        .option("multiline", "true")
        .json(
            f"s3a://market-intelligence-platform/raw/stocks/date={TODAY}/stock_prices.json"
        )
    )

    stocks_df = (
        stocks_df
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_system", lit("Yahoo"))
    )

    stocks_df.write \
        .mode("overwrite") \
        .parquet(
            f"s3a://market-intelligence-platform/bronze/stocks/date={TODAY}/"
        )

if __name__ == "__main__":
    run()