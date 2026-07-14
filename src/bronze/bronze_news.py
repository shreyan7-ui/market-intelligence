from src.config import spark
from pyspark.sql.functions import lit,current_timestamp
from datetime import datetime

TODAY = datetime.today().strftime("%Y-%m-%d")

historical_df = spark.read.parquet(
"s3a://market-intelligence-platform/raw/historical/cleaned_historical_news.parquet"
)

news_df = (spark.read.option("multiline","true")
    .json("s3a://market-intelligence-platform/raw/news/date=*/market_news.json"))

historical_df.printSchema()
news_df.printSchema()

historical_df = (
    historical_df
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_system", lit("Kaggle"))
)

news_df = (
    news_df
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_system", lit("Yahoo"))
)

historical_df.write \
.mode("overwrite") \
.parquet(
"s3a://market-intelligence-platform/bronze/historical/"
)

news_df.write \
.mode("overwrite") \
.parquet(
    f"s3a://market-intelligence-platform/bronze/news/date={TODAY}/"
)

# historical_df.show(10,truncate=False)
# news_df.show(2,truncate=False)
# historical_df.limit(5).show(truncate=False)
# print("Count of rows in historical_df:", historical_df.count())
# historical_df.describe().show()