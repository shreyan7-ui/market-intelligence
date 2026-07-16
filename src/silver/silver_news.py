from src.config import spark
from pyspark.sql.functions import lit,to_timestamp,col

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

silver_news_df.printSchema()
silver_news_df.show(5, truncate=False)

print("Rows:", silver_news_df.count())
# historical_df.printSchema()
# news_df.printSchema()

# historical_df.show(10,truncate=False)
# news_df.show(10,truncate=False)