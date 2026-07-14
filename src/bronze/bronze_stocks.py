from src.config import spark
from pyspark.sql.functions import current_timestamp, lit
from datetime import datetime

TODAY = datetime.today().strftime("%Y-%m-%d")

stocks_df=(spark.read.option("multiline","true")
           .json("s3a://market-intelligence-platform/raw/stocks/date=*/stock_prices.json"))

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
# stocks_df.printSchema()
# stocks_df.show(4)
# stocks_df.select("MSFT","NVDA").show()
# print(stocks_df.schema)
# print("count of rows :",stocks_df.count())