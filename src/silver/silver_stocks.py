from src.config import spark

stocks_df=spark.read.parquet(
    "s3a://market-intelligence-platform/bronze/stocks/date=*/"
) 

stocks_df.printSchema()

stocks_df.show(10,truncate=False)