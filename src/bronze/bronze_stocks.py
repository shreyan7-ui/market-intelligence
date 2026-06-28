from src.config import spark

stocks_df=(spark.read.option("multiline","true")
           .json("s3a://market-intelligence-platform/raw/stocks/date=*/stock_prices.json"))

stocks_df.printSchema()
stocks_df.show(10)