import pandas as pd
df=pd.read_parquet("data/raw/historical/cleaned_historical_news.parquet")
print (df.head(10))                    