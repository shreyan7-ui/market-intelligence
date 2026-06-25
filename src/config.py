from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("market-intelligence-platform")
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
    )
    .getOrCreate()
)