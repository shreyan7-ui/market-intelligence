import os

from dotenv import load_dotenv

load_dotenv()

# Windows-only Hadoop configuration
if os.name == "nt":
    # Lenovo PC
    os.environ["HADOOP_HOME"] = r"C:\Users\LENOVO\hadoop-3.5.0"
    os.environ["PATH"] += os.pathsep + r"C:\Users\LENOVO\hadoop-3.5.0\bin"

    # Office PC
    # os.environ["HADOOP_HOME"] = r"C:\hadoop-3.5.0"
    # os.environ["PATH"] += os.pathsep + r"C:\hadoop-3.5.0\bin"


def get_spark():
    """
    Create and return a SparkSession.
    Spark is initialized only when this function is called.
    """
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("market-intelligence-platform")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.4.2,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        )
        .config("spark.sql.shuffle.partitions", "12")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.path.style.access", "false")
        .getOrCreate()
    )

    print("AWS Region:", os.getenv("AWS_DEFAULT_REGION"))
    print("Spark Version:", spark.version)
    print("SparkSession created successfully!")

    return spark