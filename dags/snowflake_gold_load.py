import sys

sys.path.append("/mnt/e/market analysis")

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from datetime import datetime

from dags.test_snowflake import (
    load_market_events,
    load_stock_analytics,
    load_news_analytics,
)

with DAG(
    dag_id="snowflake_gold_load",
    description="Load Gold Parquet data from S3 into Snowflake",
    start_date=datetime(2026, 8, 3),
    schedule=None,
    catchup=False,
    tags=["market-intelligence", "snowflake", "gold"],
) as dag:

    market_events_task = PythonOperator(
        task_id="load_market_events",
        python_callable=load_market_events,
    )

    stock_analytics_task = PythonOperator(
        task_id="load_stock_analytics",
        python_callable=load_stock_analytics,
    )

    news_analytics_task = PythonOperator(
        task_id="load_news_analytics",
        python_callable=load_news_analytics,
    )

    market_events_task
    stock_analytics_task
    news_analytics_task
