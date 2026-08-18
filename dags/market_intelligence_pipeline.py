import sys
sys.path.append("/mnt/e/market analysis")

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

# from src.ingestion.ingest_his_data import run as historical_news_run
from src.ingestion.ingest_live_data import run as live_ingestion_run

from src.bronze.bronze_news import run as bronze_news_run
from src.bronze.bronze_stocks import run as bronze_stocks_run

from src.silver.silver_news import run as silver_news_run
from src.silver.silver_stocks import run as silver_stocks_run

from src.gold.gold_market_events import run as market_events_run
from src.gold.gold_stock_analytics import run as stock_analytics_run
from src.gold.gold_news_analytics import run as news_analytics_run


with DAG(
    dag_id="market_intelligence_pipeline",
    description="End-to-End Market Intelligence Data Pipeline",
    start_date=datetime(2026, 8, 3),
    schedule=None,
    catchup=False,
    tags=["market-intelligence", "data-engineering"],
) as dag:

    # historical_news_task = PythonOperator(
    #     task_id="historical_news_ingestion",
    #     python_callable=historical_news_run,
    # )
    live_ingestion_task = PythonOperator(
        task_id="live_market_ingestion",
        python_callable=live_ingestion_run,
    )
    bronze_news_task = PythonOperator(
        task_id="bronze_news",
        python_callable=bronze_news_run,
    )
    bronze_stocks_task = PythonOperator(
        task_id="bronze_stocks",
        python_callable=bronze_stocks_run,
    )
    silver_news_task = PythonOperator(
        task_id="silver_news",
        python_callable=silver_news_run,
    )
    silver_stocks_task = PythonOperator(
        task_id="silver_stocks",
        python_callable=silver_stocks_run,
    )
    gold_market_events_task = PythonOperator(
        task_id="gold_market_events",
        python_callable=market_events_run,
    )
    gold_stock_analytics_task = PythonOperator(
        task_id="gold_stock_analytics",
        python_callable=stock_analytics_run,
    )
    gold_news_analytics_task = PythonOperator(
        task_id="gold_news_analytics",
        python_callable=news_analytics_run,
    )

    live_ingestion_task

    live_ingestion_task >> bronze_news_task
    live_ingestion_task >> bronze_stocks_task

    bronze_news_task >> silver_news_task
    bronze_stocks_task >> silver_stocks_task

    [silver_news_task, silver_stocks_task] >> gold_market_events_task

    gold_market_events_task >> [
        gold_stock_analytics_task,
        gold_news_analytics_task,
    ]