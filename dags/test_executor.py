from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

def hello():
    print("HELLO FROM AIRFLOW")

with DAG(
    dag_id="test_executor",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    PythonOperator(
        task_id="hello",
        python_callable=hello,
    )