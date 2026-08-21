Something failed
      ↓
Is DAG visible?
      │
      └── NO → airflow dags list
                    ↓
              airflow dags list-import-errors
      ↓
Is DAG structure correct?
      ↓
airflow dags show <dag_id>
      ↓
Trigger DAG
      ↓
airflow dags trigger <dag_id>
      ↓
Check DAG run
      ↓
airflow dags list-runs -d <dag_id>
      ↓
Check task states
      ↓
airflow tasks states-for-dag-run <dag_id> "<run_id>"
      ↓
Find failed task
      ↓
Check task logs
      ↓
airflow tasks test <dag_id> <task_id>
      ↓
Fix issue
      ↓
Clear failed task
      ↓
Let Airflow rerun