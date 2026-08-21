# Airflow Debugging Flow

This flow can be used whenever an Airflow DAG or task fails.

```mermaid
flowchart TD

    A["Something Failed"] --> B{"Is DAG visible?"}

    B -- "No" --> C["airflow dags list"]
    C --> D["airflow dags list-import-errors"]
    D --> B

    B -- "Yes" --> E["Check DAG Structure"]
    E --> F["airflow dags show <dag_id>"]

    F --> G["Trigger DAG"]
    G --> H["airflow dags trigger <dag_id>"]

    H --> I["Check DAG Run"]
    I --> J["airflow dags list-runs -d <dag_id>"]

    J --> K["Check Task States"]
    K --> L["airflow tasks states-for-dag-run <dag_id> <run_id>"]

    L --> M["Find Failed Task"]
    M --> N["Check Task Logs"]

    N --> O["Test Failed Task"]
    O --> P["airflow tasks test <dag_id> <task_id>"]

    P --> Q["Fix the Issue"]
    Q --> R["Clear Failed Task"]
    R --> S["Let Airflow Rerun"]

    S --> T["Verify DAG Run"]
```

---

## Quick Command Reference

### 1. Check if DAG is visible

```bash
airflow dags list
```

If the DAG is missing:

```bash
airflow dags list-import-errors
```

---

### 2. Check DAG structure

```bash
airflow dags show <dag_id>
```

---

### 3. Trigger the DAG

```bash
airflow dags trigger <dag_id>
```

---

### 4. Find the DAG run

```bash
airflow dags list-runs -d <dag_id>
```

---

### 5. Check task states

```bash
airflow tasks states-for-dag-run \
<dag_id> \
"<run_id>"
```

---

### 6. Debug the failed task

Check the Airflow task logs first.

Then test the individual task:

```bash
airflow tasks test <dag_id> <task_id>
```

Example:

```bash
airflow tasks test snowflake_gold_load load_market_events
```

---

### 7. Fix and rerun

After fixing the underlying issue:

1. Clear the failed task.
2. Let Airflow rerun the task.
3. Check the DAG run again.
4. Verify that all tasks complete successfully.

---

## Debugging Principle

> **DAG → Run → Task → Logs → Root Cause → Fix → Rerun**

This keeps debugging systematic instead of repeatedly triggering the entire pipeline and guessing what went wrong.
