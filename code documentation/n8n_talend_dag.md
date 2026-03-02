Absolutely! We can use **Mermaid** syntax to create a DAG flow diagram directly in Markdown. This works well in Confluence, GitHub, or other Markdown viewers that support Mermaid. Here's an updated version of your documentation including a graph:

````markdown
# Master ETL Pipeline DAG Documentation

## Overview
The **`master_etl_pipeline`** DAG orchestrates the execution of multiple ETL processes in sequence:

1. **n8n ETL Workflow** 
2. **Talend Cloud Job**

It ensures sequential execution and monitors the success of each task.

---

## DAG Flow Diagram

```mermaid
flowchart TD
    A[Trigger n8n DAG] --> B[Trigger Talend DAG]
    subgraph "n8n Workflow"
        A
    end
    subgraph "Talend Workflow"
        B
    end
````

**Explanation:**

* `Trigger n8n DAG` starts the n8n ETL process.
* Once successful, `Trigger Talend DAG` runs the Talend Cloud job.
* Each workflow is monitored internally within its DAG.

---

## DAG Details

* **DAG ID:** `master_etl_pipeline`
* **Start Date:** 2026-02-02
* **Schedule:** None (manual trigger)
* **Catchup:** False
* **Retries:** 3
* **Retry Delay:** 5 minutes
* **Tags:** `master`, `orchestration`

---

## Tasks

### 1️⃣ `trigger_n8n_dag`

* **Type:** `TriggerDagRunOperator`
* **Triggered DAG:** `n8n_etl_with_monitoring`
* **Purpose:** Triggers the n8n ETL workflow and waits for completion.
* **Key Parameters:**

  * `wait_for_completion=True`
  * `poke_interval=10`
  * `allowed_states=["success"]`
  * `failed_states=["failed"]`

### 2️⃣ `trigger_talend_dag`

* **Type:** `TriggerDagRunOperator`
* **Triggered DAG:** `talend_cloud_job_dynamic`
* **Purpose:** Triggers Talend Cloud job after n8n completes.
* **Key Parameters:**

  * `wait_for_completion=True`
  * `poke_interval=10`
  * `allowed_states=["success"]`
  * `failed_states=["failed"]`

---

## Notes

* Sequential orchestration ensures Talend job only starts after n8n success.
* Each workflow’s detailed monitoring is handled inside its DAG.
* DAG IDs must match exactly what is defined in Airflow.

---

## References

* [Airflow TriggerDagRunOperator Documentation](https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/trigger-dag.html)
* [Airflow DAG Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)

```

---


```
