# talend_cloud_job_dynamic DAG ☁️

This DAG orchestrates Talend Cloud jobs by triggering executions and monitoring logs and component metrics.

It uses Airflow’s decorator API and polls Talend’s REST endpoints dynamically.


---

## **Main Idea**

This Airflow DAG is designed to **orchestrate a Talend Cloud job dynamically**. It triggers a Talend job, monitors its execution in real time, and fetches **logs and component-level metrics** for observability. The goal is to automate the execution of a Talend job while keeping track of its status and performance for downstream processing or analysis.

---

## **Step-by-Step Explanation**

1. **DAG Definition**

* The DAG is named `"talend_cloud_job_dynamic"` and is scheduled to run **manually** (`schedule=None`) with no catchup.
* It is tagged for clarity: `"talend"`, `"cloud"`, and `"etl"`.
* Default retry settings are applied: **3 retries** with a **5-minute delay** in case of failures.

---

2. **`trigger_job` Task**

* This task is responsible for **starting a Talend Cloud job** using the Talend API.
* It retrieves Talend credentials and the host from an Airflow connection named `"talend_cloud"`.
* Sends a POST request to Talend’s `/processing/executions` endpoint with the job ID (`executable_id`) in the payload.
* If the job is successfully triggered, it captures the **execution ID** from the response. This ID is dynamic and will be used by the monitoring task to track the job.

**Key point:** This task allows **dynamic orchestration**, as the returned execution ID is not static and can be passed to other tasks.

---

3. **`monitor_job` Task**

* This task **monitors the Talend job in real time** using the execution ID returned by `trigger_job`.

* Two main things happen here:

  **a. Fetch Logs**

  * Continuously polls the `/monitoring/executions/{execution_id}/logs` endpoint to get the latest logs.
  * Logs include timestamps, severity, and messages, providing insight into the job's execution status.

  **b. Fetch Component Metrics**

  * Polls `/monitoring/observability/executions/{execution_id}/component` to get **component-level metrics**.
  * Metrics include:

    * Component name (`connector_label`)
    * Execution duration (`component_execution_duration_milliseconds`)
    * Number of rows processed (`component_connection_rows_total`)
  * These metrics give **observability** into how each component of the Talend job is performing.

* The task keeps polling until either the **timeout is reached** or the job finishes.

* Collected logs and metrics are returned as a dictionary for **downstream tasks or storage**.

---

4. **DAG Flow**

* The workflow is simple and linear:

  1. **Trigger the Talend job** and get the execution ID.
  2. **Monitor the job** using that execution ID to collect logs and metrics dynamically.

**Summary:** This DAG acts as a **dynamic executor and observer** for Talend Cloud jobs, allowing Airflow to trigger jobs, monitor execution, and collect detailed metrics without manual intervention. It’s essentially an **automated ETL orchestration with built-in observability**.




## Configuration

The DAG is defined with **default_args** and the `@dag` decorator to manage retries and scheduling.

| Parameter | Value | Description |
| --- | --- | --- |
| dag_id | `"talend_cloud_job_dynamic"` | Unique identifier for the DAG |
| start_date | `2026-02-02` | Earliest date to start scheduling |
| schedule | `None` | Triggered manually (no schedule) |
| catchup | `False` | Do not backfill past runs |
| default_args | See table below | Retry behavior for tasks |
| tags | `["talend","cloud","etl"]` | Labels for grouping in the Airflow UI |


**default_args**

| Key | Value |
| --- | --- |
| retries | `3` |
| retry_delay | `timedelta(minutes=5)` |


## Tasks

This DAG defines two core tasks using the `@task` decorator.

- **trigger_job**: Initiates a Talend Cloud job and returns its execution ID.
- **monitor_job**: Polls execution logs and observability metrics until completion or timeout.

### trigger_job

```python
@task()
def trigger_job(executable_id):
    """Trigger a Talend Cloud job and return the execution ID."""
    conn = BaseHook.get_connection("talend_cloud")
    headers = {
        "Authorization": f"Bearer {conn.password}",
        "Content-Type": "application/json"
    }
    payload = {"executable": executable_id}
    url = f"{conn.host}/processing/executions"
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    if response.status_code not in (200, 201):
        raise Exception(f"Failed to trigger Talend job: {response.text}")
    execution_id = response.json().get("executionId") or response.json().get("id")
    print(f"✅ Talend job triggered. Execution ID: {execution_id}")
    return execution_id
```

- **Inputs**:
- `executable_id` (str): Identifier of the Talend job to execute.
- **Behavior**:
- Retrieves the `talend_cloud` Airflow Connection.
- Sends a POST to `/processing/executions`.
- Validates response and extracts `executionId`.
- Logs success and returns the ID.

### monitor_job

```python
@task()
def monitor_job(execution_id: str, poll_interval=10, timeout=50):
    """Monitor logs and fetch component metrics for a Talend execution."""
    conn = BaseHook.get_connection("talend_cloud")
    headers = {"Authorization": f"Bearer {conn.password}"}
    logs_url = f"{conn.host}/monitoring/executions/{execution_id}/logs"
    metrics_url = f"{conn.host}/monitoring/observability/executions/{execution_id}/component"
    start_time = time.time()
    all_logs, all_metrics = [], []

    while True:
        # Fetch logs
        log_resp = requests.get(logs_url, headers=headers, params={"count": 50, "order": "DESC"})
        if log_resp.status_code != 200:
            raise Exception(f"Failed fetching logs: {log_resp.text}")
        logs = log_resp.json().get("data", [])
        if logs:
            all_logs.extend(logs)
            print("\n[LOGS] Latest entries:")
            for log in logs[-30:]:
                ts, sev, msg = log["logTimestamp"], log["severity"], log["logMessage"]
                print(f"[{ts}] {sev}: {msg}")

        # Fetch metrics
        metrics_resp = requests.get(metrics_url, headers=headers, params={"limit": 200, "offset": 0})
        if metrics_resp.status_code == 200:
            items = metrics_resp.json().get("metrics", {}).get("items", [])
            if items:
                all_metrics.extend(items)
                table = PrettyTable()
                table.field_names = ["Component", "Duration ms", "Rows Processed"]
                for item in items:
                    table.add_row([
                        item.get("connector_label", "N/A"),
                        item.get("component_execution_duration_milliseconds", "N/A"),
                        item.get("component_connection_rows_total", "N/A")
                    ])
                print("\n[METRICS] Component-level Observability:")
                print(table)
        else:
            print(f"Failed fetching metrics: {metrics_resp.text}")

        # Exit on timeout
        if time.time() - start_time > timeout:
            print("Timeout reached, stopping monitoring.")
            break

        time.sleep(poll_interval)

    return {"logs": all_logs, "metrics": all_metrics}
```

**Parameters**

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| execution_id | str | — | Talend execution identifier |
| poll_interval | int | `10` | Seconds to wait between polls |
| timeout | int | `50` | Maximum monitoring duration in seconds |


**Metrics Table Mapping**

| Display | JSON Key | Notes |
| --- | --- | --- |
| Component | `connector_label` | Talend component name |
| Duration ms | `component_execution_duration_milliseconds` | Execution time in milliseconds |
| Rows Processed | `component_connection_rows_total` | Total rows processed by component |


## Execution Flow

1. **Trigger**: `trigger_job("69970ad40705b452419695c9")`
2. **Monitor**: Pass returned ID to `monitor_job`
3. **Collect**: Logs and metrics are returned for downstream tasks

```python
execution_id = trigger_job("69970ad40705b452419695c9")
monitor_job(execution_id)
```

## Dependencies

- **Airflow**
- `@dag`, `@task` decorators from `airflow.decorators`
- `BaseHook` from `airflow.hooks.base` for connection retrieval
- **Third-Party Libraries**
- `requests` for HTTP calls
- `json` for payload serialization
- `PrettyTable` for tabular metric output
- **Standard Library**
- `datetime`, `timedelta` for scheduling
- `time` for polling and timeout logic

## Integration with the Project

This DAG complements other ETL workflows (e.g., `n8n_etl_with_monitoring`) by providing a **Talend Cloud** integration. It aligns with the project’s pattern of:

- Triggering external workflows via REST APIs
- Polling and logging execution status
- Formatting observability data for Airflow logs

The DAG resides in `dags/` and uses Airflow Connections configured in the UI or `config/airflow.cfg`.