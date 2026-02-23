# talend_cloud_job_dynamic DAG ☁️

This DAG orchestrates Talend Cloud jobs by triggering executions and monitoring logs and component metrics.

It uses Airflow’s decorator API and polls Talend’s REST endpoints dynamically.

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