from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import requests
import json
import time

# Default args for retries, emails, etc.
default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="talend_cloud_job_dynamic",
    start_date=datetime(2026, 2, 2),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["talend", "cloud", "etl"]
)
def talend_cloud_job_dag():

    @task()
    def trigger_job(executable_id):
        """
        Trigger a Talend Cloud job and return the execution ID dynamically.
        """
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
        return execution_id  # ← dynamic output

    @task()
    def monitor_job(execution_id: str, poll_interval=10, timeout=50):
        """
        Monitor Talend Cloud job execution logs AND fetch component metrics dynamically
        using the execution ID.
        """
        # Get connection info from Airflow
        conn = BaseHook.get_connection("talend_cloud")
        headers = {"Authorization": f"Bearer {conn.password}"}

        # URLs
        logs_url = f"{conn.host}/monitoring/executions/{execution_id}/logs"
        metrics_url = f"{conn.host}/monitoring/observability/executions/{execution_id}/component"

        start_time = time.time()
        all_logs = []
        all_metrics = []

        while True:
            # -----------------------------
            # FETCH LOGS
            # -----------------------------
            log_resp = requests.get(logs_url, headers=headers, params={"count": 50, "order": "DESC"})
            if log_resp.status_code != 200:
                raise Exception(f"Failed fetching logs: {log_resp.text}")

            logs = log_resp.json().get("data", [])
            if logs:
                all_logs.extend(logs)
                print("\n[LOGS] Latest entries:")
                for log in logs[-30:]:
                    ts = log.get("logTimestamp")
                    sev = log.get("severity")
                    msg = log.get("logMessage")
                    print(f"[{ts}] {sev}: {msg}")

            # -----------------------------
            # FETCH METRICS
            # -----------------------------
            metrics_resp = requests.get(metrics_url, headers=headers, params={"limit": 200, "offset": 0})
            if metrics_resp.status_code == 200:
                metrics_data = metrics_resp.json()
                items = metrics_data.get("metrics", {}).get("items", [])
                if items:
                    all_metrics.extend(items)
                    # Pretty print metrics table
                    table = PrettyTable()
                    table.field_names = ["Component", "Duration ms", "Rows Processed"]
                    for item in items:
                        table.add_row([
                            item.get('connector_label', 'N/A'),
                            item.get('component_execution_duration_milliseconds', 'N/A'),
                            item.get('component_connection_rows_total', 'N/A')
                        ])
                    print("\n[METRICS] Component-level Observability:")
                    print(table)
            else:
                print(f"Failed fetching metrics: {metrics_resp.text}")

            # -----------------------------
            # Exit after timeout
            # -----------------------------
            if time.time() - start_time > timeout:
                print("Timeout reached, stopping monitoring.")
                break

            time.sleep(poll_interval)

        # Return collected data for downstream tasks
        return {"logs": all_logs, "metrics": all_metrics}
    
    # DAG flow: trigger -> monitor
        # -------------------------
    # JOB 1
    # -------------------------
    execution_id_1 = trigger_job("69970ad40705b452419695c9")
    monitor_1 = monitor_job(execution_id_1)

    # -------------------------
    # JOB 2
    # -------------------------
    execution_id_2 = trigger_job("69a7fe83225ee7bab678992b")
    monitor_2 = monitor_job(execution_id_2)

    # -------------------------
    # DEPENDENCY MANAGEMENT
    # -------------------------
    monitor_1 >> execution_id_2

dag = talend_cloud_job_dag()
