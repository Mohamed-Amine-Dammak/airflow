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
    def monitor_job(execution_id: str, poll_interval=10, timeout=30):
        """
        Monitor Talend Cloud job execution logs dynamically using the execution ID.
        """
        conn = BaseHook.get_connection("talend_cloud")
        headers = {"Authorization": f"Bearer {conn.password}"}
        url = f"{conn.host}/monitoring/executions/{execution_id}/logs"

        start_time = time.time()
        while True:
            response = requests.get(url, headers=headers, params={"count": 50, "order": "DESC"})
            if response.status_code != 200:
                raise Exception(f"Failed fetching logs: {response.text}")

            logs = response.json().get("data", [])
            if logs:
                for log in logs[-30:]:  # print last 10 logs
                    ts = log.get("logTimestamp")
                    sev = log.get("severity")
                    msg = log.get("logMessage")
                    print(f"[{ts}] {sev}: {msg}")

            # Exit after timeout
            if time.time() - start_time > timeout:
                print("Timeout reached, stopping monitoring.")
                break

            time.sleep(poll_interval)

    # DAG flow: trigger -> monitor
    execution_id = trigger_job("69970ad40705b452419695c9")  # Pass the Talend job ID here
    monitor_job(execution_id)

dag = talend_cloud_job_dag()
