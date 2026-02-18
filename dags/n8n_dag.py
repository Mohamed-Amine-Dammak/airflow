from airflow.sdk import dag, task
from airflow.sdk.bases.hook import BaseHook
from datetime import datetime, timedelta
import requests
import time

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="n8n_etl_with_monitoring",
    start_date=datetime(2026, 2, 2),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["n8n", "ETL", "monitoring"],
)
def n8n_etl_with_monitoring():

    # 🔹 CHANGE THIS
    WORKFLOW_ID = "UoeB8YVbB1TeBUXb"
    WEBHOOK_PATH = "d4727c48-61aa-4d22-a785-725bc3eff140"

    @task()
    def trigger_n8n(file_name="input.csv", environment="dev"):
        """
        Trigger n8n workflow via webhook.
        """
        conn = BaseHook.get_connection("n8n_local")

        webhook_url = f"{conn.host}/webhook-test/{WEBHOOK_PATH}"
        headers = {"Authorization": f"Bearer {conn.password}"}
        params = {"file": file_name, "env": environment}

        response = requests.get(webhook_url, headers=headers, params=params)
        response.raise_for_status()

        print("✅ n8n webhook triggered successfully")
        return True  # We do NOT expect executionId

    @task()
    def monitor_n8n(_, poll_interval=5, timeout=300):
        """
        Poll latest execution for workflow and fetch logs.
        """
        conn = BaseHook.get_connection("n8n_local")
        # The n8n Public API requires X-N8N-API-KEY.
        # Ensure the Airflow connection password contains a valid API Key (starts with "n8n_api_..."), NOT a JWT.
        headers = {"X-N8N-API-KEY": conn.password}
        print(f"DEBUG: Connection Type: {conn.conn_type}")
        print(f"DEBUG: Host: {conn.host}")
        if conn.password:
            print(f"DEBUG: Password/Key length: {len(conn.password)}")
            print(f"DEBUG: Key starts with: {conn.password[:4]}...")
        else:
            print("DEBUG: No password/key found in connection.")


        executions_url = (
            f"{conn.host}/api/v1/executions"
            f"?workflowId={WORKFLOW_ID}&limit=1"
        )

        start_time = time.time()

        while True:
            response = requests.get(executions_url, headers=headers)
            response.raise_for_status()

            executions = response.json().get("data", [])

            if not executions:
                print("No execution found yet, waiting...")
                time.sleep(poll_interval)
                continue

            execution = executions[0]
            execution_id = execution.get("id")
            status = execution.get("status")

            print(f"📌 Execution ID: {execution_id}")
            print(f"📊 Status: {status}")

            if status in ["success", "error", "failed", "canceled"]:
                print("\n===== n8n NODE LOGS =====")

                run_data = (
                    execution
                    .get("data", {})
                    .get("resultData", {})
                    .get("runData", {})
                )

                for node_name, runs in run_data.items():
                    print(f"\n🔹 Node: {node_name}")
                    for run in runs:
                        if "error" in run:
                            print("❌ Error:", run["error"])
                        if "data" in run:
                            print("Output:", run["data"])

                if status != "success":
                    raise Exception("❌ n8n workflow failed")

                print("✅ n8n workflow completed successfully")
                return status

            if time.time() - start_time > timeout:
                raise TimeoutError("Timeout waiting for n8n execution")

            print(f"Waiting {poll_interval}s before next check...")
            time.sleep(poll_interval)

    # DAG flow
    triggered = trigger_n8n()
    monitor_n8n(triggered)

dag = n8n_etl_with_monitoring()
