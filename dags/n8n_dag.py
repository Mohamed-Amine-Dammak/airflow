from airflow.sdk import dag, task
from airflow.sdk.bases.hook import BaseHook
from datetime import datetime, timedelta
from airflow.exceptions import AirflowException
import requests
import time
import json


default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="n8n_etl_multi_workflow_with_monitoring",
    start_date=datetime(2026, 2, 2),
    schedule=None,           # or "@daily", "0 3 * * *" etc.
    catchup=False,
    default_args=default_args,
    tags=["n8n", "ETL", "monitoring"],
)
def n8n_multi_workflow_etl():

    @task
    def trigger_n8n_workflow(
        workflow_id: str,
        webhook_path: str,
        file_name: str = "input.csv",
        environment: str = "dev",
        conn_id: str = "n8n_local"
    ) -> dict:
        """
        Triggers an n8n webhook and returns basic trigger info.
        """
        conn = BaseHook.get_connection(conn_id)
        webhook_url = f"{conn.host.rstrip('/')}/webhook-test/{webhook_path}"

        headers = {"Authorization": f"Bearer {conn.password}"}
        params = {"file": file_name, "env": environment}

        try:
            response = requests.get(webhook_url, headers=headers, params=params, timeout=10)
            response.raise_for_status()
            print(f"✅ Workflow {workflow_id} triggered successfully via webhook {webhook_path}")
            return {
                "workflow_id": workflow_id,
                "webhook_path": webhook_path,
                "triggered_at": datetime.utcnow().isoformat(),
                "http_status": response.status_code
            }
        except requests.exceptions.RequestException as e:
            raise AirflowException(f"Failed to trigger n8n workflow {workflow_id}: {str(e)}")


    @task
    def monitor_n8n_workflow(
        trigger_result: dict,
        poll_interval: int = 5,
        timeout: int = 300,          # increased default timeout
        max_attempts: int = 60,
        conn_id: str = "n8n_local"
    ) -> str:
        """
        Monitors the latest execution of a specific n8n workflow.
        Uses trigger_result only to know which workflow we're watching.
        """
        workflow_id = trigger_result["workflow_id"]

        conn = BaseHook.get_connection(conn_id)
        api_key = conn.password

        headers = {
            "X-N8N-API-KEY": api_key,
            "Accept": "application/json"
        }

        executions_url = (
            f"{conn.host.rstrip('/')}/api/v1/executions"
            f"?workflowId={workflow_id}"
            f"&limit=1"
            f"&includeData=true"
        )

        print(f"→ Monitoring workflow {workflow_id} ...")

        start_time = time.time()
        attempt = 0
        latest_exec = None

        while attempt < max_attempts and (time.time() - start_time) < timeout:
            attempt += 1
            print(f"  attempt {attempt}/{max_attempts} ... ", end="")

            try:
                resp = requests.get(executions_url, headers=headers, timeout=12)
                resp.raise_for_status()
                data = resp.json()
                executions = data.get("data", [])

                if executions:
                    latest_exec = executions[0]
                    status = latest_exec["status"]
                    print(f"FOUND → status = {status}")
                    break
                else:
                    print("still not visible")

            except requests.exceptions.RequestException as e:
                print(f"request error: {str(e)}")

            time.sleep(poll_interval)

        if not latest_exec:
            raise AirflowException(
                f"Could not find any recent execution for workflow {workflow_id} "
                f"after {attempt} attempts (~{int(time.time()-start_time)}s)"
            )

        # ── Print nice summary + detailed logs ───────────────────────────────
        _print_execution_details(latest_exec, workflow_id)

        final_status = latest_exec["status"]

        if final_status in ["error", "failed", "crashed"]:
            raise AirflowException(
                f"n8n workflow {workflow_id} finished with bad status: {final_status}"
            )

        print(f"Workflow {workflow_id} completed → status = {final_status}")
        return final_status


    def _print_execution_details(exec_data: dict, workflow_id: str):
        """Helper - verbose printing of execution details"""
        print("\n" + "═" * 70)
        print(f"EXECUTION DETAILS — Workflow {workflow_id}")
        print("═" * 70)

        fields = [
            ("ID", "id"),
            ("Status", "status"),
            ("Mode", "mode"),
            ("Started", "startedAt"),
            ("Stopped", "stoppedAt"),
            ("Finished", "finished"),
            ("Retry of", "retryOf"),
        ]

        for label, key in fields:
            val = exec_data.get(key, "N/A")
            if val is None:
                val = "None"
            print(f"{label:12} : {val}")

        print("\nNode execution data:")
        print("-" * 60)

        run_data = exec_data.get("data", {}).get("resultData", {}).get("runData", {})

        if not run_data:
            print("No node run data available.")
            return

        for node_name, runs in run_data.items():
            print(f"• {node_name}  (runs: {len(runs)})")
            for i, run in enumerate(runs, 1):
                print(f"  ├─ Run #{i:2}  status: {run.get('status','?')}")
                if run.get("error"):
                    err = run["error"]
                    print(f"  │   ERROR: {err.get('message','<no message>')}")
                if run.get("data", {}).get("main"):
                    print(f"  │   → {len(run['data']['main'])} output branch(es)")

        print("-" * 60 + "\n")



    # Dag flow
    # JOB 1
    trigger_1 = trigger_n8n_workflow(
        workflow_id="yjkqT3FaghsITMcJ",
        webhook_path="d4727c48-61aa-4d22-a785-725bc3eff140",  # ← put real path
        file_name="input.csv",
        environment="dev"
    )

    monitor_1 = monitor_n8n_workflow(trigger_1)

    # JOB 2
    trigger_2 = trigger_n8n_workflow(
        workflow_id="5BiWUHRn0FznS3ZA",
        webhook_path="eb4766bb-6899-4dda-8557-b4a8114c1cf1",  # ← change
        file_name="input.csv",
        environment="dev"
    )

    monitor_2 = monitor_n8n_workflow(trigger_2)

    # You can add more jobs the same way...
    # JOB 3 → trigger_3 → monitor_3 ...

    # Optional: enforce order if needed
    monitor_1 >> trigger_2    # run job 2 only after job 1 succeeded


n8n_multi_dag = n8n_multi_workflow_etl()