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
    dag_id="n8n_etl_with_monitoring",
    start_date=datetime(2026, 2, 2),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["n8n", "ETL", "monitoring"],
)
def n8n_etl_with_monitoring():

    # 🔹 CHANGE THIS
    WORKFLOW_ID = "PRmxjk46QLaxey2X"
    WEBHOOK_PATH = "d4727c48-61aa-4d22-a785-725bc3eff140"

    @task()
    def trigger_n8n(file_name="input.csv", environment="dev"):
        conn = BaseHook.get_connection("n8n_local")
        webhook_url = f"{conn.host}/webhook-test/{WEBHOOK_PATH}"
        headers = {"Authorization": f"Bearer {conn.password}"}
        params = {"file": file_name, "env": environment}

        response = requests.get(webhook_url, headers=headers, params=params)
        response.raise_for_status()
        print("✅ n8n webhook triggered successfully")

    @task()
    def monitor_n8n(trigger_result, poll_interval=5, timeout=120, max_attempts=5):
        """
        Monitors the most recent execution of the n8n workflow after it was triggered.
        Uses workflowId filter + limit=1 for reliability.
        Prints detailed execution logs including all available fields (even null/empty).
        
        Args:
            trigger_result: Output from trigger_n8n task (ignored here)
            poll_interval: Seconds between checks
            timeout: Max total wait time in seconds
            max_attempts: Safety max number of poll attempts (~60-75s with default interval)
        
        Returns:
            str: Final execution status ('success', 'error', 'failed', etc.)
        """
        conn = BaseHook.get_connection("n8n_local")
        api_key = conn.password  # assuming password field holds X-N8N-API-KEY
        
        headers = {
            "X-N8N-API-KEY": api_key,
            "Accept": "application/json"
        }

        # Use your actual workflow ID (hardcoded or from Airflow Variable / conn extra)
        WORKFLOW_ID = "PRmxjk46QLaxey2X"  # ← change if needed, or pull from Variable

        executions_url = (
            f"{conn.host.rstrip('/')}/api/v1/executions"
            f"?workflowId={WORKFLOW_ID}"
            f"&limit=1"
            f"&includeData=true"
        )

        print("Waiting for n8n execution to become visible in API...")

        start_time = time.time()
        attempt = 0
        latest_exec = None
        exec_id = None

        while attempt < max_attempts and (time.time() - start_time) < timeout:
            attempt += 1
            print(f"Attempt {attempt}/{max_attempts}... ", end="")

            try:
                resp = requests.get(executions_url, headers=headers, timeout=10)
                resp.raise_for_status()
                data = resp.json()
                executions = data.get("data", [])

                if executions:
                    latest_exec = executions[0]
                    exec_id = latest_exec["id"]
                    status = latest_exec["status"]
                    started = latest_exec.get("startedAt", "N/A")

                    print(f"FOUND! ID = {exec_id} | Status = {status} | Started = {started}")
                    break
                else:
                    print("not visible yet")

            except requests.exceptions.RequestException as e:
                print(f"Request error: {str(e)}")

            time.sleep(poll_interval)

        if not latest_exec:
            raise AirflowException(
                f"After {attempt} attempts (~{int(time.time() - start_time)}s), "
                f"no execution found for workflow '{WORKFLOW_ID}'.\n"
                f"Check n8n UI → Executions tab.\n"
                f"Verify: workflow active? Correct WORKFLOW_ID? API key permissions?"
            )

        # ────────────────────────────────────────────────
        # Get detailed run data (already included, but fallback if partial)
        # ────────────────────────────────────────────────
        run_data = latest_exec.get("data", {}).get("resultData", {}).get("runData", {})

        if not run_data:
            print("⚠️ No runData found in list response → fetching single execution...")
            detail_url = f"{conn.host.rstrip('/')}/api/v1/executions/{exec_id}?includeData=true"
            try:
                detail_resp = requests.get(detail_url, headers=headers, timeout=15)
                detail_resp.raise_for_status()
                full_exec = detail_resp.json()
                run_data = full_exec.get("data", {}).get("resultData", {}).get("runData", {})
            except Exception as e:
                print(f"Failed to fetch detailed execution: {str(e)}")

        # ────────────────────────────────────────────────
        # Verbose detailed printing (all fields, nulls included)
        # ────────────────────────────────────────────────
        print("\n" + "=" * 60)
        print("FULL N8N EXECUTION DETAILS & LOGS")
        print("=" * 60)

        print(f"Execution ID       : {exec_id}")
        print(f"Workflow ID        : {latest_exec.get('workflowId', 'N/A')}")
        print(f"Status             : {latest_exec.get('status', 'N/A')}")
        print(f"Mode               : {latest_exec.get('mode', 'N/A')}")
        print(f"Started At         : {latest_exec.get('startedAt', 'N/A')}")
        print(f"Stopped At         : {latest_exec.get('stoppedAt') or 'N/A'}")
        print(f"Finished           : {latest_exec.get('finished', 'N/A')}")
        print(f"Retry Of           : {latest_exec.get('retryOf') or 'None'}")
        print(f"Custom Data        : {latest_exec.get('customData') or {}}")
        print(f"Wait Till          : {latest_exec.get('waitTill') or 'N/A'}")
        print("\n")

        if not run_data:
            print("⚠️ No node execution data available.")
            print("   → Workflow may be empty, failed early, or data not captured.")
        else:
            print("NODE-BY-NODE DETAILS (including null/empty fields):")
            print("-" * 60 + "\n")

            for node_name, node_runs in run_data.items():
                print(f"🔹 NODE: {node_name}")
                print(f"   Total runs: {len(node_runs)}")

                for run_idx, run in enumerate(node_runs, 1):
                    print(f"   ├─ Run #{run_idx}")

                    # Run metadata
                    print(f"      Status          : {run.get('status') or 'N/A'}")
                    print(f"      Execution Time  : {run.get('executionTime') or 'N/A'} ms")
                    print(f"      Started         : {run.get('startedAt') or 'N/A'}")
                    print(f"      Stopped         : {run.get('stoppedAt') or 'N/A'}")

                    # Error block
                    if run.get("error"):
                        err = run["error"]
                        print(f"      ❌ ERROR:")
                        print(f"         Message     : {err.get('message') or 'No message'}")
                        print(f"         Name        : {err.get('name') or 'N/A'}")
                        print(f"         Description : {err.get('description') or 'N/A'}")
                        print(f"         Stack       : {err.get('stack') or 'N/A'}")

                    # Output data – full verbose dump
                    if run.get("data"):
                        output_data = run["data"]
                        main_branches = output_data.get("main", [])

                        print(f"      Output branches : {len(main_branches)}")

                        for b_idx, branch in enumerate(main_branches, 1):
                            print(f"         ├─ Branch {b_idx} ({len(branch)} items)")

                            if not branch:
                                print("            (empty)")
                                continue

                            for item_idx, item in enumerate(branch, 1):
                                print(f"            ├─ Item {item_idx}")
                                for key, value in item.items():
                                    if value is None:
                                        print(f"               {key:<14}: null")
                                    elif isinstance(value, (dict, list)):
                                        pretty = json.dumps(value, indent=2, ensure_ascii=False)
                                        print(f"               {key:<14}:")
                                        for line in pretty.splitlines():
                                            print(f"                  {line}")
                                    else:
                                        print(f"               {key:<14}: {value}")
                                print("")  # spacing
                    else:
                        print("      No output data")

                    print("")  # run separator

        print("\n" + "-" * 60)
        print("Monitoring complete.")

        final_status = latest_exec.get("status")
        if final_status in ["error", "failed"]:
            raise AirflowException(f"n8n workflow failed with status: {final_status}")

        return final_status
    # DAG flow
    triggered = trigger_n8n()
    monitor_n8n(triggered)

dag = n8n_etl_with_monitoring()
