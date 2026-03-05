from airflow.sdk import dag, task
from airflow.utils.email import send_email
from datetime import datetime

def custom_failure_email(context):
    """
    context: a dict that Airflow passes with task info
    """
    subject = f"❌ DAG {context['dag'].dag_id} | Task {context['task_instance'].task_id} Failed!"
    body = f"""
    Hello Team,

    The following task failed:

    DAG: {context['dag'].dag_id}
    Task: {context['task_instance'].task_id}
    Run ID: {context['run_id']}
    Try Number: {context['task_instance'].try_number}
    Exception: {context.get('exception')}

    Logs: {context['task_instance'].log_url}

    Please check ASAP!
    """
    send_email(to=["amineelkpfe@gmail.com"], subject=subject, html_content=body)

default_args = {
    "email_on_failure": True,  # still needed if you want automatic retries
    "on_failure_callback": custom_failure_email
}

@dag(
    dag_id="custom_failure_email_dag",
    start_date=datetime(2026, 3, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
)
def failure_email_dag():

    @task
    def failing_task():
        raise Exception("This task failed intentionally!")

    failing_task()

dag = failure_email_dag()