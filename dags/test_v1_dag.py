
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_message():
    print("testing from form ")

with DAG(
    dag_id="test_v1_dag",
    schedule="@daily",
    start_date=datetime(2026, 2, 12),
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id="print_task",
        python_callable=print_message
    )
