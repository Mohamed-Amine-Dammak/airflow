from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def hello_world():
    print("Hello, Airflow is running!")

with DAG(
    dag_id="test_dag",
    start_date=datetime(2026, 2, 11),
    schedule="@daily",   
    catchup=False,
    tags=["test"]
) as dag:

    task1 = PythonOperator(
        task_id="hello_task",
        python_callable=hello_world
    )

    task1
