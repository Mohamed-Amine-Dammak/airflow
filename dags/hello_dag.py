from airflow import DAG
from airflow.decorators import task
from datetime import datetime

with DAG(
    dag_id="hello_world_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",  # or None for manual trigger
    catchup=False,
    tags=["example"],
) as dag:

    @task
    def say_hello():
        print("Hello World from Airflow 3!")

    say_hello()