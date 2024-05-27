from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from custom_operator.hello_operator import HelloOperator

# A DAG represents a workflow, a collection of tasks
with DAG(dag_id="demo", start_date=datetime(2022, 1, 1), schedule="0 0 * * *") as dag:
    # Tasks are represented as operators
    hello = BashOperator(task_id="hello", bash_command="echo hello")

    hello_task = HelloOperator(task_id="sample-task", name="foo_bar")

    @task()
    def airflow():
        print("airflow")

    # Set dependencies between tasks
    hello >> airflow() >> hello_task
