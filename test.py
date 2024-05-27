from unittest import mock

from airflow.models import Connection
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.utils import db
from pendulum import datetime
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

import myspark_operator.myspark
from custom_operator.hello_operator import HelloOperator

with DAG(
        dag_id="simple_classic_dag",
        start_date=datetime(2023, 1, 1),
        schedule="@daily",
        catchup=False,
) as dag:  # assigning the context to an object is mandatory for using dag.test()

    t1 = EmptyOperator(task_id="t1")

    run_this = BashOperator(
        task_id="spark_bash_submit",
        bash_command="spark-submit --master local --class org.example.JavaSparkPi hdfs://10.194.186.216:8020/tmp/demo-spark-iceberg-1.0-SNAPSHOT.jar",
    )

    spark_task_k8s = myspark_operator.myspark.MySpark(
        task_id='spark_task_k8s',
        conn_id='spark_k8s_cluster',
        java_class='org.example.JavaSparkPi',
        application='hdfs://10.194.186.216:8020/tmp/demo-spark-iceberg-1.0-SNAPSHOT.jar',
        # Path to your Spark application
        total_executor_cores='2',  # Number of cores for the job
        executor_cores='1',  # Number of cores per executor
        executor_memory='1g',  # Memory per executor
        name='spark-pi-job',  # Name of the job
        verbose=True,
        num_executors=2,

        conf={
            "spark.kubernetes.authenticate.driver.serviceAccountName": "trino",
            "spark.kubernetes.container.image": "apache/spark:3.5.0"},
        dag=dag
    )

    hello_task = HelloOperator(task_id="sample-task", name="foo_bar")

t1 >> run_this >> spark_task_k8s >> hello_task

if __name__ == "__main__":
    # db.resetdb()
    db.merge_conn(
        Connection(
            conn_id="spark_k8s_cluster",
            conn_type="spark",
            host="k8s://https://lb.kubesphere.local:6443",
            extra='{"deploy-mode": "cluster"}',
        )
    )

    dag.test()
