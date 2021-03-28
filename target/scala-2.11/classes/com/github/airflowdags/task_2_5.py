import datetime as dt
import random

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago


args = {
    'owner': 'airflow',
}

dag = DAG(
    dag_id='truata_assessment_dag',
    default_args=args,
    start_date=days_ago(2),
    schedule_interval="@daily",
)


Task1 = DummyOperator(
    task_id='Task1',
    dag=dag,
)

Task2 = DummyOperator(
    task_id='Task2',
    dag=dag,
)

Task3 = DummyOperator(
    task_id='Task3',
    dag=dag,
)

Task4 = DummyOperator(
    task_id='Task4',
    dag=dag,
)

Task5 = DummyOperator(
    task_id='Task5',
    dag=dag,
)

Task6 = DummyOperator(
    task_id='Task6',
    dag=dag,
)

join = DummyOperator(
    task_id='join',
    trigger_rule='none_failed_or_skipped',
    dag=dag,
)


Task1 >> Task2
Task1 >> Task3

Task2 >> join
Task3 >> join

join >> Task4
join >> Task5
join >> Task6