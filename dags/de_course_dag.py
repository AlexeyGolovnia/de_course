from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from de_course.sessions_prepare import sessions_prepare
from de_course.sessions_processing import sessions_processing
from de_course.hits_processing import hits_processing
from de_course.add_to_sql import add_to_sql


args = {
    'owner': 'AGol',
    'start_date': days_ago(0),
}

with DAG(
        dag_id='de_course_v2.1',
        schedule_interval='@once',
        default_args=args,
        ) as dag:

    sessions_prepare = PythonOperator(
        task_id='sessions_prepare',
        python_callable=sessions_prepare,
    )

    sessions_processing = PythonOperator(
        task_id='sessions_processing',
        python_callable=sessions_processing,
    )

    hits_processing = PythonOperator(
        task_id='hits_processing',
        python_callable=hits_processing,
    )

    add_to_sql = PythonOperator(
        task_id='add_to_sql',
        python_callable=add_to_sql,
    )

    sessions_prepare >> sessions_processing >> add_to_sql
    sessions_prepare >> hits_processing >> add_to_sql

