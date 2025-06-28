
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_etl():
    from etl_pipeline import ETLPipeline
    etl = ETLPipeline()
    etl.run_etl()

with DAG(
    'ecommerce_etl',
    default_args=default_args,
    description='ETL pipeline for ecommerce data',
    schedule_interval=timedelta(hours=1),
    start_date=days_ago(1),
    tags=['ecommerce', 'etl'],
) as dag:

    t1 = PythonOperator(
        task_id='run_etl_pipeline',
        python_callable=run_etl,
    )

    t2 = BashOperator(
        task_id='notify_completion',
        bash_command='echo "ETL pipeline completed at $(date)"',
    )

    t1 >> t2