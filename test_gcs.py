from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 6, 2),
    'email': ['eamon@logistio.ie'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    dag_id="lifetime-template-sql",
    default_args=default_args,
    schedule_interval="@once",
    template_searchpath='/usr/local/airflow/dags/sql'
)


def test_gcs(ds, **context):
    test_file = "test_gcs.py"
    storage_bucket = "icabbi-202810-airflow"
    GoogleCloudStorageHook(google_cloud_storage_conn_id='google_conn').upload(bucket=storage_bucket,
                                                                                      object=test_file,
                                                                                      filename=test_file)

with dag:
    kick_off_dag = DummyOperator(task_id='kick_off_dag')
    test_gcs_dag = PythonOperator(
    task_id='get_clv-1',
    python_callable=test_gcs,
    provide_context=True
    )
    kick_off_dag >> test_gcs_dag

