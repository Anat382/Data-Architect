from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def read_sql_file(file_path):
    """Чтение содержимого SQL-файла"""
    with open(file_path, 'r') as f:
        return f.read()

def get_sql_content(**kwargs):
    """Чтение SQL-скриптов и передача их через XCom"""
    ti = kwargs['ti']
    
    # Читаем все необходимые SQL-файлы
    ddl_content = read_sql_file('/opt/airflow/scripts/dds_create_ddl.sql')
    func_content = read_sql_file('/opt/airflow/scripts/dds_create_function.sql')
    dml_content = read_sql_file('/opt/airflow/scripts/exec_function.sql')
    
    # Передаем содержимое через XCom
    ti.xcom_push(key='ddl_content', value=ddl_content)
    ti.xcom_push(key='func_content', value=func_content)
    ti.xcom_push(key='dml_content', value=dml_content)

with DAG(
    'execute_sql_script_fixed',
    default_args=default_args,
    description='DAG для выполнения SQL-скриптов из файлов',
    schedule_interval=None,
    start_date=datetime(2025, 6, 25),
    catchup=False,
    tags=['sql', 'postgres'],
) as dag:

    read_sql_files = PythonOperator(
        task_id='read_sql_files',
        python_callable=get_sql_content,
        provide_context=True,
    )

    create_ddl = PostgresOperator(
        task_id='create_ddl',
        postgres_conn_id='postgres_default',
        sql="{{ ti.xcom_pull(task_ids='read_sql_files', key='ddl_content') }}",
    )

    create_function = PostgresOperator(
        task_id='create_function',
        postgres_conn_id='postgres_default',
        sql="{{ ti.xcom_pull(task_ids='read_sql_files', key='func_content') }}",
    )

    exec_dml = PostgresOperator(
        task_id='exec_dml',
        postgres_conn_id='postgres_default',
        sql="{{ ti.xcom_pull(task_ids='read_sql_files', key='dml_content') }}",
    )

    # Определяем порядок выполнения задач
    read_sql_files >> create_ddl >> create_function >> exec_dml
    # read_sql_files >> exec_dml