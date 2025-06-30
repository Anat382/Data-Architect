from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
from scripts.elt_to_db import process_json_to_tables
from pathlib import Path
import scripts.my_connection as mc
import ijson

sys.path.append('/opt/airflow')


sys.path.append(str(Path(__file__).parent.parent))

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

def run_json_processor():
    
    sys.path.append('/opt/airflow/scripts')
    
    process_json_to_tables(mc.path_file)

with DAG(
    'json_to_postgres',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['data_processing'],
) as dag:
    
    process_json_task = PythonOperator(
        task_id='process_json_to_postgres',
        python_callable=run_json_processor,
    )