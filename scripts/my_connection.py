
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine

def db_connection(name_conn: str):
    """
    Соединение с БД для PostgresHook
    """
    hook = PostgresHook(postgres_conn_id=name_conn, echo=True)
    engine = hook.get_sqlalchemy_engine()  
    return  hook, engine

def db_connection_postgre():
    """
    Соединение с БД для sqlalchemy
    """
    DATABASE_URL = "postgresql://airflow:airflow@localhost:5432/airflow"
    engine = create_engine(DATABASE_URL)
    return engine
    

schema_db = {"stg": "stg", "dds":"dds", "dm":"dm"}
path_file = r"/opt/airflow/data/vacancy.json"