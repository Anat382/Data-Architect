from sqlalchemy import insert, Table, MetaData
import my_connection as mc

def sa_insert_to_table(data_row, nm_table, db_schema='stg'):
    """
    Загрузка данных при использовании библиотеки PostgresHook
    """
    metadata = MetaData(schema=db_schema)
    # Подключение к БД
    engine = mc.db_connection_postgre()
    with engine.begin() as connection:
        #Запись в таблицу
        table = Table(nm_table, metadata, autoload_with=connection)
        stmt = insert(table).values(**data_row)
        connection.execute(stmt)

def ph_insert_to_table(data_row, nm_schema, nm_table):
    """
    Загрузка данных при использовании библиотеки sqlalchemy
    """
    # Подключение к БД
    db_connect =  mc.db_connection("postgres_default")
    postgres_hook = db_connect[0]
    engine = db_connect[1]

    # Преобразования
    columns = ', '.join(data_row.keys())
    placeholders = ', '.join(['%s'] * len(data_row))

    #Запись в таблицу
    sql = f"INSERT INTO {nm_schema}.{nm_table} ({columns}) VALUES ({placeholders})"
    postgres_hook.run(sql, parameters=list(data_row.values()))
    postgres_hook_conn = postgres_hook.conn
    postgres_hook_conn.commit
    postgres_hook_conn.close

def get_vacancies_id(nm_schema, nm_table, column):
    """
    Получение уникального идентификатора записи в  таблице для проверки раннее записанных. 
    """
    # Подключение к БД
    db_connect =  mc.db_connection("postgres_default")
    postgres_hook = db_connect[0]
    engine = db_connect[1]
    #Запись в таблицу
    vacancy_id_query = postgres_hook.get_records(f"SELECT DISTINCT {column} FROM {nm_schema}.{nm_table}")  
    vacancy_id_list = [row[0] for row in vacancy_id_query]
    postgres_hook_conn = postgres_hook.conn
    postgres_hook_conn.commit
    postgres_hook_conn.close
    return vacancy_id_list