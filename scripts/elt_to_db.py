import json
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from typing import Dict, List
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, JSON, text
import sqlalchemy.types as sqltp
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import numpy as np
import db_objects  as dbob
import my_connection as mc
import my_sql_dml as dml
from sqlalchemy import MetaData
import load_vacancies as lv
import ijson 
import load_geo as lg
import load_company as lcm
import load_contacts as lcon
import load_education as led


def process_json_to_tables(json_path: str):
    """Основная функция обработки JSON и загрузки в PostgreSQL"""

    # Подключение к БД 
    db_connect =  mc.db_connection("postgres_default")
    postgres_hook = db_connect[0]
    engine = db_connect[1]
    dbob.Base.metadata.create_all(engine)
    db_schema = mc.schema_db["stg"]
    column = 'vacancy_id'
    # Получение идентификаторов записи
    table_name_vacancies = 'vacancies'
    vacancy_id_list_vacancies = dml.get_vacancies_id(db_schema, table_name_vacancies, column)
    table_name_geo = 'geo'
    vacancy_id_list_geo = dml.get_vacancies_id(db_schema, table_name_geo, column)
    table_name_company = 'company'
    vacancy_id_list_company = dml.get_vacancies_id(db_schema, table_name_company, column)
    table_name_education = 'education'
    vacancy_id_list_education = dml.get_vacancies_id(db_schema, table_name_education, column)
    table_name_contacts = 'contacts'
    vacancy_id_list_contacts = dml.get_vacancies_id(db_schema, table_name_contacts, 'concat(vacancy_id, contact_type)')

    # Построчное чтение json файла
    with open(json_path, 'r', encoding='utf-8') as f:

        # n = 0
        for element in ijson.items(f, "vacancies.item"):
            # Запись данных в БД
            lv.vacancies_load(element, table_name_vacancies, db_schema,  vacancy_id_list_vacancies)
            print(f"Load {table_name_vacancies}")
            lg.geo_load(element, table_name_geo, db_schema,  vacancy_id_list_geo)
            print(f"Load {table_name_geo}")
            lcm.company_load(element, table_name_company, db_schema,  vacancy_id_list_company)
            print(f"Load {table_name_company}")
            led.educat_load(element, table_name_education, db_schema,  vacancy_id_list_education)
            print(f"Load {table_name_education}")
            lcon.cotact_load(element, table_name_contacts, db_schema,  vacancy_id_list_contacts)
            print(f"Load {table_name_contacts}")
                
            # n+=1
            # if n >100:
            #     break
            # else:
            #     continue

if __name__ == "__main__":
    process_json_to_tables(mc.path_file)