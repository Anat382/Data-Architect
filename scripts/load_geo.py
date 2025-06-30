import json
# import ijson
import pandas as pd
import my_connection as mc
import db_objects as dbo
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from typing import Dict, List
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, JSON, text, MetaData
import sqlalchemy.types as sqltp
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import numpy as np
import my_sql_dml as dml


def geo_load(data, nm_table, nm_schema, check_atrebut=[]):
    """ Прасинг данных и запись в БД"""
    row = data
    # Извлечение данных
    vacancy_id = str(row.get("id", ""))
    vacancy_geo =  row.get("geo", {'latitude': "0.0", 'longitude': '0.0'})

    if row.get("id", "") not in check_atrebut:
        data_row = {
            'vacancy_id': vacancy_id,
            'latitude': vacancy_geo.get("latitude", "0.0"),
            'longitude': vacancy_geo.get("longitude", "0.0"),
        }

        # Запись данных в БД
        dml.ph_insert_to_table(data_row, nm_schema, nm_table)

        print(f"Loaded to {nm_table}")

    else:
        print(f"id - {vacancy_id}: data is exists")
