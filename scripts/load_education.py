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


def educat_load(data, nm_table, nm_schema, check_atrebut=[]):
    """ Прасинг данных и запись в БД"""

    # Извлечение данных
    row = data
    vacancy_id = str(row.get("id", ""))
    vacancy_education = row.get("educationRequirements", {})

    if row.get("id", "") not in check_atrebut:
        data_row = {
            'vacancy_id': vacancy_id,
            'education_type': vacancy_education.get("educationType", ""),
            'speciality': vacancy_education.get("educationSpeciality", ""),
        }

        # Запись данных в БД
        dml.ph_insert_to_table(data_row, nm_schema, nm_table)

        print(f"Loaded to {nm_table}")

    else:
        print(f"id - {vacancy_id}: data is exists")
