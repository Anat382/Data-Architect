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


def company_load(data, nm_table, nm_schema, check_atrebut=[]):
    """ Прасинг данных и запись в БД"""

    # Извлечение данных
    row = data
    vacancy_id = str(row.get("id", ""))
    vacancy_company = row.get("company", {})

    if row.get("id", "") not in check_atrebut:
        data_row = {
            'vacancy_id': vacancy_id,
            'name': row.get("fullCompanyName", ""),
            'company_code': vacancy_company.get("companyCode", ""),
            'inn': vacancy_company.get("inn", ""),
            'kpp': vacancy_company.get("kpp", ""),
            'ogrn': vacancy_company.get("ogrn", ""),
            'email': vacancy_company.get("email", ""),
            'phone': vacancy_company.get("phone", ""),
            'hr_agency': vacancy_company.get("hrAgency", ""),
            'site': vacancy_company.get("site", ""),
            'url': vacancy_company.get("url", ""),
        }

        # Запись данных в БД
        dml.ph_insert_to_table(data_row, nm_schema, nm_table)

        print(f"Loaded to {nm_table}")

    else:
        print(f"id - {vacancy_id}: data is exists")
