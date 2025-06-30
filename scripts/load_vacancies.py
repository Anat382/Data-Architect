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



def vacancies_load(data, nm_table, nm_schema, check_atrebut=[]):
    """ Прасинг данных и запись в БД"""

    # Извлечение данных
    row = data
    vacancy_id = str(row.get("id", ""))

    if row.get("id", "") not in check_atrebut:
        data_row = {
            'vacancy_id': vacancy_id,
            'vacancy_name': row.get("vacancyName", ""),
            'code_profession': row.get("codeProfession", ""),
            'vacancy_address_additional_info': row.get("vacancyAddressAdditionalInfo", ""),
            'vacancy_address_house': row.get("vacancyAddressHouse", ""),
            'typical_position': row.get("typicalPosition", ""),
    
            'salary': row.get("salary", ""),
            'salary_min': row.get("salaryMin", 0),
            'salary_max': row.get("salaryMax", 0),
            
            'region_name': row.get("regionName", ""),
            'status': row.get("status", ""),
            'vacancy_url': row.get("vacancyUrl", ""),
            'professional_sphere_name': row.get("professionalSphereName", ""),
            'experience_requirements': row.get("experienceRequirements", ""),
            'busy_type': row.get("busyType", ""),

            'skills': row.get("skills", ""),
            'hard_skills': row.get("hardSkills", ""),
            'soft_skills': row.get("softSkills", ""),
            
            'schedule_type': row.get("scheduleType", ""),
            'other_vacancy_benefit': row.get("otherVacancyBenefit", ""),
            'career_perspective': row.get("careerPerspective", ""),
            'id_priority_category': row.get("idPriorityCategory", ""),
            'source_type': row.get("sourceType", ""),
            'transport_compensation': row.get("transportCompensation", ""),
            'required_drive_license': row.get("requiredDriveLicense", ""),
            'required_certificates': row.get("requiredСertificates", ""),
            'contact_person': row.get("contactPerson", ""),
            'contact_source': row.get("contactSource", ""),
            'full_company_name': row.get("fullCompanyName", ""),
            'okso_code': row.get("oksoCode", ""),
            'company_business_size': row.get("companyBusinessSize", ""),
            'federal_district_code': row.get("federalDistrictCode", ""),
            'date_published': row.get("datePublished", ""),
            'accommodation_capability': row.get("accommodationCapability", ""),
            'creation_date': row.get("creationDate", ""),
            'position_requirements': row.get("positionRequirements", ""),
            'deleted': row.get("deleted", ""),
            'visibility': row.get("visibility", ""),
            'responsibilities': row.get("responsibilities", "")
        }

        # Запись данных в БД
        dml.ph_insert_to_table(data_row, nm_schema, nm_table)

        print(f"Loaded to {nm_table}")

    else:
        print(f"id - {vacancy_id}: data is exists")