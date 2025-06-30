from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, JSON, DateTime, func, Numeric
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import MetaData
import sqlalchemy as sh
import my_connection as mc


db_schema = mc.schema_db["stg"]
# metadata = MetaData(schema=db_schema) 
# Base = declarative_base(metadata)
Base = declarative_base()


class Vacancy(Base):
    __tablename__ = 'vacancies'
    __table_args__ = {'schema': db_schema}  
    
    vacancy_id = Column(String, primary_key=True)
    vacancy_name = Column(String)
    code_profession = Column(String)
    vacancy_address_additional_info = Column(String)
    vacancy_address_house = Column(String)
    typical_position = Column(String)
    
    # Поля зарплаты (можно сделать Numeric или Integer)
    salary = Column(String)  # или Integer
    salary_min = Column(Numeric(10, 2))  # или Integer
    salary_max = Column(Numeric(10, 2))  # или Integer
    
    # Остальные строковые поля
    region_name = Column(String)
    status = Column(String)
    vacancy_url = Column(String)
    professional_sphere_name = Column(String)
    experience_requirements = Column(String)
    busy_type = Column(String)
    
    # Навыки (можно оставить String или сделать JSON)
    skills = Column(String)  # или JSON
    hard_skills = Column(String)  # или JSON
    soft_skills = Column(String)  # или JSON
    
    # Дополнительные поля
    schedule_type = Column(String)
    other_vacancy_benefit = Column(String)
    career_perspective = Column(String)
    id_priority_category = Column(String)
    source_type = Column(String)
    transport_compensation = Column(String)
    required_drive_license = Column(String)
    required_certificates = Column(String)
    contact_person = Column(String)
    contact_source = Column(String)
    full_company_name = Column(String)
    okso_code = Column(String)
    company_business_size = Column(String)
    federal_district_code = Column(String)
    
    # Дата-время (лучше DateTime, но если строка - String)
    date_published = Column(String)  # или DateTime
    creation_date = Column(String)   # или DateTime
    
    # Остальные поля
    accommodation_capability = Column(String)
    position_requirements = Column(String)
    deleted = Column(String)
    visibility = Column(String)
    responsibilities = Column(String)
    
    # Дополнительные технические поля (если нужны)
    created_at = Column(DateTime, server_default=func.now())

class Geo(Base):
    __tablename__ = 'geo'
    __table_args__ = {'schema': db_schema}
    
    vacancy_id = Column(String, primary_key=True)
    latitude = Column(String)
    longitude = Column(String)
    created_dt = Column(DateTime, server_default=func.now())

class Education(Base):
    __tablename__ = 'education'
    __table_args__ = {'schema': db_schema}
    
    vacancy_id = Column(String, primary_key=True)
    education_type = Column(String)
    speciality = Column(String)
    created_dt = Column(DateTime, server_default=func.now())

class Company(Base):
    __tablename__ = 'company'
    __table_args__ = {'schema': db_schema}
    
    vacancy_id = Column(String, primary_key=True)
    name = Column(String)
    company_code = Column(String)
    inn = Column(String)
    kpp = Column(String)
    ogrn = Column(String)
    email = Column(String)
    phone = Column(String)
    hr_agency = Column(String)
    site = Column(String)
    url = Column(String)
    created_dt = Column(DateTime, server_default=func.now())

class Contacts(Base):
    __tablename__ = 'contacts'
    __table_args__ = {'schema': db_schema}
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    vacancy_id = Column(String)
    contact_type = Column(String)
    contact_value = Column(String)
    created_dt = Column(DateTime, server_default=func.now())
