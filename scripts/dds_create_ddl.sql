

-- drop table if exists dds.dict_professional_sphere_name;
CREATE TABLE if not exists dds.dict_professional_sphere_name (
    id SERIAL PRIMARY KEY,
    professional_sphere_name VARCHAR(255) NOT NULL,
    dt_write_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT dict_professional_sphere_name_id_unique UNIQUE (professional_sphere_name)
);

COMMENT ON TABLE dds.dict_professional_sphere_name IS 'Справочник наименований профессиональной сферы в слое DDS';

COMMENT ON COLUMN dds.dict_professional_sphere_name.id IS 'Уникальный идентификатор записи в справочнике (суррогатный ключ)';
COMMENT ON COLUMN dds.dict_professional_sphere_name.professional_sphere_name IS 'Наименование вакансии';
COMMENT ON COLUMN dds.dict_professional_sphere_name.dt_write_at IS 'Дата и время записи идентификатора в систему (метка времени создания записи)';


-- drop table if exists dds.dict_schedule_type;
CREATE TABLE if not exists dds.dict_schedule_type (
    id SERIAL PRIMARY KEY,
    schedule_type VARCHAR(255) NOT NULL,
    dt_write_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT dict_schedule_type_id_unique UNIQUE (schedule_type)
);

COMMENT ON TABLE dds.dict_schedule_type IS 'Справочник графика работы в слое DDS';

COMMENT ON COLUMN dds.dict_schedule_type.id IS 'Уникальный идентификатор записи в справочнике (суррогатный ключ)';
COMMENT ON COLUMN dds.dict_schedule_type.schedule_type IS 'График работы';
COMMENT ON COLUMN dds.dict_schedule_type.dt_write_at IS 'Дата и время записи идентификатора в систему (метка времени создания записи)';


-- drop table if exists dds.dict_experience_requirements;
CREATE TABLE if not exists dds.dict_experience_requirements (
    id SERIAL PRIMARY KEY,
    experience_requirements VARCHAR(255) NOT NULL,
    dt_write_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT dict_experience_requirements_id_unique UNIQUE (experience_requirements)
);

COMMENT ON TABLE dds.dict_experience_requirements IS 'Справочник требуемого опыты работы в слое DDS';

COMMENT ON COLUMN dds.dict_experience_requirements.id IS 'Уникальный идентификатор записи в справочнике (суррогатный ключ)';
COMMENT ON COLUMN dds.dict_experience_requirements.experience_requirements IS 'Требуемый опыт работы';
COMMENT ON COLUMN dds.dict_experience_requirements.dt_write_at IS 'Дата и время записи идентификатора в систему (метка времени создания записи)';


-- drop table if exists dds.dict_source_type;
CREATE TABLE if not exists dds.dict_source_type (
    id SERIAL PRIMARY KEY,
    source_type VARCHAR(255) NOT NULL,
    dt_write_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT dict_source_type_id_unique UNIQUE (source_type)
);

COMMENT ON TABLE dds.dict_source_type IS 'Справочник источника вакансии в слое DDS';

COMMENT ON COLUMN dds.dict_source_type.id IS 'Уникальный идентификатор записи в справочнике (суррогатный ключ)';
COMMENT ON COLUMN dds.dict_source_type.source_type IS 'Источник вакансии';
COMMENT ON COLUMN dds.dict_source_type.dt_write_at IS 'Дата и время записи идентификатора в систему (метка времени создания записи)';


-- drop table if exists dds.dict_company_business_size;
CREATE TABLE if not exists dds.dict_company_business_size (
    id SERIAL PRIMARY KEY,
    company_business_size VARCHAR(255) NOT NULL,
    dt_write_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT dict_company_business_size_id_unique UNIQUE (company_business_size)
);

COMMENT ON TABLE dds.dict_company_business_size IS 'Справочник размера компании в слое DDS';

COMMENT ON COLUMN dds.dict_company_business_size.id IS 'Уникальный идентификатор записи в справочнике (суррогатный ключ)';
COMMENT ON COLUMN dds.dict_company_business_size.company_business_size IS 'Размер компании';
COMMENT ON COLUMN dds.dict_company_business_size.dt_write_at IS 'Дата и время записи идентификатора в систему (метка времени создания записи)';


-- drop table if exists dds.dict_vacancies_name;
CREATE TABLE if not exists dds.dict_vacancies_name (
    id SERIAL PRIMARY KEY,
    vacancy_name VARCHAR NOT NULL,
    dt_write_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT dict_vacancies_name_id_unique UNIQUE (vacancy_name)
);

COMMENT ON TABLE dds.dict_vacancies_name IS 'Справочник наименований вакансий в слое DDS';

COMMENT ON COLUMN dds.dict_vacancies_name.id IS 'Уникальный идентификатор записи в справочнике (суррогатный ключ)';
COMMENT ON COLUMN dds.dict_vacancies_name.vacancy_name IS 'Наименование вакансии';
COMMENT ON COLUMN dds.dict_vacancies_name.dt_write_at IS 'Дата и время записи идентификатора в систему (метка времени создания записи)';


-- drop table if exists dds.dict_region_name;
CREATE TABLE if not exists dds.dict_region_name (
    id SERIAL PRIMARY KEY,
    region_name VARCHAR(255) NOT NULL,
    dt_write_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT dict_region_name_id_unique UNIQUE (region_name)
);

COMMENT ON TABLE dds.dict_region_name IS 'Справочник наименований вакансий в слое DDS';

COMMENT ON COLUMN dds.dict_region_name.id IS 'Уникальный идентификатор записи в справочнике (суррогатный ключ)';
COMMENT ON COLUMN dds.dict_region_name.region_name IS 'Регион размещения вакансии';
COMMENT ON COLUMN dds.dict_region_name.dt_write_at IS 'Дата и время записи идентификатора в систему (метка времени создания записи)';



-- drop table if exists dds.dict_status_name;
CREATE TABLE if not exists dds.dict_status_name (
    id SERIAL PRIMARY KEY,
    status_name VARCHAR(255) NOT NULL,
    dt_write_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT dict_status_name_id_unique UNIQUE (status_name)
);

COMMENT ON TABLE dds.dict_status_name IS 'Справочник наименований вакансий в слое DDS';

COMMENT ON COLUMN dds.dict_status_name.id IS 'Уникальный идентификатор записи в справочнике (суррогатный ключ)';
COMMENT ON COLUMN dds.dict_status_name.status_name IS 'Статус вакансии';
COMMENT ON COLUMN dds.dict_status_name.dt_write_at IS 'Дата и время записи идентификатора в систему (метка времени создания записи)';



-- drop table if exists dds.dict_company;
CREATE TABLE if not exists dds.dict_company (
    id SERIAL PRIMARY KEY,
    "name" VARCHAR NULL,
    company_code VARCHAR NULL,
    inn VARCHAR NULL,
    kpp VARCHAR NULL,
    ogrn VARCHAR NULL,
    email VARCHAR NULL,
    phone VARCHAR NULL,
    hr_agency VARCHAR NULL,
    site VARCHAR NULL,
    url VARCHAR null,
	dt_write_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT dict_company_id_unique UNIQUE ("name", company_code, inn, kpp)
);

COMMENT ON TABLE dds.dict_company IS 'Справочник компаний-работодателей в слое детализированных данных (DDS)';

COMMENT ON COLUMN dds.dict_company.id IS 'Суррогатный ключ компании';
COMMENT ON COLUMN dds.dict_company.name IS 'Полное наименование компании';
COMMENT ON COLUMN dds.dict_company.company_code IS 'Уникальный код компании в системе (бизнес-ключ)';
COMMENT ON COLUMN dds.dict_company.inn IS 'ИНН организации (10 или 12 цифр)';
COMMENT ON COLUMN dds.dict_company.kpp IS 'КПП организации (9 цифр)';
COMMENT ON COLUMN dds.dict_company.ogrn IS 'ОГРН/ОГРНИП организации (13 или 15 цифр)';
COMMENT ON COLUMN dds.dict_company.email IS 'Основной контактный email';
COMMENT ON COLUMN dds.dict_company.phone IS 'Основной контактный телефон';
COMMENT ON COLUMN dds.dict_company.hr_agency IS 'Наименование HR-агентства (если компания представлена через аутсорсинг)';
COMMENT ON COLUMN dds.dict_company.site IS 'URL официального сайта компании';
COMMENT ON COLUMN dds.dict_company.url IS 'Ссылка на профиль компании в системе';
COMMENT ON COLUMN dds.dict_company.dt_write_at IS 'Дата и время записи идентификатора в систему (метка времени создания записи)';


-- drop table if exists dds.dict_geo;
CREATE TABLE  if not exists dds.dict_geo (
    id SERIAL PRIMARY KEY,
    latitude VARCHAR NULL,
    longitude VARCHAR NULL,
    dt_write_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT dict_geo_id_unique UNIQUE (latitude, longitude)
);

COMMENT ON TABLE dds.dict_geo IS 'Версионный справочник географических координат в слое детализированных данных (DDS)';

COMMENT ON COLUMN dds.dict_geo.id IS 'Суррогатный ключ записи в справочнике';
COMMENT ON COLUMN dds.dict_geo.latitude IS 'Географическая широта в формате строки (градусы)';
COMMENT ON COLUMN dds.dict_geo.longitude IS 'Географическая долгота в формате строки (градусы)';
COMMENT ON COLUMN dds.dict_geo.dt_write_at IS 'Дата и время записи идентификатора в систему (метка времени создания записи)';


-- drop table if exists dds.dict_contact_type;
CREATE table  if not exists dds.dict_contact_type (
    id SERIAL PRIMARY KEY,
    contact_type VARCHAR NOT NULL,
    dt_write_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT dict_contact_type_id_unique UNIQUE (contact_type)
);

COMMENT ON TABLE dds.dict_contact_type IS 'Версионный справочник типов контактной информации (DDS слой)';

COMMENT ON COLUMN dds.dict_contact_type.id IS 'Суррогатный первичный ключ типа контакта';
COMMENT ON COLUMN dds.dict_contact_type.contact_type IS 'Наименование типа контакта (обязательное поле)';
COMMENT ON COLUMN dds.dict_contact_type.dt_write_at IS 'Дата и время записи идентификатора в систему (метка времени создания записи)';


-- drop table IF EXISTS dds.dict_education_type;
CREATE TABLE  if not exists dds.dict_education_type (
    id SERIAL PRIMARY KEY,
    education_type VARCHAR(100) NOT NULL,
    dt_write_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT dict_education_type_id_unique UNIQUE (education_type)
);

COMMENT ON TABLE dds.dict_education_type IS 'Версионный справочник типов образования (DDS слой)';

COMMENT ON COLUMN dds.dict_education_type.id IS 'Уникальный идентификатор типа образования (суррогатный ключ)';
COMMENT ON COLUMN dds.dict_education_type.education_type IS 'Наименование типа образования (пример: "Высшее", "Среднее специальное", "Бакалавриат")';
COMMENT ON COLUMN dds.dict_education_type.dt_write_at IS 'Дата и время записи идентификатора в систему (метка времени создания записи)';


-- drop table if exists dds.dict_vacancies_id;
CREATE table if not exists dds.dict_vacancies_id (
    id SERIAL PRIMARY KEY,
    vacancy_id VARCHAR(100) NOT NULL,
    dt_write_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT dict_vacancies_id_unique UNIQUE (vacancy_id)
);

COMMENT ON TABLE dds.dict_vacancies_id IS 'Справочник внешних идентификаторов вакансий (DDS слой)';

COMMENT ON COLUMN dds.dict_vacancies_id.id IS 'Суррогатный первичный ключ записи';
COMMENT ON COLUMN dds.dict_vacancies_id.vacancy_id IS 'Внешний идентификатор вакансии из системы-источника (бизнес-ключ)';
COMMENT ON COLUMN dds.dict_vacancies_id.dt_write_at IS 'Дата и время записи идентификатора в систему (метка времени создания записи)';


-- drop table IF EXISTS dds.contacts;
CREATE TABLE IF NOT EXISTS dds.contacts (
    id SERIAL PRIMARY KEY,
    id_vacancies INT NOT NULL,
    id_contact_type INT NOT NULL,
    contact_value VARCHAR NOT NULL,
    dt_write_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_contacts_vacancies FOREIGN KEY (id_vacancies) 
        REFERENCES dds.dict_vacancies_id(id),
    CONSTRAINT fk_contacts_contact_type FOREIGN KEY (id_contact_type) 
        REFERENCES dds.dict_contact_type(id),
    CONSTRAINT uq_contacts_vacancy_contact UNIQUE (id_vacancies, id_contact_type, contact_value)
);

COMMENT ON TABLE dds.contacts IS 'Контактная информация по вакансиям в слое DDS';

COMMENT ON COLUMN dds.contacts.id IS 'Уникальный идентификатор контакта (суррогатный ключ)';
COMMENT ON COLUMN dds.contacts.id_vacancies IS 'Внешний ключ на таблицу dds.dict_vacancies_id (идентификатор вакансии)';
COMMENT ON COLUMN dds.contacts.id_contact_type IS 'Внешний ключ на таблицу dds.dict_contact_type (тип контакта)';
COMMENT ON COLUMN dds.contacts.contact_value IS 'Значение контакта (телефон, email и т.д.)';
COMMENT ON COLUMN dds.contacts.dt_write_at IS 'Дата и время записи идентификатора в систему (метка времени создания записи)';


-- drop table IF EXISTS dds.vacancies;
CREATE TABLE IF NOT EXISTS dds.vacancies (
    id SERIAL PRIMARY KEY,
    id_vacancy INT NOT NULL,
    id_vacancy_name INT NOT NULL,
    id_professional_sphere INT NOT NULL,
    id_schedule_type INT NOT NULL,
    id_experience_requirements INT NOT NULL,
    id_source_type INT NOT NULL,
    id_company_business_size INT NOT NULL,
    id_region_name INT NOT NULL,
    id_status_name INT NOT NULL,
    id_company INT NOT NULL,
    id_geo INT NOT NULL,
    id_education_type INT NOT NULL,
    id_contact_type INT NOT NULL,
    salary_min NUMERIC(15, 2) NOT NULL,
    salary_max NUMERIC(15, 2) NOT NULL,
    vacancy_url VARCHAR(500),
    date_published TIMESTAMP WITH TIME ZONE,
    creation_date TIMESTAMP WITH TIME ZONE,
    dt_write_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_vacancies_vacancy_id FOREIGN KEY (id_vacancy) 
        REFERENCES dds.dict_vacancies_id(id),
    CONSTRAINT fk_vacancies_vacancy_name FOREIGN KEY (id_vacancy_name) 
        REFERENCES dds.dict_vacancies_name(id),
    CONSTRAINT fk_vacancies_professional_sphere FOREIGN KEY (id_professional_sphere) 
        REFERENCES dds.dict_professional_sphere_name(id),
    CONSTRAINT fk_vacancies_schedule_type FOREIGN KEY (id_schedule_type) 
        REFERENCES dds.dict_schedule_type(id),
    CONSTRAINT fk_vacancies_experience FOREIGN KEY (id_experience_requirements) 
        REFERENCES dds.dict_experience_requirements(id),
    CONSTRAINT fk_vacancies_source_type FOREIGN KEY (id_source_type) 
        REFERENCES dds.dict_source_type(id),
    CONSTRAINT fk_vacancies_company_size FOREIGN KEY (id_company_business_size) 
        REFERENCES dds.dict_company_business_size(id),
    CONSTRAINT fk_vacancies_region FOREIGN KEY (id_region_name) 
        REFERENCES dds.dict_region_name(id),
    CONSTRAINT fk_vacancies_status FOREIGN KEY (id_status_name) 
        REFERENCES dds.dict_status_name(id),
    CONSTRAINT fk_vacancies_company FOREIGN KEY (id_company) 
        REFERENCES dds.dict_company(id),
    CONSTRAINT fk_vacancies_geo FOREIGN KEY (id_geo) 
        REFERENCES dds.dict_geo(id),
    CONSTRAINT fk_vacancies_education FOREIGN KEY (id_education_type) 
        REFERENCES dds.dict_education_type(id),
    CONSTRAINT fk_vacancies_contact_type FOREIGN KEY (id_contact_type) 
        REFERENCES dds.dict_contact_type(id),
    CONSTRAINT uq_vacancies UNIQUE (id_vacancy)
);

COMMENT ON TABLE dds.vacancies IS 'Основная таблица вакансий в слое DDS';

COMMENT ON COLUMN dds.vacancies.id IS 'Суррогатный первичный ключ вакансии';
COMMENT ON COLUMN dds.vacancies.id_vacancy IS 'Внешний ключ на справочник внешних идентификаторов вакансий (dict_vacancies_id)';
COMMENT ON COLUMN dds.vacancies.id_vacancy_name IS 'Внешний ключ на справочник наименований вакансий (dict_vacancies_name)';
COMMENT ON COLUMN dds.vacancies.id_professional_sphere IS 'Внешний ключ на справочник профессиональных сфер (dict_professional_sphere_name)';
COMMENT ON COLUMN dds.vacancies.id_schedule_type IS 'Внешний ключ на справочник типов графиков работы (dict_schedule_type)';
COMMENT ON COLUMN dds.vacancies.id_experience_requirements IS 'Внешний ключ на справочник требований к опыту (dict_experience_requirements)';
COMMENT ON COLUMN dds.vacancies.id_source_type IS 'Внешний ключ на справочник источников вакансий (dict_source_type)';
COMMENT ON COLUMN dds.vacancies.id_company_business_size IS 'Внешний ключ на справочник размеров компаний (dict_company_business_size)';
COMMENT ON COLUMN dds.vacancies.id_region_name IS 'Внешний ключ на справочник регионов (dict_region_name)';
COMMENT ON COLUMN dds.vacancies.id_status_name IS 'Внешний ключ на справочник статусов вакансий (dict_status_name)';
COMMENT ON COLUMN dds.vacancies.id_company IS 'Внешний ключ на справочник компаний (dict_company)';
COMMENT ON COLUMN dds.vacancies.id_geo IS 'Внешний ключ на справочник географических координат (dict_geo), может быть NULL';
COMMENT ON COLUMN dds.vacancies.id_education_type IS 'Внешний ключ на справочник типов образования (dict_education_type)';
COMMENT ON COLUMN dds.vacancies.id_contact_type IS 'Внешний ключ на справочник типов контактов (dict_contact_type)';
COMMENT ON COLUMN dds.vacancies.salary_min IS 'Минимальный уровень зарплаты (число с 2 знаками после запятой)';
COMMENT ON COLUMN dds.vacancies.salary_max IS 'Максимальный уровень зарплаты (число с 2 знаками после запятой)';
COMMENT ON COLUMN dds.vacancies.vacancy_url IS 'URL-адрес вакансии в источнике (максимальная длина 500 символов)';
COMMENT ON COLUMN dds.vacancies.date_published IS 'Дата и время публикации вакансии с учетом временной зоны';
COMMENT ON COLUMN dds.vacancies.creation_date IS 'Дата и время создания записи в системе';
COMMENT ON COLUMN dds.vacancies.dt_write_at IS 'Дата и время последнего обновления записи (метка времени по умолчанию CURRENT_TIMESTAMP)';



-- DROP TABLE IF EXISTS dm.vacancies;
CREATE TABLE IF NOT EXISTS dm.vacancies (
    id SERIAL PRIMARY KEY,
    vacancy_id VARCHAR(100) NOT NULL,
    vacancy_name VARCHAR NOT NULL,
    professional_sphere_name VARCHAR(255) NOT NULL,
    schedule_type VARCHAR(255) NOT NULL,
    experience_requirements VARCHAR(255) NOT NULL,
    source_type VARCHAR(255) NOT NULL,
    company_business_size VARCHAR(255) NOT NULL,
    region_name VARCHAR(255) NOT NULL,
    status_name VARCHAR(255) NOT NULL,
    company_name VARCHAR NULL,
    company_code VARCHAR NULL,
    company_inn VARCHAR NULL,
    company_kpp VARCHAR NULL,
    company_ogrn VARCHAR NULL,
    company_email VARCHAR NULL,
    company_phone VARCHAR NULL,
    company_hr_agency VARCHAR NULL,
    company_site VARCHAR NULL,
    company_url VARCHAR NULL,
    latitude VARCHAR NULL,
    longitude VARCHAR NULL,
    education_type VARCHAR(100) NOT NULL,
    contact_type VARCHAR NOT NULL,
    contact_value VARCHAR NOT NULL,
    salary_min NUMERIC(15, 2),
    salary_max NUMERIC(15, 2),
    vacancy_url VARCHAR(500),
    date_published DATE,
    creation_date DATE,
    dt_write_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT dm_vacancies_vacancy_id_unique UNIQUE (vacancy_id)
);

COMMENT ON TABLE dm.vacancies IS 'Денормализованная таблица вакансий в витрине данных (DM слой)';

COMMENT ON COLUMN dm.vacancies.id IS 'Суррогатный первичный ключ вакансии';
COMMENT ON COLUMN dm.vacancies.vacancy_id IS 'Внешний идентификатор вакансии из системы-источника';
COMMENT ON COLUMN dm.vacancies.vacancy_name IS 'Наименование вакансии';
COMMENT ON COLUMN dm.vacancies.professional_sphere_name IS 'Профессиональная сфера';
COMMENT ON COLUMN dm.vacancies.schedule_type IS 'График работы';
COMMENT ON COLUMN dm.vacancies.experience_requirements IS 'Требуемый опыт работы';
COMMENT ON COLUMN dm.vacancies.source_type IS 'Источник вакансии';
COMMENT ON COLUMN dm.vacancies.company_business_size IS 'Размер компании';
COMMENT ON COLUMN dm.vacancies.region_name IS 'Регион размещения вакансии';
COMMENT ON COLUMN dm.vacancies.status_name IS 'Статус вакансии';
COMMENT ON COLUMN dm.vacancies.company_name IS 'Наименование компании';
COMMENT ON COLUMN dm.vacancies.company_code IS 'Код компании';
COMMENT ON COLUMN dm.vacancies.company_inn IS 'ИНН компании';
COMMENT ON COLUMN dm.vacancies.company_kpp IS 'КПП компании';
COMMENT ON COLUMN dm.vacancies.company_ogrn IS 'ОГРН компании';
COMMENT ON COLUMN dm.vacancies.company_email IS 'Email компании';
COMMENT ON COLUMN dm.vacancies.company_phone IS 'Телефон компании';
COMMENT ON COLUMN dm.vacancies.company_hr_agency IS 'HR-агентство';
COMMENT ON COLUMN dm.vacancies.company_site IS 'Сайт компании';
COMMENT ON COLUMN dm.vacancies.company_url IS 'URL профиля компании';
COMMENT ON COLUMN dm.vacancies.latitude IS 'Географическая широта';
COMMENT ON COLUMN dm.vacancies.longitude IS 'Географическая долгота';
COMMENT ON COLUMN dm.vacancies.education_type IS 'Требуемый уровень образования';
COMMENT ON COLUMN dm.vacancies.contact_type IS 'Тип контактной информации';
COMMENT ON COLUMN dm.vacancies.contact_value IS 'Контактная информация';
COMMENT ON COLUMN dm.vacancies.salary_min IS 'Минимальный уровень зарплаты';
COMMENT ON COLUMN dm.vacancies.salary_max IS 'Максимальный уровень зарплаты';
COMMENT ON COLUMN dm.vacancies.vacancy_url IS 'URL вакансии';
COMMENT ON COLUMN dm.vacancies.date_published IS 'Дата публикации вакансии';
COMMENT ON COLUMN dm.vacancies.creation_date IS 'Дата создания записи';
COMMENT ON COLUMN dm.vacancies.dt_write_at IS 'Дата последнего обновления';
