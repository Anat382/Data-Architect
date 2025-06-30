-- Загрузка данных в справочники DDS слоя

-- 1. dict_schedule_type
--TRUNCATE TABLE dds.dict_schedule_type RESTART IDENTITY;
SELECT dds.load_dict_schedule_type();
-- SELECT * FROM dds.dict_schedule_type;

-- 2. dict_experience_requirements
--TRUNCATE TABLE dds.dict_experience_requirements RESTART IDENTITY;
SELECT dds.load_dict_experience_requirements();
-- SELECT * FROM dds.dict_experience_requirements;

-- 3. dict_source_type
--TRUNCATE TABLE dds.dict_source_type RESTART IDENTITY;
SELECT dds.load_dict_source_type();
-- SELECT * FROM dds.dict_source_type;

-- 4. dict_company_business_size
--TRUNCATE TABLE dds.dict_company_business_size RESTART IDENTITY;
SELECT dds.load_dict_company_business_size();
-- SELECT * FROM dds.dict_company_business_size;

-- 5. dict_vacancies_name
--TRUNCATE TABLE dds.dict_vacancies_name RESTART IDENTITY;
SELECT dds.load_dict_vacancies_name();
-- SELECT * FROM dds.dict_vacancies_name;

-- 6. dict_region_name
--TRUNCATE TABLE dds.dict_region_name RESTART IDENTITY;
SELECT dds.load_dict_region_name();
-- SELECT * FROM dds.dict_region_name;

-- 7. dict_status_name
--TRUNCATE TABLE dds.dict_status_name RESTART IDENTITY;
SELECT dds.load_dict_status_name();
-- SELECT * FROM dds.dict_status_name;

-- 8. dict_company
--TRUNCATE TABLE dds.dict_company RESTART IDENTITY;
SELECT dds.load_dict_company();
-- SELECT * FROM dds.dict_company;

-- 9. dict_geo
--TRUNCATE TABLE dds.dict_geo RESTART IDENTITY;
SELECT dds.load_dict_geo();
-- SELECT * FROM dds.dict_geo;

-- 10. dict_contact_type
--TRUNCATE TABLE dds.dict_contact_type RESTART IDENTITY;
SELECT dds.load_dict_contact_type();
-- SELECT * FROM dds.dict_contact_type;

-- 11. dict_education_type
--TRUNCATE TABLE dds.dict_education_type RESTART IDENTITY;
SELECT dds.load_dict_education_type();
-- SELECT * FROM dds.dict_education_type;

-- 12. dict_vacancies_id
--TRUNCATE TABLE dds.dict_vacancies_id RESTART IDENTITY;
SELECT dds.load_dict_vacancies_id();
-- SELECT * FROM dds.dict_vacancies_id;


-- Очистка таблицы
--TRUNCATE TABLE dds.contacts RESTART IDENTITY;
-- Загрузка данных
SELECT dds.load_contacts();
-- Проверка результатов
-- SELECT * FROM dds.contacts LIMIT 100;


-- Очистка таблицы
--TRUNCATE TABLE dds.vacancies RESTART IDENTITY;

-- Загрузка данных
SELECT dds.load_vacancies();
-- Проверка результатов
-- SELECT * FROM dds.vacancies where id_contact_type is not null LIMIT 100;


-- Очистка таблицы
--TRUNCATE TABLE dm.vacancies RESTART IDENTITY;
-- Загрузка данных
SELECT dm.load_vacancies();

-- Проверка результатов
-- SELECT * FROM dm.vacancies LIMIT 100;