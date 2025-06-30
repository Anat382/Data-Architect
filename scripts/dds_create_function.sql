
CREATE OR REPLACE FUNCTION dds.load_dict_professional_sphere_name()
RETURNS TEXT AS $$
DECLARE
    inserted_rows INTEGER;
    existing_rows INTEGER;
    error_message TEXT;
BEGIN
    -- Проверяем существование исходной таблицы
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables 
                  WHERE table_schema = 'stg' AND table_name = 'vacancies') THEN
        RETURN 'Ошибка: Исходная таблица stg.vacancies не существует';
    END IF;

    -- Проверяем существование целевой таблицы
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables 
                  WHERE table_schema = 'dds' AND table_name = 'dict_professional_sphere_name') THEN
        RETURN 'Ошибка: Целевая таблица dds.dict_professional_sphere_name не существует';
    END IF;

    BEGIN
        -- Вставляем только уникальные значения, которых еще нет в целевой таблице
        WITH row_spheres AS (
			SELECT DISTINCT 
				case when professional_sphere_name = '' then 'Не указано' else professional_sphere_name end AS professional_sphere_name
            FROM stg.vacancies sr
        ),
		new_spheres as (
			SELECT professional_sphere_name
            FROM row_spheres sr
            WHERE NOT exists (
                  SELECT 1 
                  FROM dds.dict_professional_sphere_name tg
				  WHERE sr.professional_sphere_name = tg.professional_sphere_name
              )

		)
        INSERT INTO dds.dict_professional_sphere_name (professional_sphere_name)
        SELECT professional_sphere_name FROM new_spheres;
        
        GET DIAGNOSTICS inserted_rows = ROW_COUNT;
        
        RETURN 'Успешно. Вставлено ' || inserted_rows || ' новых записей. ';
        
    EXCEPTION
        WHEN OTHERS THEN
            error_message := 'Ошибка при загрузке данных: ' || SQLERRM;
            RETURN error_message;
    END;
END;
$$ LANGUAGE plpgsql;



-- Функция для загрузки данных в dds.dict_schedule_type
CREATE OR REPLACE FUNCTION dds.load_dict_schedule_type()
RETURNS TEXT AS $$
DECLARE
    inserted_rows INTEGER;
    error_message TEXT;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables 
                  WHERE table_schema = 'stg' AND table_name = 'vacancies') THEN
        RETURN 'Ошибка: Исходная таблица stg.vacancies не существует';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.tables 
                  WHERE table_schema = 'dds' AND table_name = 'dict_schedule_type') THEN
        RETURN 'Ошибка: Целевая таблица dds.dict_schedule_type не существует';
    END IF;

    BEGIN
        WITH row_data AS (
            SELECT DISTINCT 
                CASE WHEN schedule_type = '' THEN 'Не указано' ELSE schedule_type END AS schedule_type
            FROM stg.vacancies
        ),
        new_data AS (
            SELECT schedule_type
            FROM row_data
            WHERE NOT EXISTS (
                SELECT 1 
                FROM dds.dict_schedule_type tg
                WHERE row_data.schedule_type = tg.schedule_type
            )
        )
        INSERT INTO dds.dict_schedule_type (schedule_type)
        SELECT schedule_type FROM new_data;
        
        GET DIAGNOSTICS inserted_rows = ROW_COUNT;
        
        RETURN 'Успешно. Вставлено ' || inserted_rows || ' новых записей в dict_schedule_type.';
        
    EXCEPTION
        WHEN OTHERS THEN
            error_message := 'Ошибка при загрузке данных в dict_schedule_type: ' || SQLERRM;
            RETURN error_message;
    END;
END;
$$ LANGUAGE plpgsql;

-- Функция для загрузки данных в dds.dict_experience_requirements
CREATE OR REPLACE FUNCTION dds.load_dict_experience_requirements()
RETURNS TEXT AS $$
DECLARE
    inserted_rows INTEGER;
    error_message TEXT;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables 
                  WHERE table_schema = 'stg' AND table_name = 'vacancies') THEN
        RETURN 'Ошибка: Исходная таблица stg.vacancies не существует';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.tables 
                  WHERE table_schema = 'dds' AND table_name = 'dict_experience_requirements') THEN
        RETURN 'Ошибка: Целевая таблица dds.dict_experience_requirements не существует';
    END IF;

    BEGIN
        WITH row_data AS (
            SELECT DISTINCT 
                CASE WHEN experience_requirements = '' THEN 'Не указано' ELSE experience_requirements END AS experience_requirements
            FROM stg.vacancies
        ),
        new_data AS (
            SELECT experience_requirements
            FROM row_data
            WHERE NOT EXISTS (
                SELECT 1 
                FROM dds.dict_experience_requirements tg
                WHERE row_data.experience_requirements = tg.experience_requirements
            )
        )
        INSERT INTO dds.dict_experience_requirements (experience_requirements)
        SELECT experience_requirements FROM new_data;
        
        GET DIAGNOSTICS inserted_rows = ROW_COUNT;
        
        RETURN 'Успешно. Вставлено ' || inserted_rows || ' новых записей в dict_experience_requirements.';
        
    EXCEPTION
        WHEN OTHERS THEN
            error_message := 'Ошибка при загрузке данных в dict_experience_requirements: ' || SQLERRM;
            RETURN error_message;
    END;
END;
$$ LANGUAGE plpgsql;

-- Функция для загрузки данных в dds.dict_source_type
CREATE OR REPLACE FUNCTION dds.load_dict_source_type()
RETURNS TEXT AS $$
DECLARE
    inserted_rows INTEGER;
    error_message TEXT;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables 
                  WHERE table_schema = 'stg' AND table_name = 'vacancies') THEN
        RETURN 'Ошибка: Исходная таблица stg.vacancies не существует';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.tables 
                  WHERE table_schema = 'dds' AND table_name = 'dict_source_type') THEN
        RETURN 'Ошибка: Целевая таблица dds.dict_source_type не существует';
    END IF;

    BEGIN
        WITH row_data AS (
            SELECT DISTINCT 
                CASE WHEN source_type = '' THEN 'Не указано' ELSE source_type END AS source_type
            FROM stg.vacancies
        ),
        new_data AS (
            SELECT source_type
            FROM row_data
            WHERE NOT EXISTS (
                SELECT 1 
                FROM dds.dict_source_type tg
                WHERE row_data.source_type = tg.source_type
            )
        )
        INSERT INTO dds.dict_source_type (source_type)
        SELECT source_type FROM new_data;
        
        GET DIAGNOSTICS inserted_rows = ROW_COUNT;
        
        RETURN 'Успешно. Вставлено ' || inserted_rows || ' новых записей в dict_source_type.';
        
    EXCEPTION
        WHEN OTHERS THEN
            error_message := 'Ошибка при загрузке данных в dict_source_type: ' || SQLERRM;
            RETURN error_message;
    END;
END;
$$ LANGUAGE plpgsql;

-- Функция для загрузки данных в dds.dict_company_business_size
CREATE OR REPLACE FUNCTION dds.load_dict_company_business_size()
RETURNS TEXT AS $$
DECLARE
    inserted_rows INTEGER;
    error_message TEXT;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables 
                  WHERE table_schema = 'stg' AND table_name = 'vacancies') THEN
        RETURN 'Ошибка: Исходная таблица stg.vacancies не существует';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.tables 
                  WHERE table_schema = 'dds' AND table_name = 'dict_company_business_size') THEN
        RETURN 'Ошибка: Целевая таблица dds.dict_company_business_size не существует';
    END IF;

    BEGIN
        WITH row_data AS (
            SELECT DISTINCT 
                CASE WHEN company_business_size = '' THEN 'Не указано' ELSE company_business_size END AS company_business_size
            FROM stg.vacancies
        ),
        new_data AS (
            SELECT company_business_size
            FROM row_data
            WHERE NOT EXISTS (
                SELECT 1 
                FROM dds.dict_company_business_size tg
                WHERE row_data.company_business_size = tg.company_business_size
            )
        )
        INSERT INTO dds.dict_company_business_size (company_business_size)
        SELECT company_business_size FROM new_data;
        
        GET DIAGNOSTICS inserted_rows = ROW_COUNT;
        
        RETURN 'Успешно. Вставлено ' || inserted_rows || ' новых записей в dict_company_business_size.';
        
    EXCEPTION
        WHEN OTHERS THEN
            error_message := 'Ошибка при загрузке данных в dict_company_business_size: ' || SQLERRM;
            RETURN error_message;
    END;
END;
$$ LANGUAGE plpgsql;

-- Функция для загрузки данных в dds.dict_vacancies_name
CREATE OR REPLACE FUNCTION dds.load_dict_vacancies_name()
RETURNS TEXT AS $$
DECLARE
    inserted_rows INTEGER;
    error_message TEXT;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables 
                  WHERE table_schema = 'stg' AND table_name = 'vacancies') THEN
        RETURN 'Ошибка: Исходная таблица stg.vacancies не существует';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.tables 
                  WHERE table_schema = 'dds' AND table_name = 'dict_vacancies_name') THEN
        RETURN 'Ошибка: Целевая таблица dds.dict_vacancies_name не существует';
    END IF;

    BEGIN
        WITH row_data AS (
            SELECT DISTINCT 
                CASE WHEN vacancy_name = '' THEN 'Не указано' ELSE vacancy_name END AS vacancy_name
            FROM stg.vacancies
        ),
        new_data AS (
            SELECT vacancy_name
            FROM row_data
            WHERE NOT EXISTS (
                SELECT 1 
                FROM dds.dict_vacancies_name tg
                WHERE row_data.vacancy_name = tg.vacancy_name
            )
        )
        INSERT INTO dds.dict_vacancies_name (vacancy_name)
        SELECT vacancy_name FROM new_data;
        
        GET DIAGNOSTICS inserted_rows = ROW_COUNT;
        
        RETURN 'Успешно. Вставлено ' || inserted_rows || ' новых записей в dict_vacancies_name.';
        
    EXCEPTION
        WHEN OTHERS THEN
            error_message := 'Ошибка при загрузке данных в dict_vacancies_name: ' || SQLERRM;
            RETURN error_message;
    END;
END;
$$ LANGUAGE plpgsql;

-- Функция для загрузки данных в dds.dict_region_name
CREATE OR REPLACE FUNCTION dds.load_dict_region_name()
RETURNS TEXT AS $$
DECLARE
    inserted_rows INTEGER;
    error_message TEXT;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables 
                  WHERE table_schema = 'stg' AND table_name = 'vacancies') THEN
        RETURN 'Ошибка: Исходная таблица stg.vacancies не существует';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.tables 
                  WHERE table_schema = 'dds' AND table_name = 'dict_region_name') THEN
        RETURN 'Ошибка: Целевая таблица dds.dict_region_name не существует';
    END IF;

    BEGIN
        WITH row_data AS (
            SELECT DISTINCT 
                CASE WHEN region_name = '' THEN 'Не указано' ELSE region_name END AS region_name
            FROM stg.vacancies
        ),
        new_data AS (
            SELECT region_name
            FROM row_data
            WHERE NOT EXISTS (
                SELECT 1 
                FROM dds.dict_region_name tg
                WHERE row_data.region_name = tg.region_name
            )
        )
        INSERT INTO dds.dict_region_name (region_name)
        SELECT region_name FROM new_data;
        
        GET DIAGNOSTICS inserted_rows = ROW_COUNT;
        
        RETURN 'Успешно. Вставлено ' || inserted_rows || ' новых записей в dict_region_name.';
        
    EXCEPTION
        WHEN OTHERS THEN
            error_message := 'Ошибка при загрузке данных в dict_region_name: ' || SQLERRM;
            RETURN error_message;
    END;
END;
$$ LANGUAGE plpgsql;

-- Функция для загрузки данных в dds.dict_status_name
CREATE OR REPLACE FUNCTION dds.load_dict_status_name()
RETURNS TEXT AS $$
DECLARE
    inserted_rows INTEGER;
    error_message TEXT;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables 
                  WHERE table_schema = 'stg' AND table_name = 'vacancies') THEN
        RETURN 'Ошибка: Исходная таблица stg.vacancies не существует';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.tables 
                  WHERE table_schema = 'dds' AND table_name = 'dict_status_name') THEN
        RETURN 'Ошибка: Целевая таблица dds.dict_status_name не существует';
    END IF;

    BEGIN
        WITH row_data AS (
            SELECT DISTINCT 
                CASE WHEN status = '' THEN 'Не указано' ELSE status END AS status_name
            FROM stg.vacancies
        ),
        new_data AS (
            SELECT status_name
            FROM row_data
            WHERE NOT EXISTS (
                SELECT 1 
                FROM dds.dict_status_name tg
                WHERE row_data.status_name = tg.status_name
            )
        )
        INSERT INTO dds.dict_status_name (status_name)
        SELECT status_name FROM new_data;
        
        GET DIAGNOSTICS inserted_rows = ROW_COUNT;
        
        RETURN 'Успешно. Вставлено ' || inserted_rows || ' новых записей в dict_status_name.';
        
    EXCEPTION
        WHEN OTHERS THEN
            error_message := 'Ошибка при загрузке данных в dict_status_name: ' || SQLERRM;
            RETURN error_message;
    END;
END;
$$ LANGUAGE plpgsql;

-- Функция для загрузки данных в dds.dict_company
CREATE OR REPLACE FUNCTION dds.load_dict_company()
RETURNS TEXT AS $$
DECLARE
    inserted_rows INTEGER;
    error_message TEXT;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables 
                  WHERE table_schema = 'stg' AND table_name = 'vacancies') THEN
        RETURN 'Ошибка: Исходная таблица stg.vacancies не существует';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.tables 
                  WHERE table_schema = 'dds' AND table_name = 'dict_company') THEN
        RETURN 'Ошибка: Целевая таблица dds.dict_company не существует';
    END IF;

    BEGIN
        WITH row_data AS (
            SELECT DISTINCT 
                name,
                company_code,
                inn,
                kpp,
                ogrn,
                email,
                phone,
                hr_agency,
                site,
                url
            FROM stg.company
        ),
        new_data AS (
            SELECT 
                name,
                company_code,
                inn,
                kpp,
                ogrn,
                email,
                phone,
                hr_agency,
                site,
                url
            FROM row_data
            WHERE NOT EXISTS (
                SELECT 1 
                FROM dds.dict_company tg
                WHERE COALESCE(row_data.company_code, '') = COALESCE(tg.company_code, '')
            )
        )
        INSERT INTO dds.dict_company (
            name, company_code, inn, kpp, ogrn, 
            email, phone, hr_agency, site, url
        )
        SELECT 
            name, company_code, inn, kpp, ogrn, 
            email, phone, hr_agency, site, url
        FROM new_data;
        
        GET DIAGNOSTICS inserted_rows = ROW_COUNT;
        
        RETURN 'Успешно. Вставлено ' || inserted_rows || ' новых записей в dict_company.';
        
    EXCEPTION
        WHEN OTHERS THEN
            error_message := 'Ошибка при загрузке данных в dict_company: ' || SQLERRM;
            RETURN error_message;
    END;
END;
$$ LANGUAGE plpgsql;

-- Функция для загрузки данных в dds.dict_geo
CREATE OR REPLACE FUNCTION dds.load_dict_geo()
RETURNS TEXT AS $$
DECLARE
    inserted_rows INTEGER;
    error_message TEXT;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables 
                  WHERE table_schema = 'stg' AND table_name = 'vacancies') THEN
        RETURN 'Ошибка: Исходная таблица stg.vacancies не существует';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.tables 
                  WHERE table_schema = 'dds' AND table_name = 'dict_geo') THEN
        RETURN 'Ошибка: Целевая таблица dds.dict_geo не существует';
    END IF;

    BEGIN
        WITH row_data AS (
            SELECT DISTINCT 
                latitude,
                longitude
            FROM stg.geo
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        ),
        new_data AS (
            SELECT 
                latitude,
                longitude
            FROM row_data
            WHERE NOT EXISTS (
                SELECT 1 
                FROM dds.dict_geo tg
                WHERE row_data.latitude = tg.latitude
                AND row_data.longitude = tg.longitude
            )
        )
        INSERT INTO dds.dict_geo (latitude, longitude)
        SELECT latitude, longitude FROM new_data;
        
        GET DIAGNOSTICS inserted_rows = ROW_COUNT;
        
        RETURN 'Успешно. Вставлено ' || inserted_rows || ' новых записей в dict_geo.';
        
    EXCEPTION
        WHEN OTHERS THEN
            error_message := 'Ошибка при загрузке данных в dict_geo: ' || SQLERRM;
            RETURN error_message;
    END;
END;
$$ LANGUAGE plpgsql;

-- Функция для загрузки данных в dds.dict_contact_type
CREATE OR REPLACE FUNCTION dds.load_dict_contact_type()
RETURNS TEXT AS $$
DECLARE
    inserted_rows INTEGER;
    error_message TEXT;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables 
                  WHERE table_schema = 'stg' AND table_name = 'vacancies') THEN
        RETURN 'Ошибка: Исходная таблица stg.vacancies не существует';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.tables 
                  WHERE table_schema = 'dds' AND table_name = 'dict_contact_type') THEN
        RETURN 'Ошибка: Целевая таблица dds.dict_contact_type не существует';
    END IF;

    BEGIN
        WITH row_data AS (
            SELECT DISTINCT 
                CASE WHEN contact_type = '' THEN 'Не указано' ELSE contact_type END AS contact_type
            FROM stg.contacts
            WHERE contact_type IS NOT NULL
        ),
        new_data AS (
            SELECT contact_type
            FROM row_data
            WHERE NOT EXISTS (
                SELECT 1 
                FROM dds.dict_contact_type tg
                WHERE row_data.contact_type = tg.contact_type
            )
        )
        INSERT INTO dds.dict_contact_type (contact_type)
        SELECT contact_type FROM new_data;
        
        GET DIAGNOSTICS inserted_rows = ROW_COUNT;
        
        RETURN 'Успешно. Вставлено ' || inserted_rows || ' новых записей в dict_contact_type.';
        
    EXCEPTION
        WHEN OTHERS THEN
            error_message := 'Ошибка при загрузке данных в dict_contact_type: ' || SQLERRM;
            RETURN error_message;
    END;
END;
$$ LANGUAGE plpgsql;

-- Функция для загрузки данных в dds.dict_education_type
CREATE OR REPLACE FUNCTION dds.load_dict_education_type()
RETURNS TEXT AS $$
DECLARE
    inserted_rows INTEGER;
    error_message TEXT;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables 
                  WHERE table_schema = 'stg' AND table_name = 'vacancies') THEN
        RETURN 'Ошибка: Исходная таблица stg.vacancies не существует';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.tables 
                  WHERE table_schema = 'dds' AND table_name = 'dict_education_type') THEN
        RETURN 'Ошибка: Целевая таблица dds.dict_education_type не существует';
    END IF;

    BEGIN
        WITH row_data AS (
            SELECT DISTINCT 
                CASE WHEN education_type = '' THEN 'Не указано' ELSE education_type END AS education_type
            FROM stg.education
        ),
        new_data AS (
            SELECT education_type
            FROM row_data
            WHERE NOT EXISTS (
                SELECT 1 
                FROM dds.dict_education_type tg
                WHERE row_data.education_type = tg.education_type
            )
        )
        INSERT INTO dds.dict_education_type (education_type)
        SELECT education_type FROM new_data;
        
        GET DIAGNOSTICS inserted_rows = ROW_COUNT;
        
        RETURN 'Успешно. Вставлено ' || inserted_rows || ' новых записей в dict_education_type.';
        
    EXCEPTION
        WHEN OTHERS THEN
            error_message := 'Ошибка при загрузке данных в dict_education_type: ' || SQLERRM;
            RETURN error_message;
    END;
END;
$$ LANGUAGE plpgsql;


-- Функция для загрузки данных в dds.dict_vacancies_id
CREATE OR REPLACE FUNCTION dds.load_dict_vacancies_id()
RETURNS TEXT AS $$
DECLARE
    inserted_rows INTEGER;
    error_message TEXT;
BEGIN
    -- Проверяем существование исходной таблицы
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables 
                  WHERE table_schema = 'stg' AND table_name = 'vacancies') THEN
        RETURN 'Ошибка: Исходная таблица stg.vacancies не существует';
    END IF;

    -- Проверяем существование целевой таблицы
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables 
                  WHERE table_schema = 'dds' AND table_name = 'dict_vacancies_id') THEN
        RETURN 'Ошибка: Целевая таблица dds.dict_vacancies_id не существует';
    END IF;

    BEGIN
        -- Вставляем только уникальные vacancy_id, которых еще нет в целевой таблице
        WITH row_data AS (
            SELECT DISTINCT 
                CASE WHEN vacancy_id = '' THEN 'Не указано' ELSE vacancy_id END AS vacancy_id
            FROM stg.vacancies
        ),
        new_data AS (
            SELECT vacancy_id
            FROM row_data
            WHERE NOT EXISTS (
                SELECT 1 
                FROM dds.dict_vacancies_id tg
                WHERE row_data.vacancy_id = tg.vacancy_id
            )
        )
        INSERT INTO dds.dict_vacancies_id (vacancy_id)
        SELECT vacancy_id FROM new_data;
        
        GET DIAGNOSTICS inserted_rows = ROW_COUNT;
        
        RETURN 'Успешно. Вставлено ' || inserted_rows || ' новых записей в dict_vacancies_id.';
        
    EXCEPTION
        WHEN OTHERS THEN
            error_message := 'Ошибка при загрузке данных в dict_vacancies_id: ' || SQLERRM;
            RETURN error_message;
    END;
END;
$$ LANGUAGE plpgsql;


--drop FUNCTION dds.f_load_contacts;
CREATE OR REPLACE FUNCTION dds.load_contacts()
RETURNS TEXT AS $$
DECLARE
    inserted_rows INTEGER;
    error_message TEXT;
BEGIN
    -- Проверяем существование исходной таблицы
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables 
                  WHERE table_schema = 'stg' AND table_name = 'contacts') THEN
        RETURN 'Ошибка: Исходная таблица stg.contacts не существует';
    END IF;

    -- Проверяем существование связанных справочников
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables 
                  WHERE table_schema = 'dds' AND table_name = 'dict_vacancies_id') THEN
        RETURN 'Ошибка: Целевая таблица dds.dict_vacancies_id не существует';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables 
                  WHERE table_schema = 'dds' AND table_name = 'dict_contact_type') THEN
        RETURN 'Ошибка: Целевая таблица dds.dict_contact_type не существует';
    END IF;

    BEGIN
        -- Вставляем только новые уникальные контакты
        WITH source_data AS (
            SELECT distinct
                v.id AS vacancy_id,
                ct.id AS contact_type_id,
                case when sc.contact_value = '' then 'Не указано' else sc.contact_value end contact_value
--                sc.dt_write_at
            FROM stg.contacts sc
            JOIN dds.dict_vacancies_id v ON sc.vacancy_id = v.vacancy_id
            JOIN dds.dict_contact_type ct ON sc.contact_type = ct.contact_type
			where sc.contact_type in( 'Телефон')
        ),
        new_contacts AS (
            SELECT 
                vacancy_id,
                contact_type_id,
                contact_value
            FROM source_data sd
            WHERE NOT EXISTS (
                SELECT 1 
                FROM dds.contacts c
                WHERE c.id_vacancies = sd.vacancy_id
                  AND c.id_contact_type = sd.contact_type_id
                  AND c.contact_value = sd.contact_value
            )
        )
        INSERT INTO dds.contacts (
            id_vacancies,
            id_contact_type,
            contact_value
        )
        SELECT 
            vacancy_id,
            contact_type_id,
            contact_value
        FROM new_contacts;
        
        GET DIAGNOSTICS inserted_rows = ROW_COUNT;
        
        RETURN 'Успешно. Вставлено ' || inserted_rows || ' новых контактов.';
        
    EXCEPTION
        WHEN OTHERS THEN
            error_message := 'Ошибка при загрузке контактов: ' || SQLERRM;
            RETURN error_message;
    END;
END;
$$ LANGUAGE plpgsql;



CREATE OR REPLACE FUNCTION dds.load_vacancies()
RETURNS TEXT AS $$
DECLARE
    inserted_rows INTEGER;
    error_message TEXT;
BEGIN
    -- Проверяем существование исходной таблицы
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables 
                  WHERE table_schema = 'stg' AND table_name = 'vacancies') THEN
        RETURN 'Ошибка: Исходная таблица stg.vacancies не существует';
    END IF;

    -- Проверяем существование всех необходимых справочников
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables 
                  WHERE table_schema = 'dds' AND table_name = 'dict_vacancies_id') THEN
        RETURN 'Ошибка: Справочник dict_vacancies_id не существует';
    END IF;
    
    -- Аналогичные проверки для других справочников...

    BEGIN

		TRUNCATE TABLE dm.vacancies RESTART IDENTITY;
        -- Вставляем данные в таблицу вакансий
        WITH vacancy_data AS (
            SELECT 
                vi.id AS vacancy_id,
                vn.id AS vacancy_name_id,
                ps.id AS professional_sphere_id,
                st.id AS schedule_type_id,
                er.id AS experience_id,
                sr.id AS source_id,
                cbs.id AS company_size_id,
                rn.id AS region_id,
                sn.id AS status_id,
                c.id AS company_id,
                g.id AS geo_id,
                et.id AS education_id,
				COALESCE(cont.id_contact_type,2) id_contact_type,
                sv.salary_min,
                case when sv.salary_max = 0 then sv.salary_min else sv.salary_max end salary_max,
                sv.vacancy_url,
                sv.date_published::TIMESTAMP WITH TIME zone date_published,
                sv.creation_date::TIMESTAMP WITH TIME zone creation_date
            FROM stg.vacancies sv
			JOIN stg.company sc 
				on sc.vacancy_id = sv.vacancy_id
			JOIN stg.education se
				on se.vacancy_id = sv.vacancy_id
			JOIN stg.geo sg
				on sg.vacancy_id = sv.vacancy_id
            JOIN dds.dict_vacancies_id vi ON sv.vacancy_id = vi.vacancy_id
			join dds.contacts cont on 
				cont.id_vacancies = vi.id
            JOIN dds.dict_vacancies_name vn ON 
                CASE WHEN sv.vacancy_name = '' THEN 'Не указано' ELSE sv.vacancy_name END = vn.vacancy_name
            JOIN dds.dict_professional_sphere_name ps ON 
                CASE WHEN sv.professional_sphere_name = '' THEN 'Не указано' ELSE sv.professional_sphere_name END = ps.professional_sphere_name
            JOIN dds.dict_schedule_type st ON 
                CASE WHEN sv.schedule_type = '' THEN 'Не указано' ELSE sv.schedule_type END = st.schedule_type
            JOIN dds.dict_experience_requirements er ON 
                CASE WHEN sv.experience_requirements = '' THEN 'Не указано' ELSE sv.experience_requirements END = er.experience_requirements
            JOIN dds.dict_source_type sr ON 
                CASE WHEN sv.source_type = '' THEN 'Не указано' ELSE sv.source_type END = sr.source_type
            JOIN dds.dict_company_business_size cbs ON 
                CASE WHEN sv.company_business_size = '' THEN 'Не указано' ELSE sv.company_business_size END = cbs.company_business_size
            JOIN dds.dict_region_name rn ON 
                CASE WHEN sv.region_name = '' THEN 'Не указано' ELSE sv.region_name END = rn.region_name
            JOIN dds.dict_status_name sn ON 
                CASE WHEN sv.status = '' THEN 'Не указано' ELSE sv.status END = sn.status_name
            JOIN dds.dict_company c ON COALESCE(sc."name", 'Не указано') = COALESCE(c."name", 'Не указано')
				and COALESCE(sc.company_code, '') = COALESCE(c.company_code, '')
				and COALESCE(sc.inn, '') = COALESCE(c.inn, '')
            JOIN dds.dict_geo g ON sg.latitude = g.latitude 
				AND sg.longitude = g.longitude
            JOIN dds.dict_education_type et ON 
                CASE WHEN se.education_type = '' THEN 'Не указано' ELSE se.education_type END = et.education_type
        ),
        new_vacancies AS (
            SELECT 
                vacancy_id,
                vacancy_name_id,
                professional_sphere_id,
                schedule_type_id,
                experience_id,
                source_id,
                company_size_id,
                region_id,
                status_id,
                company_id,
                geo_id,
                education_id,
				id_contact_type,
                salary_min,
                salary_max,
                vacancy_url,
                date_published,
                creation_date
            FROM vacancy_data vd
            WHERE NOT EXISTS (
                SELECT 1 
                FROM dds.vacancies v
                WHERE v.id_vacancy = vd.vacancy_id
            )
        )
        INSERT INTO dds.vacancies (
            id_vacancy,
            id_vacancy_name,
            id_professional_sphere,
            id_schedule_type,
            id_experience_requirements,
            id_source_type,
            id_company_business_size,
            id_region_name,
            id_status_name,
            id_company,
            id_geo,
            id_education_type,
			id_contact_type,
            salary_min,
            salary_max,
            vacancy_url,
            date_published,
            creation_date
        )
        SELECT 
            vacancy_id,
            vacancy_name_id,
            professional_sphere_id,
            schedule_type_id,
            experience_id,
            source_id,
            company_size_id,
            region_id,
            status_id,
            company_id,
            geo_id,
            education_id,
			id_contact_type,
            salary_min,
            salary_max,
            vacancy_url,
            date_published,
            creation_date
        FROM new_vacancies;
        
        GET DIAGNOSTICS inserted_rows = ROW_COUNT;
        
        RETURN 'Успешно. Вставлено ' || inserted_rows || ' новых вакансий.';
        
    EXCEPTION
        WHEN OTHERS THEN
            error_message := 'Ошибка при загрузке вакансий: ' || SQLERRM;
            RETURN error_message;
    END;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION dm.load_vacancies()
RETURNS TEXT AS $$
DECLARE
    inserted_rows INTEGER;
    error_message TEXT;
BEGIN
    -- Проверяем существование исходных таблиц
    IF NOT EXISTS (SELECT 1 FROM information_schema.tables 
                  WHERE table_schema = 'dds' AND table_name = 'vacancies') THEN
        RETURN 'Ошибка: Исходная таблица dds.vacancies не существует';
    END IF;

    BEGIN
        -- Вставляем данные с подстановкой значений вместо ID
        INSERT INTO dm.vacancies (
            vacancy_id,
            vacancy_name,
            professional_sphere_name,
            schedule_type,
            experience_requirements,
            source_type,
            company_business_size,
            region_name,
            status_name,
            company_name,
            company_code,
            company_inn,
            company_kpp,
            company_ogrn,
            company_email,
            company_phone,
            company_hr_agency,
            company_site,
            company_url,
            latitude,
            longitude,
            education_type,
            contact_type,
            contact_value,
            salary_min,
            salary_max,
            vacancy_url,
            date_published,
            creation_date
        )
        SELECT 
            vi.vacancy_id,
            vn.vacancy_name,
            ps.professional_sphere_name,
            st.schedule_type,
            er.experience_requirements,
            sr.source_type,
            cbs.company_business_size,
            rn.region_name,
            sn.status_name,
            c.name AS company_name,
            c.company_code,
            c.inn AS company_inn,
            c.kpp AS company_kpp,
            c.ogrn AS company_ogrn,
            c.email AS company_email,
            c.phone AS company_phone,
            c.hr_agency AS company_hr_agency,
            c.site AS company_site,
            c.url AS company_url,
            g.latitude,
            g.longitude,
            et.education_type,
            ct.contact_type,
            co.contact_value,
            v.salary_min,
            v.salary_max,
            v.vacancy_url,
            v.date_published::date date_published,
            v.creation_date::date creation_date
        FROM dds.vacancies v
        JOIN dds.dict_vacancies_id vi ON v.id_vacancy = vi.id
        JOIN dds.dict_vacancies_name vn ON v.id_vacancy_name = vn.id
        JOIN dds.dict_professional_sphere_name ps ON v.id_professional_sphere = ps.id
        JOIN dds.dict_schedule_type st ON v.id_schedule_type = st.id
        JOIN dds.dict_experience_requirements er ON v.id_experience_requirements = er.id
        JOIN dds.dict_source_type sr ON v.id_source_type = sr.id
        JOIN dds.dict_company_business_size cbs ON v.id_company_business_size = cbs.id
        JOIN dds.dict_region_name rn ON v.id_region_name = rn.id
        JOIN dds.dict_status_name sn ON v.id_status_name = sn.id
        JOIN dds.dict_company c ON v.id_company = c.id
        JOIN dds.dict_geo g ON v.id_geo = g.id
        JOIN dds.dict_education_type et ON v.id_education_type = et.id
        JOIN dds.dict_contact_type ct ON v.id_contact_type = ct.id
        JOIN dds.contacts co ON v.id_vacancy = co.id_vacancies AND v.id_contact_type = co.id_contact_type;
        
        GET DIAGNOSTICS inserted_rows = ROW_COUNT;
        
        RETURN 'Успешно. Вставлено ' || inserted_rows || ' вакансий в витрину данных.';
        
    EXCEPTION
        WHEN OTHERS THEN
            error_message := 'Ошибка при загрузке вакансий в витрину: ' || SQLERRM;
            RETURN error_message;
    END;
END;
$$ LANGUAGE plpgsql;