--select * from public.dag
select * from public.products

drop table if exists products;
drop table if exists users_local;
drop table if exists orders;

select * from users_local
select * from orders
-- Создание таблицы продуктов

--drop table if exists vacancies;
drop table if exists company;
drop table if exists geo;
drop table if exists contacts;
drop table if exists education;


select * from vacancies;
select * from  company;
select * from  geo;
select * from  contacts;
select * from  education;


select count(1) from vacancies;
select count(1) from  company;
select count(1) from  geo;
select count(1) from  contacts;
select count(1) from  education;


select concat(vacancy_id, code_profession) from stg.vacancies

select * from geo