-- создание таблиц Customer и Transaction

create table customer (
	customer_id int4 primary key,
	first_name varchar(50),
	last_name varchar(50),
	gender varchar(30),
	dob date,
	job_title varchar(50),
	job_industry_category varchar(50),
	wealth_segment varchar(50),
	deceased_indicator varchar (50),
	owns_car varchar (30),
	address varchar(50),
	postcode varchar (30),
	state varchar (30),
	country varchar (30),
	property_valuation int4

);

CREATE TABLE TRANSACTION (
	transaction_id int4 PRIMARY KEY,
	product_id	int4,
	customer_id int4,
	transaction_date date,
	online_order boolean,
	order_status varchar (30),
	brand varchar (30),
	product_line varchar (30),
	product_class varchar (30),
	product_size varchar (30),
	list_price float4,
	standard_cost float4
);

-- все уникальные бренды, у которых стандартная стоимость выше $1500.
select distinct(t.brand) from "transaction" t
where standard_cost > 1500;

--все подтвержденные транзакции за период '2017-04-01' по '2017-04-09' включительно.
select t.* from "transaction" t
where t.order_status = 'Approved' and t.transaction_date::date between '2017-04-01' and '2017-04-09';


-- все профессии у клиентов из сферы IT или Financial Services, которые начинаются с 'Senior'.
select c.job_title from customer c
where c.job_title like 'Senior%' and (c.job_industry_category = 'IT' or c.job_industry_category = 'Financial Services');

-- все бренды, которые закупают клиенты, работающие в сфере Financial Services
select distinct(t.brand) from "transaction" t
where t.customer_id in (select c.customer_id  from customer c 
where c.job_industry_category = 'Financial Services');

-- 10 клиентов, которые оформили онлайн-заказ продукции из брендов 'Giant Bicycles', 'Norco Bicycles', 'Trek Bicycles'.
select c.first_name, c.last_name from customer c
where c.customer_id in (select t.customer_id from "transaction" t where t.brand in ('Giant Bicycles', 'Norco Bicycles', 'Trek Bicycles'));

-- все клиентов, у которых нет транзакций.
select c.first_name, c.last_name from customer c
where c.customer_id not in (select t.customer_id from "transaction" t);

-- все клиенты из IT, у которых транзакции с максимальной стандартной стоимостью.
select c.customer_id, c.first_name, c.last_name, c.job_industry_category, t.standard_cost  from customer c
join "transaction" t on c.customer_id = t.customer_id
where c.job_industry_category = 'IT' and t.standard_cost notnull and t.standard_cost = (select max(t.standard_cost) from "transaction" t);

-- все клиенты из сферы IT и Health, у которых есть подтвержденные транзакции за период '2017-07-07' по '2017-07-17'.
select c.customer_id, c.first_name, c.last_name, c.job_industry_category, t.transaction_date,  t.order_status  from customer c
join "transaction" t on c.customer_id = t.customer_id
where c.job_industry_category in ('IT', 'Health') and t.order_status = 'Approved' and t.transaction_date::date between '2017-07-07' and '2017-07-17';