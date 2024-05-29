-- Вывести распределение (количество) клиентов по сферам деятельности, отсортировав результат по убыванию количества.

select  job_industry_category, count(*) as count_of_customers
from customer_20240101 c
group by job_industry_category
order by count_of_customers desc;

--  Найти сумму транзакций за каждый месяц по сферам деятельности, отсортировав по месяцам и по сфере деятельности.

select to_char(date_trunc('month', t.transaction_date::date), 'YYYY-MM') as transaction_month, c.job_industry_category, sum(t.list_price)
from transaction_20240101 t
inner join customer_20240101 c
on t.customer_id = c.customer_id
group by c.job_industry_category , transaction_month
order by transaction_month, c.job_industry_category;

-- Вывести количество онлайн-заказов для всех брендов в рамках подтвержденных заказов клиентов из сферы IT.

select t.brand , count(*)
from transaction_20240101 t
inner join customer_20240101 c
on t.customer_id = c.customer_id
where c.job_industry_category = 'IT'  and t.online_order = 'True' and t.order_status = 'Approved'
group by t.brand
order by t.brand desc;

-- Найти по всем клиентам сумму всех транзакций (list_price), максимум, минимум и количество транзакций, отсортировав результат по убыванию суммы транзакций и количества клиентов. Выполните двумя способами:
--используя только group by и используя только оконные функции. Сравните результат.

select  customer_id ,
		sum(list_price) as sum_of_transactions,
		max(list_price) as max_of_transactions,
		min(list_price) as min_of_transactions,
		count(transaction_id) as transaction_count
from transaction_20240101 t
group by customer_id
order by sum(list_price) desc, count(transaction_id) desc;

select  distinct
		customer_id,
		sum(list_price) over(partition by customer_id) as sum_of_transactions,
		max(list_price) over(partition by customer_id) as max_of_transactions,
		min(list_price) over(partition by customer_id) as min_of_transactions,
		count(transaction_id) over(partition by customer_id) as transaction_count
from transaction_20240101 t
order by sum_of_transactions desc, transaction_count desc

-- Найти имена и фамилии клиентов с минимальной/максимальной суммой транзакций за весь период (сумма транзакций не может быть null).
-- Напишите отдельные запросы для минимальной и максимальной суммы.

--клиент с минимальной суммой:

with grouped_table as
(
select c.customer_id, c.first_name , c.last_name , sum(t.list_price) as sum_of_transactions
from transaction_20240101 t
inner join customer_20240101 c
on t.customer_id = c.customer_id
group by c.customer_id, c.first_name , c.last_name
)

select *
from grouped_table
where sum_of_transactions = (select min(sum_of_transactions) from grouped_table);

-- клиент с максимальной суммой транзакций:

with grouped_table as
(
select c.customer_id, c.first_name , c.last_name , sum(t.list_price) as sum_of_transactions
from transaction_20240101 t
inner join customer_20240101 c
on t.customer_id = c.customer_id
group by c.customer_id, c.first_name , c.last_name
)

select *
from grouped_table
where sum_of_transactions = (select max(sum_of_transactions) from grouped_table);

-- Вывести только самые первые транзакции клиентов. Решить с помощью оконных функций.

with window_table as
(
select  customer_id
		,transaction_date::date
		,transaction_id
		,ROW_NUMBER() over(partition by customer_id order by transaction_date::date asc) as row
from transaction_20240101 t
)

select customer_id, transaction_date, transaction_id
from window_table
where row = 1;

-- Вывести имена, фамилии и профессии клиентов, между транзакциями которых был максимальный интервал (интервал вычисляется в днях)

with grouped_table as
(
with window_table as
(
select  c.customer_id, c.first_name, c.last_name, c.job_title
		,t.transaction_date::date
		,lead(t.transaction_date::date) over(partition by c.first_name, c.last_name order by t.transaction_date::date) as next_transaction_date
from transaction_20240101 t
inner join customer_20240101 c
on t.customer_id = c.customer_id
)

select *,
		max(next_transaction_date - transaction_date) as max_days_between
from window_table
where next_transaction_date - transaction_date notnull
group by customer_id, first_name, last_name, job_title, transaction_date, next_transaction_date
order by max_days_between desc
)

select *
from grouped_table
where max_days_between = (select max(max_days_between) from grouped_table);



