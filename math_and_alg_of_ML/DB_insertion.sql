insert into brand (name)
values ('Trek Bicycles'), ('Giant Bicycles'), ('Solex');

insert into country (country_name)
values ('Australia');

insert into gender (value)
values ('M'), ('F'), ('U');

insert into job_industry_category  (category_name)
values ('IT'), ('Health'), ('Retail'), ('Manufacturing');

insert into product_class  (value)
values ('low'), ('medium'), ('high');

insert into product_size  (value)
values ('medium'), ('large'), ('small');

insert into product_line  (value)
values ('Standart'), ('Road'), ('Mountain'), ('Touring');

insert into wealth_segment  (value)
values ('Mass Customer'), ('Affluent Customer'), ('High Net Worth');

INSERT INTO public.address (id, address_string, postcode, state, country, property_valuation)
values 	(1, '2 Beilfuss Plaza', 2000, 'NSW', 'Australia', 9),
		(2, '422 Valley Edge Pass', 2000, 'NSW', 'Australia', 10),
		(3, '2 Union Way', 2035, 'NSW', 'Australia', 12);


INSERT INTO public.customer (id, address_id, first_name, last_name, gender, dob, job_title, job_industry_category, wealth_segment, owns_car)
values	(3693, 3, 'Brenna', 'Raft', 'Female', '1985-01-19 12:00:00 AM', null, 'Manufacturing', 'Affluent Customer', true),
		(2625, 1, 'Kristal', 'McRobbie', 'Female', '1972-03-16 12:00:00 AM', 'Research Assistant I', 'Manufacturing', 'Affluent Customer', true),
		(3618, 2, 'Evyn', 'Rouby', 'Male', '1966-04-28 12:00:00 AM', 'Statistician IV', 'Retail', 'Affluent Customer', true);
		

INSERT INTO public.product (id, brand, product_line, product_class, product_size, list_price, standart_cost)
values	(58, 'Trek Bicycles', 'Standart', 'high', 'small', 478.16, 298.72);
		
INSERT INTO public."transaction" (id, product_id, customer_id, transaction_date, online_order, order_status)
values	(8268, 58, 2625, '2017-10-07 12:00:00 AM', true, 'Approved');