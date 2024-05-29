CREATE TABLE "address" (
  "id" integer PRIMARY KEY,
  "address_string" varchar,
  "postcode" integer,
  "state" varchar,
  "country" varchar,
  "property_valuation" integer
);

CREATE TABLE "customer" (
  "id" integer PRIMARY KEY,
  "address_id" integer NOT null UNIQUE,
  "first_name" varchar NOT NULL,
  "last_name" varchar,
  "gender" varchar NOT NULL,
  "dob" varchar,
  "job_title" varchar,
  "job_industry_category" varchar NOT NULL,
  "wealth_segment" varchar NOT NULL,
  "owns_car" bool
);

CREATE TABLE "transaction" (
  "id" integer PRIMARY KEY,
  "product_id" integer NOT NULL,
  "customer_id" integer NOT NULL,
  "transaction_date" varchar NOT NULL,
  "online_order" bool,
  "order_status" varchar
);

CREATE TABLE "product" (
  "id" integer PRIMARY KEY,
  "brand" varchar,
  "product_line" varchar,
  "product_class" varchar,
  "product_size" varchar,
  "list_price" float NOT NULL,
  "standart_cost" float
);


CREATE TABLE "gender" (
  "value" varchar PRIMARY KEY
);

CREATE TABLE "wealth_segment" (
  "value" varchar PRIMARY KEY
);

CREATE TABLE "product_line" (
  "value" varchar PRIMARY KEY
);

CREATE TABLE "product_class" (
  "value" varchar PRIMARY KEY
);

CREATE TABLE "product_size" (
  "value" varchar PRIMARY KEY
);

CREATE TABLE "brand" (
  "name" varchar PRIMARY KEY
);

CREATE TABLE "job_industry_category" (
  "category_name" varchar PRIMARY KEY
);

CREATE TABLE "country" (
  "country_name" varchar PRIMARY KEY
);

COMMENT ON COLUMN "customer"."owns_car" IS '0 - No, 1 - Yes';

ALTER TABLE "transaction" ADD FOREIGN KEY ("product_id") REFERENCES "product" ("id");

ALTER TABLE "transaction" ADD FOREIGN KEY ("customer_id") REFERENCES "customer" ("id");

ALTER TABLE "customer" ADD FOREIGN KEY ("address_id") REFERENCES "address" ("id");

ALTER TABLE "customer" ADD FOREIGN KEY ("gender") REFERENCES "gender" ("value");

ALTER TABLE "customer" ADD FOREIGN KEY ("wealth_segment") REFERENCES "wealth_segment" ("value");

ALTER TABLE "customer" ADD FOREIGN KEY ("job_industry_category") REFERENCES "job_industry_category" ("category_name");

ALTER TABLE "product" ADD FOREIGN KEY ("product_class") REFERENCES "product_class" ("value");

ALTER TABLE "product" ADD FOREIGN KEY ("product_line") REFERENCES "product_line" ("value");

ALTER TABLE "product" ADD FOREIGN KEY ("product_size") REFERENCES "product_size" ("value");

ALTER TABLE "product" ADD FOREIGN KEY ("brand") REFERENCES "brand" ("name");

ALTER TABLE "address" ADD FOREIGN KEY ("country") REFERENCES "country" ("country_name");
