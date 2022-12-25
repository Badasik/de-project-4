--Проектирование DDS-слоя

--Удаление таблицы dm_restaurants, если существует
DROP TABLE IF EXISTS dds.dm_restaurants;

--Создание таблицы dm_restaurants
CREATE TABLE dds.dm_restaurants (
	id						SERIAL PRIMARY KEY,
	restaurant_id			VARCHAR NOT NULL UNIQUE,
	restaurant_name			VARCHAR NOT NULL,
	active_from				TIMESTAMP WITHOUT TIME ZONE NOT NULL,
	active_to				TIMESTAMP WITHOUT TIME ZONE NOT NULL
	);
	
--Удаление таблицы dm_users, если существует
DROP TABLE IF EXISTS dds.dm_users;

--Создание таблицы dm_users
CREATE TABLE dds.dm_users(
	id						SERIAL PRIMARY KEY,
	user_id					VARCHAR NOT NULL UNIQUE,
	user_name				VARCHAR NOT NULL,
	user_login				VARCHAR NOT NULL UNIQUE
	);

--Удаление таблицы dm_timestamps, если существует
DROP TABLE IF EXISTS dds.dm_timestamps CASCADE;

--Создание таблицы dm_timestamps
CREATE TABLE dds.dm_timestamps(
	id						SERIAL PRIMARY KEY,
	ts						TIMESTAMP WITHOUT TIME ZONE NOT NULL,
	"year" 					SMALLINT NOT NULL CHECK ("year" >= 2022 AND "year" <= 2200),
	"month"					SMALLINT NOT NULL CHECK ("month" >= 1 AND "month" <= 12),
	"day"					SMALLINT NOT NULL CHECK ("day" >= 1 AND "day" <= 31),
	"time"					TIME NOT NULL,
	"date"					DATE NOT NULL,
	special_mark 			INTEGER DEFAULT 0
	);
	
--Удаление таблицы dm_products, если существует	
DROP TABLE IF EXISTS dds.dm_products;

--Создание таблицы dm_products
CREATE TABLE dds.dm_products (
	id						SERIAL PRIMARY KEY,
	restaurant_id			INTEGER NOT NULL,
	product_id				VARCHAR NOT NULL,
	product_name			VARCHAR NOT NULL,
	product_price			NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (product_price >= 0),
	active_from				TIMESTAMP WITHOUT TIME ZONE NOT NULL,
	active_to				TIMESTAMP WITHOUT TIME ZONE NOT NULL
	);

ALTER TABLE dds.dm_products
  ADD CONSTRAINT dm_products_restaurant_id_fkey FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants (id);
  
--Удаление таблицы dm_orders, если сущетсвует
DROP TABLE IF EXISTS dds.dm_orders;
 
--Создание таблицы dm_orders
CREATE TABLE dds.dm_orders (
	id						SERIAL PRIMARY KEY,
	user_id					INTEGER NOT NULL,
	restaurant_id			INTEGER NOT NULL,
	timestamp_id			INTEGER NOT NULL,
	order_key				VARCHAR NOT NULL UNIQUE,
	order_status			VARCHAR	NOT NULL
	);
	
ALTER TABLE dds.dm_orders 
  ADD CONSTRAINT dm_orders_restaurant_id_fkey FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants (id);
  
ALTER TABLE dds.dm_orders 
  ADD CONSTRAINT dm_orders_user_id_fkey	FOREIGN KEY (user_id) REFERENCES dds.dm_users (id);
  
ALTER TABLE dds.dm_orders 
  ADD CONSTRAINT dm_orders_timestamp_id_fkey FOREIGN KEY (timestamp_id) REFERENCES dds.dm_timestamps (id);
  
--Удаление таблицы fct_product_sales, если существует
 DROP TABLE IF EXISTS dds.fct_product_sales;
 
--Создание таблицы fct_product_sales
CREATE TABLE dds.fct_product_sales (
	id						SERIAL PRIMARY KEY,
	product_id				INTEGER NOT NULL,
	order_id				INTEGER NOT NULL,
	"count"					INTEGER NOT NULL DEFAULT 0 CHECK ("count" >= 0),
	price					NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (price >= 0::NUMERIC),
	total_sum				NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (total_sum >= 0::NUMERIC),
	bonus_payment 			NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (bonus_payment >= 0::NUMERIC),
	bonus_grant				NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (bonus_grant >= 0::NUMERIC)
	);
	
ALTER TABLE dds.fct_product_sales 
  ADD CONSTRAINT fct_product_sales_product_id_fkey FOREIGN KEY (product_id) REFERENCES dds.dm_products (id);
  
ALTER TABLE dds.fct_product_sales 
  ADD CONSTRAINT fct_product_sales_order_id_fkey FOREIGN KEY (order_id) REFERENCES dds.dm_orders (id);
  
--Удаление таблицы dm_couriers
DROP TABLE IF EXISTS dds.dm_couriers;

--Создание таблицы dm_couriers, если существует
CREATE TABLE dds.dm_couriers (
	id						SERIAL PRIMARY KEY,
	courier_id				VARCHAR	NOT NULL UNIQUE,
	courier_name			VARCHAR NOT NULL
	);
	
--Удаление таблицы dm_deliveries, если существует
DROP TABLE IF EXISTS dds.dm_deliveries;

--Создание таблицы dm_deliveries
CREATE TABLE dds.dm_deliveries (
	id						SERIAL PRIMARY KEY,
	order_id				VARCHAR NOT NULL,
	delivery_id				VARCHAR NOT NULL UNIQUE,
	courier_id				INTEGER NOT NULL,
	address					VARCHAR NOT NULL,
	delivery_ts_id			INTEGER NOT NULL,
	rate					INTEGER NOT NULL,
	order_sum				NUMERIC(14,2) NOT NULL CHECK (order_sum >= 0::NUMERIC),
	tip_sum					NUMERIC(14,2) NOT NULL DEFAULT 0
	);
	
ALTER TABLE dds.dm_deliveries 
  ADD CONSTRAINT dm_deliveries_courier_id_fkey FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers (id);