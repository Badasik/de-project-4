--Проектирование STG-слоя

--Удаление таблицы srv_etl_settings, если существует
DROP TABLE IF EXISTS stg.srv_etl_settings;

--Создание таблицы srv_etl_settings
CREATE TABLE stg.srv_etl_settings (
	id						SERIAL PRIMARY KEY,
	workflow_key			VARCHAR ,
	workflow_settings		TEXT UNIQUE
	);
	
--Удаление таблицы bonussystem_users, если существует
DROP TABLE IF EXISTS stg.bonussystem_users;

--Создание таблицы bonussystem_users
CREATE TABLE stg.bonussystem_users (
	id						INTEGER PRIMARY KEY,
	order_user_id			TEXT NOT NULL
	);
	
--Удаление таблицы bonussystem_ranks, если существует
DROP TABLE IF EXISTS stg.bonussystem_ranks;

--Создание таблицы bonussystem_ranks
CREATE TABLE stg.bonussystem_ranks (
	id						INTEGER PRIMARY KEY,
	"name"					VARCHAR(2048) NOT NULL,
	bonus_percent			NUMERIC(19,5) NOT NULL DEFAULT 0,
	min_payment_threshold	NUMERIC(19,5) NOT NULL DEFAULT 0
	);

--Удаление таблицы bonussystem_events, если существует
DROP TABLE IF EXISTS stg.bonussystem_events;

--Создание таблицы bonussystem_events
CREATE TABLE stg.bonussystem_events(
	id						INTEGER PRIMARY KEY,
	event_ts				TIMESTAMP WITHOUT TIME ZONE NOT NULL,
	event_type				VARCHAR NOT NULL,
	event_value				TEXT NOT NULL
	);

--Удаление таблицы ordersystem_orders, если существует
DROP TABLE IF EXISTS stg.ordersystem_orders;

--Создание таблицы ordersystem_orders
CREATE TABLE stg.ordersystem_orders (
	id						SERIAL PRIMARY KEY,
	object_id				VARCHAR UNIQUE NOT NULL,
	object_value			TEXT NOT NULL,
	update_ts 				TIMESTAMP WITHOUT TIME ZONE NOT NULL
	);

--Удаление таблицы ordersystem_restaurants, если существует
DROP TABLE IF EXISTS stg.ordersystem_restaurants;

--Создание таблицы stg.ordersystem_restaurants
CREATE TABLE stg.ordersystem_restaurants (
	id						SERIAL PRIMARY KEY,
	object_id				VARCHAR UNIQUE NOT NULL,
	object_value			TEXT NOT NULL,
	update_ts				TIMESTAMP WITHOUT TIME ZONE NOT NULL
	);

--Удаление таблицы ordersystem_users, если существует
DROP TABLE IF EXISTS stg.ordersystem_users;

--Создание таблицы ordersystem_users
CREATE TABLE stg.ordersystem_users (
	id						SERIAL PRIMARY KEY,
	object_id				VARCHAR UNIQUE NOT NULL,
	object_value			TEXT NOT NULL,
	update_ts				TIMESTAMP WITHOUT TIME ZONE NOT NULL
	);

--Удаление таблицы couriers, если существует
DROP TABLE IF EXISTS stg.couriers;

--Создание таблицы couriers
CREATE TABLE stg.couriers (
	id						SERIAL PRIMARY KEY,
	courier_id				VARCHAR NOT NULL,
	courier_name			VARCHAR NOT NULL
	);

--Удаление таблицы deliveries, если существует
DROP TABLE IF EXISTS stg.deliveries

--Создание таблицы deliveries
CREATE TABLE stg.deliveries (
	id						SERIAL PRIMARY KEY,
	order_id				VARCHAR NOT NULL,
	order_ts				TIMESTAMP WITHOUT TIME ZONE NOT NULL,
	delivery_id				VARCHAR NOT NULL,
	courier_id				VARCHAR NOT NULL,
	address					VARCHAR NOT NULL,
	delivery_ts				TIMESTAMP NOT NULL,
	rate					INTEGER NOT NULL,
	"sum"					NUMERIC(14,2),
	tip_sum					NUMERIC(14,2)
	);
