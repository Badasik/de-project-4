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


--Добавление процедур и промежуточных таблиц
DROP TABLE IF EXISTS stg.api_couriers;

CREATE TABLE IF NOT EXISTS stg.api_couriers(
	id						SERIAL PRIMARY KEY,
	"content"					TEXT UNIQUE,
	load_ts					TIMESTAMP WITHOUT TIME ZONE DEFAULT current_timestamp
	);
	
DROP TABLE IF EXISTS stg.api_deliveries;

CREATE TABLE IF NOT EXISTS stg.api_deliveries(
	id 						SERIAL PRIMARY KEY,
	"content"					TEXT UNIQUE,
	load_ts					TIMESTAMP WITHOUT TIME ZONE DEFAULT current_timestamp
	);

ALTER TABLE stg.couriers 
  ADD COLUMN load_ts TIMESTAMP WITHOUT TIME ZONE;
 
ALTER TABLE stg.deliveries  
  ADD COLUMN load_ts TIMESTAMP WITHOUT TIME ZONE;
	
 
DROP PROCEDURE if exists Load_stg_couriers ();

CREATE PROCEDURE Load_stg_couriers()
LANGUAGE SQL
AS $$
TRUNCATE TABLE stg.couriers ;
INSERT INTO stg.couriers (courier_id, courier_name, load_ts)
SELECT
    content::json#>>'{_id}' as _id
  , content::json#>>'{name}' as name
  , load_ts
FROM stg.api_couriers;
$$;




drop PROCEDURE if exists Load_stg_deliveries();

CREATE PROCEDURE Load_stg_deliveries()
LANGUAGE SQL
AS $$
TRUNCATE TABLE stg.deliveries;
INSERT INTO  stg.deliveries
(
		order_id,
		order_ts,
		delivery_id,
		courier_id,
		address,
		delivery_ts,
		rate,
		sum,
		tip_sum,
		load_ts)
    SELECT
       content::json#>>'{order_id}' order_id
     , CAST(content::json#>>'{order_ts}' as timestamp)::timestamptz order_ts
     , content::json#>>'{courier_id}' courier_id
     , content::json#>>'{delivery_id}' delivery_id
     , content::json#>>'{address}' address
     , CAST(content::json#>>'{delivery_ts}' as timestamp)::timestamptz delivery_ts
     , CAST(content::json#>>'{rate}' AS int) rate
     , CAST(content::json#>>'{sum}' AS numeric(14, 5)) sum
     , CAST(content::json#>>'{tip_sum}'AS numeric(14, 5)) tip_sum
     , load_ts
    FROM stg.api_deliveries;
$$;
