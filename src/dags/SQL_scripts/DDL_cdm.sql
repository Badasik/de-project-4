--Создание схемы cdm
CREATE SCHEMA IF NOT EXISTS cdm;

--Удаление витрины dm_courier_ledger, если она существует
DROP TABLE IF EXISTS cdm.dm_courier_ledger;

--Создание витрины dm_courier_ledger
CREATE TABLE cdm.dm_courier_ledger (
	id						SERIAL PRIMARY KEY,
	courier_id				VARCHAR NOT NULL,
	courier_name			VARCHAR NOT NULL,
	settlement_year			INTEGER NOT NULL CHECK (settlement_year >=2022 AND settlement_year <= 2200),
	settlement_month		INTEGER NOT NULL CHECK (settlement_month >= 1 AND settlement_month <= 12),
	orders_count			BIGINT NOT NULL CHECK (orders_count >= 0),
	orders_total_sum		NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (orders_total_sum >= 0::NUMERIC),
	rate_avg				NUMERIC(4,2) NOT NULL DEFAULT 0 CHECK (rate_avg >= 0::NUMERIC),
	order_processing_fee	NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (order_processing_fee >= 0::NUMERIC),
	courier_order_sum		NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (courier_order_sum >= 0::NUMERIC),
	courier_tips_sum		NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (courier_tips_sum >= 0::NUMERIC),
	courier_reward_sum		NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (courier_reward_sum >= 0::NUMERIC)
	--UNIQUE (courier_id, settlement_year, settlement_month)
	);

--Удаление витрины dm_settlement_reprot, если она существует
DROP TABLE IF EXISTS cdm.dm_settlement_report

--Создание витрины dm_settlement_report
CREATE TABLE cdm.dm_settlement_report (
	id							SERIAL PRIMARY KEY,
	restaurant_id				INTEGER NOT NULL,
	restaurant_name				VARCHAR NOT NULL,
	settlement_date 			DATE NOT NULL CHECK (settlement_date>='2022-01-01' AND settlement_date < '2500-01-01'),
	orders_count				INTEGER NOT NULL CHECK (orders_count >=0),
	orders_total_sum			NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (orders_total_sum >= 0::NUMERIC) ,
	orders_bonus_payment_sum	NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (orders_bonus_payment_sum >= 0::NUMERIC),
	orders_bonus_granted_sum	NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (orders_bonus_granted_sum >= 0::NUMERIC),
	order_processing_fee		NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (order_processing_fee >= 0::NUMERIC),
	restaurant_reward_sum		NUMERIC(14,2) NOT NULL DEFAULT 0 CHECK (restaurant_reward_sum >= 0::NUMERIC)
	);




