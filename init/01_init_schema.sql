CREATE SCHEMA IF NOT EXISTS economic;

CREATE TABLE IF NOT EXISTS economic.currency_rates (
    "date" DATE NOT NULL,
    currency VARCHAR(100) NOT NULL,
    value NUMERIC(10, 4) NOT NULL,
    CONSTRAINT usd_to_rub_rates_date_currency_key UNIQUE (date, currency)
);

CREATE TABLE IF NOT EXISTS economic.rf_inter_reserves (
	"date" DATE NOT NULL,
	value NUMERIC(10, 2) NOT NULL,
	CONSTRAINT rf_inter_reserves_date_key UNIQUE (date)
);

CREATE TABLE IF NOT EXISTS economic.inflation_key_rate (
	"date" date NOT NULL,
	inflation numeric(6, 2) NULL,
	inflation_target numeric(6, 2) NULL,
	key_rate numeric(6, 2) NULL,
	CONSTRAINT inflation_key_rate_date_key UNIQUE (date)
);