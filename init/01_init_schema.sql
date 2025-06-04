CREATE SCHEMA IF NOT EXISTS economic;

CREATE TABLE IF NOT EXISTS economic.currency_rates (
    "date" DATE NOT NULL,
    currency VARCHAR(100) NOT NULL,
    value NUMERIC(10, 4) NOT NULL,
    CONSTRAINT usd_to_rub_rates_date_currency_key UNIQUE (date, currency)
);

