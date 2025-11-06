CREATE SCHEMA IF NOT EXISTS sales;

DROP TABLE IF EXISTS sales.sales_cleaned,
                      sales.sales_daily_report,
                      sales.sales_quarantine;

CREATE TABLE sales.sales_cleaned (
    id              SERIAL PRIMARY KEY,
    sale_date       DATE,
    channel         TEXT,
    product_id      TEXT,
    product_name    TEXT,
    quantity        INT,
    unit_price      NUMERIC(10,2),
    total_amount    NUMERIC(10,2),
    loaded_at       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE sales.sales_daily_report (
    report_date     DATE,
    channel         TEXT,
    total_sales     BIGINT,
    total_revenue   NUMERIC(12,2),
    top_products    JSONB,
    PRIMARY KEY (report_date, channel)
);

CREATE TABLE sales.sales_quarantine (
    id           SERIAL PRIMARY KEY,
    filename     TEXT,
    row_number   INT,
    raw_data     TEXT,
    error_reason TEXT,
    loaded_at    TIMESTAMP DEFAULT NOW()
);