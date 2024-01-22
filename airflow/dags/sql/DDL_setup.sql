BEGIN TRANSACTION;

CREATE SCHEMA IF NOT EXISTS showcase_data_pipeline;

CREATE TABLE IF NOT EXISTS showcase_data_pipeline.stage_products (
    product_id INT NOT NULL DISTKEY SORTKEY,
    product_name VARCHAR(255),
    product_category VARCHAR(50),
    product_rating INT
)
;

CREATE TABLE IF NOT EXISTS showcase_data_pipeline.dim_products (
    product_key INT IDENTITY(1,1) NOT NULL DISTKEY SORTKEY PRIMARY KEY,
    product_id INT NOT NULL,
    product_name VARCHAR(255),
    product_category VARCHAR(50),
    product_rating INT
);

CREATE TABLE IF NOT EXISTS showcase_data_pipeline.stage_customers (
    customer_id INT NOT NULL DISTKEY SORTKEY,
    customer_name VARCHAR(255),
    customer_industry VARCHAR(50),
    customer_satisfaction_rating INT
)
;

CREATE TABLE IF NOT EXISTS showcase_data_pipeline.dim_customers (
    customer_key INT IDENTITY(1,1) NOT NULL DISTKEY SORTKEY PRIMARY KEY,
    customer_id INT NOT NULL,
    customer_name VARCHAR(255),
    customer_industry VARCHAR(50),
    customer_satisfaction_rating INT,
    row_effective_date date,
    row_expired_date date,
    is_current bool
);

CREATE TABLE IF NOT EXISTS showcase_data_pipeline.stage_sales (
date_id DATE SORTKEY DISTKEY,
product_id INT,
customer_id INT,
quantity_sold INT,
revenue REAL,
transaction_code NVARCHAR(255)
)
;

CREATE TABLE IF NOT EXISTS showcase_data_pipeline.fact_sales (
sales_key INT IDENTITY(1, 1),
date_key INT SORTKEY DISTKEY,
product_key INT,
customer_key INT,
quantity_sold INT,
revenue REAL,
transaction_code NVARCHAR(255)
)
;

CREATE TABLE IF NOT EXISTS showcase_data_pipeline.fact_sales_aggregate (
sales_key INT IDENTITY(1, 1),
date_key INT SORTKEY DISTKEY,
quantity_sold INT,
revenue REAL
)
;

CREATE TABLE IF NOT EXISTS showcase_data_pipeline.dim_date (
    date_key INT DISTKEY SORTKEY,
    full_date DATE,
    day_of_month INT,
    month INT,
    year INT
);


-- populate dim_date with values
TRUNCATE TABLE showcase_data_pipeline.dim_date;
INSERT INTO showcase_data_pipeline.dim_date (date_key, full_date, day_of_month, month, year)
VALUES
(-1, NULL, NULL, NULL, NULL),
(20240101, '2024-01-01', 1, 1, 2024),
(20240102, '2024-01-02', 2, 1, 2024),
(20240103, '2024-01-03', 3, 1, 2024),
(20240104, '2024-01-04', 4, 1, 2024),
(20240105, '2024-01-05', 5, 1, 2024),
(20240106, '2024-01-06', 6, 1, 2024),
(20240107, '2024-01-07', 7, 1, 2024),
(20240108, '2024-01-08', 8, 1, 2024),
(20240109, '2024-01-09', 9, 1, 2024),
(20240110, '2024-01-10', 10, 1, 2024),
(20240111, '2024-01-11', 11, 1, 2024),
(20240112, '2024-01-12', 12, 1, 2024),
(20240113, '2024-01-13', 13, 1, 2024),
(20240114, '2024-01-14', 14, 1, 2024),
(20240115, '2024-01-15', 15, 1, 2024),
(20240116, '2024-01-16', 16, 1, 2024),
(20240117, '2024-01-17', 17, 1, 2024),
(20240118, '2024-01-18', 18, 1, 2024),
(20240119, '2024-01-19', 19, 1, 2024),
(20240120, '2024-01-20', 20, 1, 2024),
(20240121, '2024-01-21', 21, 1, 2024),
(20240122, '2024-01-22', 22, 1, 2024),
(20240123, '2024-01-23', 23, 1, 2024),
(20240124, '2024-01-24', 24, 1, 2024),
(20240125, '2024-01-25', 25, 1, 2024),
(20240126, '2024-01-26', 26, 1, 2024),
(20240127, '2024-01-27', 27, 1, 2024),
(20240128, '2024-01-28', 28, 1, 2024),
(20240129, '2024-01-29', 29, 1, 2024),
(20240130, '2024-01-30', 30, 1, 2024),
(20240131, '2024-01-31', 31, 1, 2024),
(20240201, '2024-02-01', 1, 2, 2024),
(20240202, '2024-02-02', 2, 2, 2024),
(20240203, '2024-02-03', 3, 2, 2024),
(20240204, '2024-02-04', 4, 2, 2024),
(20240205, '2024-02-05', 5, 2, 2024),
(20240206, '2024-02-06', 6, 2, 2024),
(20240207, '2024-02-07', 7, 2, 2024),
(20240208, '2024-02-08', 8, 2, 2024),
(20240209, '2024-02-09', 9, 2, 2024),
(20240210, '2024-02-10', 10, 2, 2024),
(20240211, '2024-02-11', 11, 2, 2024),
(20240212, '2024-02-12', 12, 2, 2024),
(20240213, '2024-02-13', 13, 2, 2024),
(20240214, '2024-02-14', 14, 2, 2024),
(20240215, '2024-02-15', 15, 2, 2024),
(20240216, '2024-02-16', 16, 2, 2024),
(20240217, '2024-02-17', 17, 2, 2024),
(20240218, '2024-02-18', 18, 2, 2024),
(20240219, '2024-02-19', 19, 2, 2024),
(20240220, '2024-02-20', 20, 2, 2024),
(20240221, '2024-02-21', 21, 2, 2024),
(20240222, '2024-02-22', 22, 2, 2024),
(20240223, '2024-02-23', 23, 2, 2024),
(20240224, '2024-02-24', 24, 2, 2024),
(20240225, '2024-02-25', 25, 2, 2024),
(20240226, '2024-02-26', 26, 2, 2024),
(20240227, '2024-02-27', 27, 2, 2024),
(20240228, '2024-02-28', 28, 2, 2024)
;

END TRANSACTION;