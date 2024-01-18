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


CREATE TABLE IF NOT EXISTS showcase_data_pipeline.stage_sales (
date_id DATE SORTKEY DISTKEY,
product_id INT,
quantity_sold INT,
revenue REAL,
transaction_code NVARCHAR(255)
)


-- CREATE TABLE IF NOT EXISTS showcase_data_pipeline.fact_sales
CREATE TABLE IF NOT EXISTS showcase_data_pipeline.fact_sales (
sales_key INT IDENTITY(1, 1),
date_key INT SORTKEY DISTKEY,
product_key INT,
quantity_sold INT,
revenue REAL,
transaction_code NVARCHAR(255)
)





CREATE TABLE IF NOT EXISTS showcase_data_pipeline.dim_date (
    date_key INT DISTKEY SORTKEY,
    full_date DATE,
    day_of_month INT,
    month INT,
    year INT
);

-- populate dim_date with values
INSERT INTO showcase_data_pipeline.dim_date (date_key, full_date, day_of_month, month, year)
VALUES
(-1,	 NULL, NULL, NULL, NULL),
(20240101,	 '2024-01-01', 1	,1, 2024),
(20240102,	 '2024-01-02', 2	,1, 2024),
(20240103,	 '2024-01-03', 3	,1, 2024),
(20240104,	 '2024-01-04', 4	,1, 2024),
(20240105,	 '2024-01-05', 5	,1, 2024),
(20240106,	 '2024-01-06', 6	,1, 2024),
(20240107,	 '2024-01-07', 7	,1, 2024),
(20240108,	 '2024-01-08', 8	,1, 2024),
(20240109,	 '2024-01-09', 9	,1, 2024),
(20240110,	 '2024-01-10', 10	,1, 2024),
(20240111,	 '2024-01-11', 11	,1, 2024),
(20240112,	 '2024-01-12', 12	,1, 2024),
(20240113,	 '2024-01-13', 13	,1, 2024),
(20240114,	 '2024-01-14', 14	,1, 2024),
(20240115,	 '2024-01-15', 15	,1, 2024),
(20240116,	 '2024-01-16', 16	,1, 2024),
(20240117,	 '2024-01-17', 17	,1, 2024),
(20240118,	 '2024-01-18', 18	,1, 2024),
(20240119,	 '2024-01-19', 19	,1, 2024),
(20240120,	 '2024-01-20', 20	,1, 2024),
(20240121,	 '2024-01-21', 21	,1, 2024),
(20240122,	 '2024-01-22', 22	,1, 2024),
(20240123,	 '2024-01-23', 23	,1, 2024),
(20240124,	 '2024-01-24', 24	,1, 2024),
(20240125,	 '2024-01-25', 25	,1, 2024),
(20240126,	 '2024-01-26', 26	,1, 2024),
(20240127,	 '2024-01-27', 27	,1, 2024),
(20240128,	 '2024-01-28', 28	,1, 2024),
(20240129,	 '2024-01-29', 29	,1, 2024),
(20240130,	 '2024-01-30', 30	,1, 2024),
(20240131,	 '2024-01-31', 31	,1, 2024)
;