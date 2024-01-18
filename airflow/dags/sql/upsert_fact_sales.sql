BEGIN TRANSACTION;

-- gather dimensional keys into #final_fact_sales to prepare for upsert
SELECT
    NVL(dt.date_key, -1) as date_key,
    NVL(prod.product_key, -1) as product_key,
    stg.quantity_sold as quantity_sold,
    stg.revenue as revenue,
    stg.transaction_code as transaction_code
INTO #final_fact_sales
FROM showcase_data_pipeline.stage_sales stg
LEFT JOIN showcase_data_pipeline.dim_date dt
    ON stg.date_id = dt.full_date
LEFT JOIN showcase_data_pipeline.dim_products prod
    ON stg.product_id = prod.product_id
;

-- delete fact_sales rows that match with staging_sales
DELETE FROM showcase_data_pipeline.fact_sales
USING #final_fact_sales
WHERE #final_fact_sales.transaction_code = showcase_data_pipeline.fact_sales.transaction_code
;

-- insert all rows from #final_fact_sales into fact_sales
INSERT INTO showcase_data_pipeline.fact_sales (
    date_key,
    product_key,
    quantity_sold,
    revenue,
    transaction_code
)
SELECT
    date_key,
    product_key,
    quantity_sold,
    revenue,
    transaction_code
FROM #final_fact_sales
;

-- drop temp table
DROP TABLE IF EXISTS #final_fact_sales;

END TRANSACTION;
