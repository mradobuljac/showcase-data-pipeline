BEGIN TRANSACTION;

-- aggregate data for logical date of DAG run
SELECT
    date_key,
    SUM(quantity_sold) as quantity_sold,
    SUM(revenue) as revenue
INTO #final_fact_sales_aggregate
FROM showcase_data_pipeline.fact_sales sls
WHERE date_key = '{{ ds_nodash }}'
GROUP BY date_key

-- delete existing fact_sales_aggregate data for logical date if exists
DELETE FROM showcase_data_pipeline.fact_sales_aggregate
USING #final_fact_sales_aggregate
WHERE #final_fact_sales_aggregate.date_key = showcase_data_pipeline.fact_sales_aggregate.date_key
;

-- insert
INSERT INTO fact_sales_aggregate (sales_key, date_key, quantity_sold, revenue)
SELECT sales_key, date_key, quantity_sold, revenue
FROM #final_fact_sales_aggregate

-- cleanup
DROP TABLE IF EXISTS #final_fact_sales_aggregate

END TRANSACTION;