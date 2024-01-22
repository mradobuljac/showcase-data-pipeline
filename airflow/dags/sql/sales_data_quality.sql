/*
delete duplicates
*/

BEGIN TRANSACTION;  -- begin delete duplicates

-- identify dupes, deduplicate and insert into temp table
SELECT
    date_id,
    product_id,
    quantity_sold,
    revenue,
    transaction_code
INTO #sales_dupes
FROM showcase_data_pipeline.stage_sales
GROUP BY
    date_id,
    product_id,
    quantity_sold,
    revenue,
    transaction_code
HAVING COUNT(*) > 1
;

-- delete dupes from stage table
DELETE FROM showcase_data_pipeline.stage_sales
USING #sales_dupes
WHERE
    showcase_data_pipeline.stage_sales.date_id = #sales_dupes.date_id
    AND showcase_data_pipeline.stage_sales.product_id = #sales_dupes.product_id
    AND showcase_data_pipeline.stage_sales.quantity_sold = #sales_dupes.quantity_sold
    AND showcase_data_pipeline.stage_sales.revenue = #sales_dupes.revenue
    AND showcase_data_pipeline.stage_sales.transaction_code = #sales_dupes.transaction_code
;

-- insert deduplicated data back into stage table
INSERT INTO showcase_data_pipeline.stage_sales (date_id, product_id, quantity_sold, revenue, transaction_code)
SELECT
    date_id,
    product_id,
    quantity_sold,
    revenue,
    transaction_code
FROM #sales_dupes
;

-- cleanup
DROP TABLE #sales_dupes
;

END TRANSACTION; -- end delete duplicates


/*
other data quality actions like handling NULL values, data formatting, etc
*/
