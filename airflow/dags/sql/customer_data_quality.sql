/*
delete duplicates
*/

BEGIN TRANSACTION;  -- begin delete duplicates

-- identify dupes, deduplicate and insert into temp table
SELECT
    customer_id,
    customer_name,
    customer_industry,
    customer_satisfaction_rating
INTO #customer_dupes
FROM showcase_data_pipeline.stage_customers
GROUP BY
    customer_id,
    customer_name,
    customer_industry,
    customer_satisfaction_rating
HAVING COUNT(*) > 1
;

-- delete dupes from stage table
DELETE FROM showcase_data_pipeline.stage_customers
USING #customer_dupes
WHERE
    showcase_data_pipeline.stage_customers.customer_id = #customer_dupes.customer_id
    AND showcase_data_pipeline.stage_customers.customer_name = #customer_dupes.customer_name
    AND showcase_data_pipeline.stage_customers.customer_industry = #customer_dupes.customer_industry
    AND showcase_data_pipeline.stage_customers.customer_satisfaction_rating = #customer_dupes.customer_satisfaction_rating
;

-- insert deduplicated data back into stage table
INSERT INTO showcase_data_pipeline.stage_customers (customer_id, customer_name, customer_industry, customer_satisfaction_rating)
SELECT
    customer_id,
    customer_name,
    customer_industry,
    customer_satisfaction_rating
FROM #customer_dupes
;

-- cleanup
DROP TABLE #customer_dupes
;

END TRANSACTION; -- end delete duplicates


/*
other data quality actions like handling NULL values, data formatting, etc
*/
