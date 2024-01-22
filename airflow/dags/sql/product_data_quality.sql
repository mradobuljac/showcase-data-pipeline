/*
delete duplicates
*/

BEGIN TRANSACTION;  -- begin delete duplicates

-- identify dupes, deduplicate and insert into temp table
SELECT
    product_id,
    product_name,
    product_category,
    product_rating
INTO #product_dupes
FROM showcase_data_pipeline.stage_products
GROUP BY
    product_id,
    product_name,
    product_category,
    product_rating
HAVING COUNT(*) > 1
;

-- delete dupes from stage table
DELETE FROM showcase_data_pipeline.stage_products
USING #product_dupes
WHERE
    showcase_data_pipeline.stage_products.product_id = #product_dupes.product_id
    AND showcase_data_pipeline.stage_products.product_name = #product_dupes.product_name
    AND showcase_data_pipeline.stage_products.product_category = #product_dupes.product_category
    AND showcase_data_pipeline.stage_products.product_rating = #product_dupes.product_rating
;

-- insert deduplicated data back into stage table
INSERT INTO showcase_data_pipeline.stage_products (product_id, product_name, product_category, product_rating)
SELECT
    product_id,
    product_name,
    product_category,
    product_rating
FROM #product_dupes
;

-- cleanup
DROP TABLE #product_dupes
;

END TRANSACTION; -- end delete duplicates


/*
other data quality actions like handling NULL values, data formatting, etc
*/
