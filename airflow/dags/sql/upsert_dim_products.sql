
--SCD1/UPSERT via INSERT & UPDATE

BEGIN TRANSACTION;

--update existing records
UPDATE
    showcase_data_pipeline.dim_products
SET
    product_name = src.product_name,
    product_category = src.product_category,
    product_rating = src.product_rating
FROM showcase_data_pipeline.dim_products tgt
INNER JOIN showcase_data_pipeline.stage_products src
    ON tgt.product_id = src.product_id
WHERE -- identify changed rows
    NVL(tgt.product_name, '') <> NVL(src.product_name, '')
    OR NVL(tgt.product_category, '') <> NVL(src.product_category, '')
    OR NVL(tgt.product_rating, -1) <> NVL(src.product_rating, -1)
;

-- insert new records
INSERT INTO
    showcase_data_pipeline.dim_products (
        product_id,
        product_name,
        product_category,
        product_rating
    )
SELECT
    src.product_id,
    src.product_name,
    src.product_category,
    src.product_rating
FROM showcase_data_pipeline.stage_products src
LEFT JOIN showcase_data_pipeline.dim_products tgt
    ON src.product_id = tgt.product_id
WHERE tgt.product_id IS NULL    -- identify new rows
;

-- logic for late arriving dimensions.
-- Inferred row strategy
-- If there is no corresponding dimension row for incoming fact table row, insert new row with only natural key and generated surrogate key
-- That way row can be immediately joined to fact table, and can be updated later when the rest of dimensional context arrives
INSERT INTO showcase_data_pipeline.dim_products (product_id)
SELECT DISTINCT sls.product_id
FROM showcase_data_pipeline.stage_sales sls
LEFT JOIN showcase_data_pipeline.dim_products prod
    ON sls.product_id = prod.product_id
WHERE prod.product_id IS NULL
;

END TRANSACTION;
