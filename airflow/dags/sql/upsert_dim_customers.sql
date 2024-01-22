
--SCD2/UPSERT

BEGIN TRANSACTION;

-- insert brand new rows
INSERT INTO showcase_data_pipeline.dim_customers (customer_id, customer_name, customer_industry, customer_satisfaction_rating, row_effective_date, row_expired_date, is_current)
SELECT
    src.customer_id,
    src.customer_name,
    src.customer_industry,
    src.customer_satisfaction_rating,
    '{{ ds }}',    -- RowExpiredDate
    CAST('9999-12-31' AS Date),    -- RowEffectiveDate
    1   -- IsCurrent
FROM showcase_data_pipeline.stage_customers src
LEFT JOIN showcase_data_pipeline.dim_customers tgt
    ON src.customer_id = tgt.customer_id    -- natural key
WHERE tgt.customer_id IS NULL   -- identify new rows
;

-- expire matched rows
UPDATE showcase_data_pipeline.dim_customers tgt
SET
    row_expired_date = '{{yesterday_ds}}',    -- yesterday
    is_current = 0
FROM showcase_data_pipeline.stage_customers  src
WHERE
    src.customer_id = tgt.customer_id
    AND tgt.is_current = 1  -- compare source data only with currently active rows
    AND (   -- identify changes
        NVL(src.customer_name, '') <> NVL(tgt.customer_name, '')
        OR NVL(src.customer_industry, '') <> NVL(tgt.customer_industry, '')
        OR NVL(src.customer_satisfaction_rating, -1) <> NVL(tgt.customer_satisfaction_rating, -1)
        )

-- INSERT NEW MATCHED ROWS
;WITH maxColumn AS -- get latest row for each NaturalKey in target table
(
SELECT
    customer_id,
    MAX(row_expired_date) as max_expired_date,
    MAX(row_effective_date) as max_effective_date
FROM showcase_data_pipeline.dim_customers
GROUP BY customer_id
)

INSERT INTO showcase_data_pipeline.dim_customers(customer_id, customer_name, customer_industry, customer_satisfaction_rating, row_effective_date, row_expired_date, is_current)
SELECT
    src.customer_id,
    src.customer_name,
    src.customer_industry,
    src.customer_satisfaction_rating,
    '{{ ds }}',
    CAST('9999-12-31' AS Date),
    1
FROM showcase_data_pipeline.stage_customers src
INNER JOIN showcase_data_pipeline.dim_customers tgt
    ON src.customer_id = tgt.customer_id
INNER JOIN maxColumn maxCol
    ON tgt.customer_id = maxCol.customer_id
WHERE
    tgt.row_expired_date = maxCol.max_expired_date AND tgt.row_effective_date = maxCol.max_effective_date	-- get only latest version of the tgt row. That way way we won't compare changes between src and already expired rows
    AND (	-- identify changes
        NVL(src.customer_name, '') <> NVL(tgt.customer_name, '')
        OR NVL(src.customer_industry, '') <> NVL(tgt.customer_industry, '')
        OR NVL(src.customer_satisfaction_rating, -1) <> NVL(tgt.customer_satisfaction_rating, -1)
        )
;

END TRANSACTION;