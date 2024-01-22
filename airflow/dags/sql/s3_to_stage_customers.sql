TRUNCATE TABLE showcase_data_pipeline.stage_customers;

COPY showcase_data_pipeline.stage_customers
FROM 's3://showcase-data-pipeline/{{ ds }}/customers.csv'
IAM_ROLE 'arn:aws:iam::534389016860:role/redshift-s3-full-access'
IGNOREHEADER 1
DELIMITER ','
EMPTYASNULL
;