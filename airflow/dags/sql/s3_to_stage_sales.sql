TRUNCATE TABLE showcase_data_pipeline.stage_sales;

COPY showcase_data_pipeline.stage_sales
FROM 's3://showcase-data-pipeline/{{ ds }}/sales.parquet'
IAM_ROLE 'arn:aws:iam::534389016860:role/redshift-s3-full-access'
FORMAT AS PARQUET
;

