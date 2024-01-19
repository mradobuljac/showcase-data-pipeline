import json
from airflow.decorators import dag
from datetime import datetime
from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaInvokeFunctionOperator,
)
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
import configparser


@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",  # at midnight
    default_args={"depends_on_past": True},  # all tasks will have this option enabled
    max_active_runs=1,  # only one DAG run can be executing at any point in time
    catchup=False,  # run all DAG runs from start_date till current data
)
def showcase_data_pipeline():
    # {{ ds }} is a template for logical date. On runtime will be resolved into yyyy-mm-dd corresponding to each dagrun
    # will be sent to lambda functions to orchestrate pipeline around "logical date" concept
    payload = {"date": "{{ ds }}"}

    parser = configparser.ConfigParser()
    parser.read(
        "/opt/airflow/dags/redshift_config.txt"  # location of redshift_config.txt file in docker container
    )
    redshift_cluster = parser.get("redshift", "redshift_cluster")
    redshift_database = parser.get("redshift", "redshift_database")
    redshift_user = parser.get("redshift", "redshift_user")

    # creating and initializing Redshift objects
    redshift_ddl_setup = RedshiftDataOperator(
        task_id="redshift_DDL_setup",
        aws_conn_id="aws_conn",
        cluster_identifier=redshift_cluster,
        database=redshift_database,
        db_user=redshift_user,
        sql="./sql/DDL_setup.sql",
        wait_for_completion=True,
    )

    # get products data from API endpoint and upload to S3 bucket
    products_from_api_to_s3 = LambdaInvokeFunctionOperator(
        task_id="products_to_s3",
        aws_conn_id="aws_conn",
        function_name="showcase_data_pipeline_products_to_s3",
        invocation_type="RequestResponse",
        payload=json.dumps(payload),
    )

    # get sales data from API endpoint and upload to S3 bucket
    sales_from_api_to_s3 = LambdaInvokeFunctionOperator(
        task_id="sales_to_s3",
        aws_conn_id="aws_conn",
        function_name="showcase_data_pipeline_sales_to_s3",
        invocation_type="RequestResponse",
        payload=json.dumps(payload),
    )

    # COPY products data from S3 bucket into Redshift staging table
    s3_to_redshift_stage_products = RedshiftDataOperator(
        task_id="s3_to_redshift_stage_products",
        aws_conn_id="aws_conn",
        cluster_identifier=redshift_cluster,
        database=redshift_database,
        db_user=redshift_user,
        sql="./sql/s3_to_stage_products.sql",
        wait_for_completion=True,
    )

    # COPY sales data from S3 bucket into Redshift staging table
    s3_to_redshift_stage_sales = RedshiftDataOperator(
        task_id="s3_to_redshift_stage_sales",
        aws_conn_id="aws_conn",
        cluster_identifier=redshift_cluster,
        database=redshift_database,
        db_user=redshift_user,
        sql="./sql/s3_to_stage_sales.sql",
        wait_for_completion=True,
    )

    # upsert dim_products table with new data
    upsert_dim_products = RedshiftDataOperator(
        task_id="upsert_dim_products",
        aws_conn_id="aws_conn",
        cluster_identifier=redshift_cluster,
        database=redshift_database,
        db_user=redshift_user,
        sql="./sql/upsert_dim_products.sql",
        wait_for_completion=True,
    )

    # upsert fact_sales table with new data
    upsert_fact_sales = RedshiftDataOperator(
        task_id="upsert_fact_sales",
        aws_conn_id="aws_conn",
        cluster_identifier=redshift_cluster,
        database=redshift_database,
        db_user=redshift_user,
        sql="./sql/upsert_fact_sales.sql",
        wait_for_completion=True,
    )

    # dependencies setup
    redshift_ddl_setup >> [products_from_api_to_s3, sales_from_api_to_s3]
    products_from_api_to_s3 >> s3_to_redshift_stage_products
    sales_from_api_to_s3 >> s3_to_redshift_stage_sales
    (
        [s3_to_redshift_stage_products, s3_to_redshift_stage_sales]
        >> upsert_dim_products
        >> upsert_fact_sales
    )


showcase_data_pipeline()
