import json
from airflow.decorators import dag
from datetime import datetime
from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaInvokeFunctionOperator,
)
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
import configparser


@dag(
    start_date=datetime(2024, 1, 10),
    schedule="@daily",  # at midnight
    default_args={
        "depends_on_past": True,
        "aws_conn_id": "aws_conn",
    },  # all tasks will have this option enabled
    max_active_runs=1,  # only one DAG run can be executing at any point in time
    catchup=False,  # run all DAG runs from start_date till current data
)
def showcase_data_pipeline():
    # {{ ds }} is a template for logical date. On runtime will be resolved into yyyy-mm-dd corresponding to each dagrun
    # will be sent to lambda functions to orchestrate pipeline around "logical date" concept
    # as a side note, {{ ds }} is also used in .sql files to ensure only data for this DAG run is moved and transformed

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
        cluster_identifier=redshift_cluster,
        database=redshift_database,
        db_user=redshift_user,
        sql="./sql/DDL_setup.sql",
        wait_for_completion=True,
    )

    # get sales data from API endpoint and upload to S3 bucket
    sales_from_api_to_s3 = LambdaInvokeFunctionOperator(
        task_id="sales_to_s3",
        function_name="showcase_data_pipeline_sales_to_s3",
        invocation_type="RequestResponse",
        payload=json.dumps(
            {"date": "{{ ds }}"}
        ),  # extract only delta data for incremental load. Also necessary for S3 prefix
    )

    # get products data from API endpoint and upload to S3 bucket
    products_from_api_to_s3 = LambdaInvokeFunctionOperator(
        task_id="products_to_s3",
        function_name="showcase_data_pipeline_dims_to_s3",
        invocation_type="RequestResponse",
        payload=json.dumps(
            {
                "date": "{{ ds }}",
                "dimension": "products",
            }  # delta extract not needed for dims, but necessary for S3 prefix
        ),
    )

    # get customers data from API endpoint and upload to S3 bucket
    customers_from_api_to_s3 = LambdaInvokeFunctionOperator(
        task_id="customers_to_s3",
        function_name="showcase_data_pipeline_dims_to_s3",
        invocation_type="RequestResponse",
        payload=json.dumps(
            {
                "date": "{{ ds }}",
                "dimension": "customers",
            }  # delta extract not needed for dims, but necessary for S3 prefix
        ),
    )

    # COPY products data from S3 bucket into Redshift staging table
    s3_to_redshift_stage_products = RedshiftDataOperator(
        task_id="s3_to_redshift_stage_products",
        cluster_identifier=redshift_cluster,
        database=redshift_database,
        db_user=redshift_user,
        sql="./sql/s3_to_stage_products.sql",
        wait_for_completion=True,
    )

    # data quality actions for product data
    product_data_quality = RedshiftDataOperator(
        task_id="product_data_quality",
        cluster_identifier=redshift_cluster,
        database=redshift_database,
        db_user=redshift_user,
        sql="./sql/product_data_quality.sql",
        wait_for_completion=True,
    )

    # COPY customers data from S3 bucket into Redshift staging table
    s3_to_redshift_stage_customers = RedshiftDataOperator(
        task_id="s3_to_redshift_stage_customers",
        cluster_identifier=redshift_cluster,
        database=redshift_database,
        db_user=redshift_user,
        sql="./sql/s3_to_stage_customers.sql",
        wait_for_completion=True,
    )

    # data quality actions for customer data
    customer_data_quality = RedshiftDataOperator(
        task_id="customer_data_quality",
        cluster_identifier=redshift_cluster,
        database=redshift_database,
        db_user=redshift_user,
        sql="./sql/customer_data_quality.sql",
        wait_for_completion=True,
    )

    # COPY sales data from S3 bucket into Redshift staging table
    s3_to_redshift_stage_sales = RedshiftDataOperator(
        task_id="s3_to_redshift_stage_sales",
        cluster_identifier=redshift_cluster,
        database=redshift_database,
        db_user=redshift_user,
        sql="./sql/s3_to_stage_sales.sql",
        wait_for_completion=True,
    )

    # data quality actions for sales data
    sales_data_quality = RedshiftDataOperator(
        task_id="sales_data_quality",
        cluster_identifier=redshift_cluster,
        database=redshift_database,
        db_user=redshift_user,
        sql="./sql/sales_data_quality.sql",
        wait_for_completion=True,
    )

    # upsert dim_products table with new data
    upsert_dim_products_scd1 = RedshiftDataOperator(
        task_id="upsert_dim_products_scd1",
        cluster_identifier=redshift_cluster,
        database=redshift_database,
        db_user=redshift_user,
        sql="./sql/upsert_dim_products.sql",
        wait_for_completion=True,
    )

    # upsert dim_customers table with new data
    upsert_dim_customers_scd2 = RedshiftDataOperator(
        task_id="upsert_dim_customers_scd2",
        cluster_identifier=redshift_cluster,
        database=redshift_database,
        db_user=redshift_user,
        sql="./sql/upsert_dim_customers.sql",
        wait_for_completion=True,
    )

    # upsert fact_sales table with new data
    upsert_fact_sales = RedshiftDataOperator(
        task_id="upsert_fact_sales",
        cluster_identifier=redshift_cluster,
        database=redshift_database,
        db_user=redshift_user,
        sql="./sql/upsert_fact_sales.sql",
        wait_for_completion=True,
    )

    # upsert fact_sales_aggregate table with new data
    upsert_fact_sales_aggregate = RedshiftDataOperator(
        task_id="upsert_fact_sales_aggregate",
        cluster_identifier=redshift_cluster,
        database=redshift_database,
        db_user=redshift_user,
        sql="./sql/upsert_fact_sales_aggregate.sql",
        wait_for_completion=True,
    )

    # dependencies setup
    redshift_ddl_setup >> [
        products_from_api_to_s3,
        sales_from_api_to_s3,
        customers_from_api_to_s3,
    ]
    products_from_api_to_s3 >> s3_to_redshift_stage_products >> product_data_quality
    sales_from_api_to_s3 >> s3_to_redshift_stage_sales >> sales_data_quality
    customers_from_api_to_s3 >> s3_to_redshift_stage_customers >> customer_data_quality
    (
        [product_data_quality, sales_data_quality, customer_data_quality]
        >> upsert_dim_products_scd1
        >> upsert_dim_customers_scd2
        >> upsert_fact_sales
        >> upsert_fact_sales_aggregate
    )


showcase_data_pipeline()
