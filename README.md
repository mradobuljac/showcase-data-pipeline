# Showcase Data Solution

## Overview

Provide a brief introduction to the project, its purpose, and the problems it aims to solve. Highlight key features and benefits for potential users or contributors.

## Architecture

Include an image that visually represents the project's architecture. Briefly describe the high-level components and how they interact.
![Architecture Diagram](link-to-image)

Explain airflow pipeline
![Airflow pipeline](link-to-image)


## Data Model

draw.io

dim_date(PK)
dim_product(PK, NK, scd1, housekeeping)
dim_customer(PK, NK, scd2, housekeeping)
fact_sales (PK,FK, cust_key je late arriving dimension, housekeeping cols)
log_data_quality


## Tech

	Airflow             # Orchestrator for Lambda functions and Redshift code 
	Docker              # Enables consistent and reproducible local Airflow deployment
	GitHub Actions      # CI/CD service, automating building and deployment of data solution to AWS cloud
	GitHub              # Version control
	SQL                 # Data modeling and transformation logic within Redshift 
	Python              # Backend language for HTTP APIs in AWS Lambda; also used for API data extraction logic and writing Airflow orchestration code
	AWS SAM Framework   # SAM templates and SAM CLI for development, testing, and deployment of serverless apps, including Lambda functions and API Gateway endpoints
	AWS SAM Template    # IaaC for the project, extends AWS CloudFormation
	AWS Lambda          # Serverless backend service for APIs, and API data extraction logic
	AWS S3              # Object storage and data lake 
	AWS Redshift        # Cloud data warehouse 
	AWS API Gateway     # Hosts API endpoints, acts as a data source

## Project Structure

```bash
/project-root
|--/.github/
|   |--/workflows
|      |--cicd.yaml                         # CI/CD pipeline definition
|-- /airflow
|   |--/dags
|      |--/sql
|         |--DDL_setup.sql                  # sql code for schema and table creation
|         |--s3_to_stage_products.sql       # sql code for loading data from S3 into Redshift
|         |--s3_to_stage_sales.sql          # sql code for loading data from S3 into Redshift
|         |--upsert_dim_products.sql        # sql code for updating from stage into dim table
|         |--upsert_fact_sales.sql          # sql code for updating from stage into fact table
|      |-- redshift_config.txt              # sql config file for Redshift connection in Airflow
|      |-- showcase_data_pipeline.txt       # airflow data pipeline definition
|   docker-compose.yaml                     # docker-compose file for starting up Airflow containers locally
|-- /src
|   |--/get_changed_product_data
|      |--get_changed_product_data.py       # lambda function code
|      |--product_data.txt                  # file containing products
|      |--requirements.txt                  # requirements file for this lambda function
|   |--/get_generated_sales_data
|      |--get_changed_product_data.py       # lambda function code
|   |--/dims_to_s3
|      |--dims_to_s3.py                 # lambda function code
|      |--requirements.py                   # requirements file for this lambda function
|   |--/sales_to_s3
|      |--dims_to_s3.py                 # lambda function code
|      |--requirements.py                   # requirements file for this lambda function
|--.gitignore
|--.pre-commit-config.yaml                  # pre-commit hooks executing Black code formatter before each commit
|--requirements-dev                         # requirements file holding python dependencies used during development
|--samconfig.toml                           # config file containing AWS SAM framework configuration
|--template.yaml                            # file holding AWS SAM IaaC
|--README.md
```

## Setup

### Clone the repository
git clone https://github.com/mradobuljac/showcase-data-pipeline

### Install necessary tools
- Install Docker
- Install AWS CLI and configure access_key and secret_access_key
- Install AWS SAM CLI

### Redshift 
- Provisioning Redshift from IaaC template is not in scope of this project. Please create your own cluster
- Please ensure that IAM role attached to your cluster can COPY from S3, and that Cluster can be accessed from local clients
- change airflow/dags/redshift_config.txt values to your cluster values
- change IAM Role in s3_to_stage_products.sql and s3_to_stage_sales.sql to IAM role attached to your cluster

### Setup Airflow locally
```bash
cd showcase-data-pipeline/airflow
docker compose up airflow-init
docker compose up
```
- alternatively, follow official instructions https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html
- to enter web UI to manage and observe workflows, go to http://localhost:8080/home
- username: airflow
- password: airflow 
- Go to Admin -> Connections -> Add a new record 
- 
- IMAGE


airflow connections --add --conn_id your_connection_id --conn_uri "your_connection_uri"



# Deploy SAM project to cloud
- sam build 
- sam deploy