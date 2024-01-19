import os
import requests as r
import awswrangler as wr
import pandas as pd
import logging

# initialization code to take advantage of hot-start containers
logging.basicConfig(
    level=logging.INFO, format=" %(asctime)s -  %(levelname)s -  %(message)s"
)

ENDPOINT = os.environ["ENDPOINT"]
BUCKET_NAME = os.environ["BUCKET"]
FILE_NAME = "sales.parquet"


def lambda_handler(event: dict, context: dict) -> str:
    """Reads sales data from API Gateway endpoint and uploads to S3 bucket

    :param event: Payload sent by Airflow in form of {"date": "{{ ds }}"}. Holds pipeline execution logical date
    :param context: Managed by AWS. Contains info about function execution and environment
    :return: execution status result
    """

    # get pipeline execution logical date from Airflow
    if "date" in event:
        date = event["date"]
    else:
        date = "2024-01-01"  # fallback value

    payload = {"date": date}
    logging.info(f"Query string params sent to /sales endpoint: {payload}")

    try:
        resp = r.get(ENDPOINT, params=payload)
        resp.raise_for_status()
    except r.RequestException as e:
        logging.info(e)
        return "Request failed"

    data = resp.json()

    # Extract header and data rows
    header = data[0]
    data_rows = data[1:]

    # Specify column data types to match with Redshift table
    dtype_mapping = {
        "date_id": "DATE",
        "product_id": "INTEGER",
        "quantity_sold": "INTEGER",
        "revenue": "FLOAT",
        "transaction_code": "VARCHAR(100)",
    }

    # Create Pandas DataFrame
    df = pd.DataFrame(data_rows, columns=header)

    # write dataframe directly to s3
    full_path = f"s3://{BUCKET_NAME}/{date}/{FILE_NAME}"
    logging.info(f"File upload location: {full_path}")
    wr.s3.to_parquet(df, path=full_path, index=False, dtype=dtype_mapping)

    return "Success"
