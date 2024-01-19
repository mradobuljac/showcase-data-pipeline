import os
import requests as r
import awswrangler as wr
import pandas as pd
import logging

# initialization code to take advantage of hot-start containers
if logging.getLogger().hasHandlers():
    # Lambda environment pre-configures a handler logging to stderr. If a handler is already configured,
    # `.basicConfig` does not execute. Thus, we set the level directly.
    logging.getLogger().setLevel(logging.INFO)
else:
    logging.basicConfig(
        level=logging.INFO, format=" %(asctime)s -  %(levelname)s -  %(message)s"
    )

ENDPOINT = os.environ["ENDPOINT"]
BUCKET_NAME = os.environ["BUCKET"]
FILE_NAME = "products.csv"


def lambda_handler(event: dict, context: dict) -> str:
    """Reads products data from API Gateway endpoint and uploads to S3 bucket

    :param event: Payload sent by Airflow in form of {"date": "{{ ds }}"}. Holds pipeline execution logical date
    :param context: Managed by AWS. Contains info about function execution and environment
    :return: execution status result
    """
    try:
        resp = r.get(ENDPOINT)
        resp.raise_for_status()
    except r.RequestException as e:
        logging.info(e)
        return "Request failed"

    # extract data
    data = resp.json()

    # Create Pandas DataFrame
    df = pd.DataFrame(data)

    # get pipeline execution logical date from Airflow
    # used to partition S3 bucket upload location
    if "date" in event:
        date = event["date"]
    else:
        date = "2024-01-01"  # fallback value

    # write dataframe directly to s3
    full_path = f"s3://{BUCKET_NAME}/{date}/{FILE_NAME}"
    logging.info(f"File upload location: {full_path}")
    wr.s3.to_csv(df, path=full_path, index=False)

    return "Success"
