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

BUCKET_NAME = os.environ["BUCKET"]


def lambda_handler(event: dict, context: dict) -> str:
    """Reads products data from API Gateway endpoint and uploads to S3 bucket

    :param event: Payload sent by Airflow in form of {"date": "{{ ds }}", "dimension": "products/customers"}.
        Governs whether this function returns products or customers data
    :param context: Managed by AWS. Contains info about function execution and environment
    :return: execution status result
    """

    # setup
    if event["dimension"] == "products":
        endpoint = os.environ["PRODUCTS_ENDPOINT"]
        file_name = "products.csv"
    elif event["dimension"] == "customers":
        endpoint = os.environ["CUSTOMERS_ENDPOINT"]
        file_name = "customers.csv"
    else:
        logging.info("payload format wrong")
        logging.info(event)
        return "Failure"

    try:
        resp = r.get(endpoint)
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
    full_path = f"s3://{BUCKET_NAME}/{date}/{file_name}"
    logging.info(f"File upload location: {full_path}")
    wr.s3.to_csv(df, path=full_path, index=False)

    return "Success"
