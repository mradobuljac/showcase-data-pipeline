import json
import os
import requests as r
import awswrangler as wr
import pandas as pd


ENDPOINT = os.environ["ENDPOINT"]
BUCKET_NAME = os.environ["BUCKET"]
FILE_NAME = "products.csv"
FULL_PATH = f"s3://{BUCKET_NAME}/{FILE_NAME}"


def lambda_handler(event, context):
    try:
        resp = r.get(ENDPOINT)
        resp.raise_for_status()
    except r.RequestException as e:
        print(e)
        return "Request failed"

    data = resp.json()

    # Extract header and data rows
    header = data[0]
    data_rows = data[1:]

    # Create Pandas DataFrame
    df = pd.DataFrame(data_rows, columns=header)

    # get {{ ds }} from Airflow
    if "ds" in event:
        ds = event["ds"]
    else:
        ds = "2024-01-01"  # dummy value for dev purposes

    # write dataframe directly to s3
    wr.s3.to_csv(df, f"s3://{BUCKET_NAME}/{ds}/{FILE_NAME}", index=False)

    return {"statusCode": 200, "body": json.dumps("Success")}
