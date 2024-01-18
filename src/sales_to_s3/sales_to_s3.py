import json
import os
import requests as r
import awswrangler as wr
import pandas as pd

ENDPOINT = os.environ["ENDPOINT"]
BUCKET_NAME = os.environ["BUCKET"]
FILE_NAME = "sales.parquet"


def lambda_handler(event, context):

    # get logical date from Airflow
    if "date" in event:
        date = event["date"]
    else:
        date = "2024-01-01"  # dummy value for dev purposes

    payload = {"date": date}

    try:
        resp = r.get(ENDPOINT, params=payload)
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

    # write dataframe directly to s3
    full_path = f"s3://{BUCKET_NAME}/{date}/{FILE_NAME}"
    wr.s3.to_parquet(df, path=full_path, index=False)

    return {"statusCode": 200, "body": json.dumps("Success")}
