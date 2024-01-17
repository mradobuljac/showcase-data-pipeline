import json
import requests as r
import awswrangler as wr
import pandas as pd
import os

API = os.environ["ENDPOINT"]
ENDPOINT = "https://5vhs3sddtk.execute-api.us-east-1.amazonaws.com/products"
BUCKET_NAME = "mradobuljac-bucket"
FILE_NAME = "products.csv"
FULL_PATH = f"s3://{BUCKET_NAME}/{FILE_NAME}"


def lambda_handler(event, context):
    try:
        resp = r.get(ENDPOINT)
        resp.raise_for_status()
    except r.RequestException as e:
        print(e)
        return resp.status_code

    data = resp.json()

    # Extract header and data rows
    header = data[0]
    data_rows = data[1:]

    # Create Pandas DataFrame
    df = pd.DataFrame(data_rows, columns=header)

    # get {{ ds }} from Airflow
    if event:
        ds = event["ds"]
    else:
        ds = "2024-01-01"  # dummy value for dev purposes

    # write dataframe directly to s3
    wr.s3.to_csv(df, f"s3://{BUCKET_NAME}/{ds}/{FILE_NAME}", index=False)

    print(f"API: {API}")

    # TODO implement
    return {"statusCode": 200, "body": json.dumps("Success")}
