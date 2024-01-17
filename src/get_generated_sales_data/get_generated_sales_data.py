from random import randint, uniform
import uuid
import json
import awswrangler as wr
import pandas as pd

NUM_OF_GENERATED_ROWS = 30
BUCKET_NAME = "mradobuljac-bucket"
FILE_NAME = "sales_data.parquet"


def lambda_handler(event, context):
    # get {{ ds }} from Airflow
    if "ds" in event:
        ds = event["ds"]
    else:
        ds = "2024-01-01"  # dummy value for dev purposes

    headers = [
        "date_id",
        "customer_id",
        "product_id",
        "quantity_sold",
        "revenue",
        "transaction_code",
    ]
    data = []

    # generate 30 rows of random fact data
    for i in range(NUM_OF_GENERATED_ROWS):
        data_row = []

        if "ds" in event:  # date_id
            data_row.append(event["ds"])
        else:
            data_row.append("2020-01-01")
        data_row.append(randint(1, 30))  # customer_id
        data_row.append(randint(1, 30))  # product_id
        data_row.append(randint(1, 100))  # quantity_sold
        data_row.append(uniform(1.0, 1000))  # revenue
        data_row.append(str(uuid.uuid4()))  # transaction_code

        data.append(data_row)

    # create dataframe
    df = pd.DataFrame(data=data, columns=headers)

    # write dataframe directly to s3
    wr.s3.to_parquet(df, f"s3://{BUCKET_NAME}/{ds}/{FILE_NAME}", index=False)

    # TODO implement
    return {"statusCode": 200, "body": json.dumps("Success")}
