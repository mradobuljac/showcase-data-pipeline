from random import randint, uniform
import uuid
import json

NUM_OF_GENERATED_ROWS = 50


def lambda_handler(event, context):
    headers = [
        "date_id",
        "product_id",
        "quantity_sold",
        "revenue",
        "transaction_code",
    ]

    sales_data = []
    sales_data.append(headers)

    # generate 30 rows of random fact data
    for i in range(NUM_OF_GENERATED_ROWS):
        data_row = []

        if "queryStringParameters" in event:  # date_id
            data_row.append(event["queryStringParameters"]["date"])
        else:
            data_row.append(
                "2024-01-01"
            )  # dummy values if date queryStringParameter is omitted from endpoint URL
        data_row.append(randint(1, 100))  # product_id
        data_row.append(randint(1, 100))  # quantity_sold
        data_row.append(uniform(1.0, 1000))  # revenue
        data_row.append(str(uuid.uuid4()))  # transaction_code

        sales_data.append(data_row)

    return {"statusCode": 200, "body": json.dumps(sales_data)}
