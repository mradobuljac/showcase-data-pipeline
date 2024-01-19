from random import randint, uniform
import uuid
import json
import logging

# initialization code to take advantage of hot-start containers
logging.basicConfig(
    level=logging.INFO, format=" %(asctime)s -  %(levelname)s -  %(message)s"
)

NUM_OF_GENERATED_ROWS = 50


def lambda_handler(event: dict, context: dict) -> dict:
    """Back-end code for API Gateway endpoint serving sales data

    :param event: Payload sent by API Gateway. Used to extract date parameter from endpoint URL
    :param context: Managed by AWS. Contains info about function execution and environment
    :return: json formatted list of semi-randomly generated sales transactions
    """

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
                "2024-01-01"  # dummy values if date queryStringParameter is omitted from endpoint URL
            )
        data_row.append(randint(1, 100))  # product_id
        data_row.append(randint(1, 100))  # quantity_sold
        data_row.append(uniform(1.0, 1000))  # revenue
        data_row.append(str(uuid.uuid4()))  # transaction_code
        logging.info(f"Generated row: {data_row}")

        sales_data.append(data_row)

    return {"statusCode": 200, "body": json.dumps(sales_data)}
