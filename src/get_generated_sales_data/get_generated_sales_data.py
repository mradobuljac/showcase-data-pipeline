from random import randint, uniform
import uuid
import json
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

NUM_OF_GENERATED_ROWS = 50


def lambda_handler(event: dict, context: dict) -> dict:
    """Back-end code for API Gateway endpoint serving sales data

    :param event: Payload sent by API Gateway. Used to extract date parameter from endpoint URL
    :param context: Managed by AWS. Contains info about function execution and environment
    :return: json formatted list of semi-randomly generated sales transactions
    """

    # generate NUM_OF_GENERATED_ROWS rows of random fact data
    sales_data = []
    for i in range(NUM_OF_GENERATED_ROWS):
        d = {}

        if "queryStringParameters" in event:  # date_id
            d["date_id"] = event["queryStringParameters"]["date"]
        else:
            d[
                "date_id"
            ] = "2024-01-01"  # fallback value if date queryStringParameter is omitted from endpoint URL

        d["product_id"] = randint(1, 100)
        d["customer_id"] = randint(1, 100)
        d["quantity_sold"] = randint(1, 100)
        d["revenue"] = uniform(1.0, 1000)
        d["transaction_code"] = str(uuid.uuid4())
        logging.info(f"Generated row: {d}")

        sales_data.append(d)

    return {"statusCode": 200, "body": json.dumps(sales_data)}
