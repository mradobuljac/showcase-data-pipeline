import csv
import random
from random import randint
import json
from faker import Faker
import logging


# initialization code to take advantage of hot-start containers
fake = Faker()
if logging.getLogger().hasHandlers():
    # Lambda environment pre-configures a handler logging to stderr. If a handler is already configured,
    # `.basicConfig` does not execute. Thus, we set the level directly.
    logging.getLogger().setLevel(logging.INFO)
else:
    logging.basicConfig(
        level=logging.INFO, format=" %(asctime)s -  %(levelname)s -  %(message)s"
    )

NUM_CHANGES = 3  # number of updates to source data file
NUM_NEW_ROWS = 3  # number of newly generated product rows
SOURCE_DATA_FILE = r"product_data.txt"


def lambda_handler(event: any, context: dict) -> dict:
    """Back-end code for API Gateway endpoint serving product data

    :param event: Payload sent by API Gateway. Not used in this function
    :param context: Managed by AWS. Contains info about function execution and environment
    :return: json formatted list of semi-randomly generated products
    """

    # read source data file containing base products into list of dicts
    products = []
    with open(SOURCE_DATA_FILE) as f:
        reader = csv.reader(f)
        next(reader, None)  # skip headers row
        for product in reader:
            d = {
                "ProductId": product[0],
                "ProductName": product[1],
                "ProductCategory": product[2],
                "ProductRating": product[3],
            }
            products.append(d)

    # change ProductRating value of randomly selected products
    rands = [randint(1, 30) for _ in range(NUM_CHANGES)]
    for rand in rands:
        products[rand]["ProductRating"] = randint(1, 100)
        logging.info(f"Changed existing product to: {products[rand]}")

    # generate new rows
    categories = (
        "Electronics",
        "Audio",
        "Accessories",
        "Home & Living",
        "Fitness",
        "Wearables",
        "Gadgets",
        "Smart Home",
        "Virtual Reality",
        "Outdoor",
    )

    for _ in range(NUM_NEW_ROWS):
        d = {
            "ProductId": randint(50, 100),
            "ProductName": f"{fake.word().capitalize()} {fake.word().capitalize()}",
            "ProductCategory": random.choice(categories),
            "ProductRating": randint(1, 100),
        }
        logging.info(f"Newly generated product: {d}")
        products.append(d)

    return {"statusCode": 200, "body": json.dumps(products)}
