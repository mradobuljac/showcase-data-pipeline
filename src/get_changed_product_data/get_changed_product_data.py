import csv
import random
from random import randint
import json
from faker import Faker
import logging


# initialization code to take advantage of hot-start containers
fake = Faker()
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

    # read source data file containing base products
    with open(SOURCE_DATA_FILE) as f:
        products = list(csv.reader(f))

    # change ProductRating value of randomly selected products
    rands = [randint(1, 30) for _ in range(NUM_CHANGES)]
    for rand in rands:
        products[rand][3] = randint(1, 100)
        logging.info(f"Changed existing product to: {products[rand]}")

    # generate new rows
    categories = (
        "Electronics",
        "Audio",
        "Accessories",
        "Home & Living",
        "Fitness",
        "Wearables",
    )
    for _ in range(NUM_NEW_ROWS):
        data_row = []
        data_row.append(randint(50, 100))  # ProductId
        data_row.append(
            f"{fake.word().capitalize()} {fake.word().capitalize()}"  # ProductName
        )
        data_row.append(random.choice(categories))  # ProductCategory
        data_row.append(randint(1, 100))  # ProductRating
        logging.info(f"Newly generated product: {data_row}")

        products.append(data_row)

    print(products)

    return {"statusCode": 200, "body": json.dumps(products)}
