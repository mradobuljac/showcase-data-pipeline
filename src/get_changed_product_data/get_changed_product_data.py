import csv
from random import randint
import json

NUM_CHANGES = 3  # number of updates to source data file
SOURCE_DATA_FILE = r"product_data.txt"


def lambda_handler(event, context):
    # read source data file
    with open(SOURCE_DATA_FILE) as f:
        products = list(csv.reader(f))

    # change ProductRating value of randomly selected products
    rands = [randint(1, 30) for _ in range(NUM_CHANGES)]
    for rand in rands:
        products[rand][3] = randint(1, 100)

    return {"statusCode": 200, "body": json.dumps(products)}
