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
NUM_NEW_ROWS = 3  # number of newly generated customer rows
SOURCE_FILE_NUM_CUSTS = 30
SOURCE_DATA_FILE = r"customer_data.txt"


def lambda_handler(event: any, context: dict) -> dict:
    """Back-end code for API Gateway endpoint serving customer data

    :param event: Payload sent by API Gateway. Not used in this function
    :param context: Managed by AWS. Contains info about function execution and environment
    :return: json formatted list of semi-randomly generated customers
    """

    # read source data file containing base customers into list of dicts
    customers = []
    with open(SOURCE_DATA_FILE) as f:
        reader = csv.reader(f)
        next(reader, None)  # skip headers row
        for customer in reader:
            d = {
                "CustomerId": customer[0],
                "CustomerName": customer[1],
                "Industry": customer[2],
                "CustomerSatisfactionRating": customer[3],
            }
            customers.append(d)

    # change CustomerSatisfactionRating value of randomly selected customers
    rands = [randint(1, SOURCE_FILE_NUM_CUSTS - 1) for _ in range(NUM_CHANGES)]
    for rand in rands:
        customers[rand]["CustomerSatisfactionRating"] = randint(1, 100)
        logging.info(f"Changed existing customer to: {customers[rand]}")

    # generate new rows
    industry = (
        "Technology",
        "Consulting",
        "Aerospace",
        "Robotics",
        "Automotive",
        "Manufacturing",
        "Fashion",
        "Software",
        "Finance",
        "Engineering",
    )

    for _ in range(NUM_NEW_ROWS):
        d = {
            "CustomerId": randint(50, 100),
            "CustomerName": f"{fake.company().title()}",
            "Industry": random.choice(industry),
            "CustomerSatisfactionRating": randint(1, 100),
        }
        logging.info(f"Newly generated customer: {d}")
        customers.append(d)

    return {"statusCode": 200, "body": json.dumps(customers)}
