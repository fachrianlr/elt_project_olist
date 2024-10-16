import csv
import os

from dotenv import load_dotenv

from src.config.common import PARENT_FOLDER
from src.config.db_conf import connect_to_db
from src.config.logging_conf import logger

load_dotenv()

DB_URI = os.getenv("DWH_DB_URI")


def read_file(file_path):
    with open(file_path, "r") as file:
        return file.read()


def write_csv(path, data):
    with open(path, mode="w", newline="") as file:
        fieldnames = ["name", "age", "city"]
        writer = csv.DictWriter(file, fieldnames=fieldnames)

        # Write the header (field names)
        writer.writeheader()

        # Write the data rows
        writer.writerows(data)


def transform_data(data):
    # Connect to the database
    engine = connect_to_db(DB_URI)
    if engine is None:
        logger.error("Could not connect to the database")
        return

    logger.info("start transform data")

    try:
        sql_folder = os.path.join(PARENT_FOLDER, "data", "transform", "test.csv")
        write_csv(sql_folder, data)

    except Exception as e:
        logger.error(f"failed to load data: {e}")


if __name__ == '__main__':
    data = [
        {"name": "Alice", "age": 30, "city": "New York"},
        {"name": "Bob", "age": 25, "city": "Los Angeles"},
        {"name": "Charlie", "age": 35, "city": "Chicago"}
    ]
    transform_data(data)
