import os

from dotenv import load_dotenv

from src.config.common import PARENT_FOLDER
from src.config.db_conf import connect_to_db
from src.config.logging_conf import logger
from src.config.pandas_conf import csv_to_sql

load_dotenv()

DB_URI = os.getenv("DWH_DB_URI")


def read_file(file_path):
    with open(file_path, "r") as file:
        return file.read()


def load_data(file_mapping):
    # Connect to the database
    engine = connect_to_db(DB_URI)
    if engine is None:
        logger.error("Could not connect to the database")
        return

    logger.info("start load data")

    try:
        sql_folder = os.path.join(PARENT_FOLDER, "data", "load")

        for csv_name, table_name in file_mapping.items():
            csv_path = os.path.join(sql_folder, csv_name)
            csv_to_sql(engine, csv_path, table_name)

    except Exception as e:
        logger.error(f"failed to load data: {e}")


if __name__ == '__main__':
    file_mapping = {
        'dim_customer.csv': 'dim_customer',
        'dim_product.csv': 'dim_product',
        'dim_seller.csv': 'dim_seller'
    }
    load_data(file_mapping)
