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


def load_data():
    # Connect to the database
    engine = connect_to_db(DB_URI)
    if engine is None:
        logger.error("Could not connect to the database")
        return

    logger.info("start load data")

    try:
        sql_folder = os.path.join(PARENT_FOLDER, "data", "load")

        dim_customer_csv = os.path.join(sql_folder, "dim_customer.csv")
        csv_to_sql(engine, dim_customer_csv, "dim_customer")

        dim_seller_csv = os.path.join(sql_folder, "dim_seller.csv")
        csv_to_sql(engine, dim_seller_csv, "dim_seller")

        dim_product_csv = os.path.join(sql_folder, "dim_product.csv")
        csv_to_sql(engine, dim_product_csv, "dim_product")

        logger.info("successfully loaded data")
    except Exception as e:
        logger.error(f"failed to load data: {e}")


if __name__ == '__main__':
    load_data()
