import os

from dotenv import load_dotenv

from src.config.common import PARENT_FOLDER
from src.config.db_conf import connect_to_db
from src.config.logging_conf import logger
from src.config.pandas_conf import select_to_df, save_to_csv

load_dotenv()

DB_URI = os.getenv("SRC_DB_URI")


def read_file(file_path):
    with open(file_path, "r") as file:
        file_content = file.read()
        logger.info(f" file : {file_path}, file content : {file_content}")
        return file_content


def extract_data(file_mapping):
    # Connect to the database
    engine = connect_to_db(DB_URI)
    if engine is None:
        logger.error("Could not connect to the database")
        return

    logger.info("start extract data")

    sql_folder = os.path.join(PARENT_FOLDER, "data", "sql")

    for sql_name, csv_name in file_mapping.items():
        sql_path = os.path.join(sql_folder, sql_name)
        str_sql = read_file(sql_path)
        df = select_to_df(engine, str_sql)
        dim_customer_path = os.path.join(PARENT_FOLDER, "data", "load", csv_name)
        save_to_csv(df, dim_customer_path)


if __name__ == '__main__':
    file_mapping = {
        'customers.sql': 'dim_customer.csv',
        'products.sql': 'dim_product.csv',
        'sellers.sql': 'dim_seller.csv'
    }
    extract_data(file_mapping)
