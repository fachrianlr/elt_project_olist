import csv
import os
from datetime import datetime

from dotenv import load_dotenv

from src.config.common import PARENT_FOLDER
from src.config.db_conf import connect_to_db
from src.config.logging_conf import logger
from src.config.pandas_conf import csv_to_sql, total_data_csv

load_dotenv()

DB_URI = os.getenv("DWH_DB_URI")


def read_file(file_path):
    with open(file_path, "r") as file:
        return file.read()


def write_csv(path, fieldnames, data):
    with open(path, mode="w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)

        # Write the header (field names)
        writer.writeheader()

        # Write the data rows
        writer.writerows(data)


def load_data(file_mapping):
    # Connect to the database
    engine = connect_to_db(DB_URI)
    if engine is None:
        logger.error("Could not connect to the database")
        return

    logger.info("start load data")

    try:
        extract_folder = os.path.join(PARENT_FOLDER, "data", "extract")
        load_folder = os.path.join(PARENT_FOLDER, "data", "load")

        csv_name_list = []
        start_date_list = []
        end_date_list = []
        total_data_list = []
        for csv_name, table_name in file_mapping.items():
            start_date = datetime.now()

            csv_path = os.path.join(extract_folder, csv_name)
            csv_to_sql(engine, csv_path, table_name)

            end_date = datetime.now()

            csv_name_list.append(csv_name)
            start_date_list.append(start_date)
            end_date_list.append(end_date)
            total_data_list.append(total_data_csv(csv_path))

        data = {
            "csv_name": csv_name_list,
            "start_date": start_date_list,
            "end_date": end_date_list,
            "total_data": total_data_list
        }

        csv_file_path = os.path.join(load_folder, "status.csv")

        with open(csv_file_path, mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(data.keys())
            writer.writerows(zip(*data.values()))

    except Exception as e:
        logger.error(f"failed to load data: {e}")


if __name__ == '__main__':
    file_mapping = {
        'dim_customer.csv': 'dim_customer',
        'dim_product.csv': 'dim_product',
        'dim_seller.csv': 'dim_seller'
    }
    load_data(file_mapping)
