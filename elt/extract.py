import os

from dotenv import load_dotenv

from config.common import PARENT_FOLDER
from config.db_conf import connect_to_db
from config.logging_conf import logger
from config.pandas_conf import select_to_df, save_to_csv

load_dotenv()

DB_URI = os.getenv("DB_URI")


def extract_data():
    # Connect to the database
    engine = connect_to_db(DB_URI)
    if engine is None:
        logger.error("Could not connect to the database")
        return

    sql_folder = os.path.join(PARENT_FOLDER, "sql")
    for filename in os.listdir(sql_folder):
        if filename.endswith('.sql'):
            file_path = os.path.join(sql_folder, filename)  # Get the full path to the file
            with open(file_path, 'r') as file:
                str_sql = file.read()  # Read the content of the file
                print(f"Contents of {filename}: {str_sql}\n")  # Print the content

            df = select_to_df(str_sql, engine)

            # Save the transformed data to a CSV file
            csv_path_customer = os.path.join(PARENT_FOLDER, "data", "source", filename.replace(".sql", ".csv"))
            save_to_csv(df, csv_path_customer)

    logger.info("successfully extracted data")


if __name__ == '__main__':
    extract_data()
