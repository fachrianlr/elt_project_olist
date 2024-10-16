import pandas as pd

from src.config.logging_conf import logger


def select_to_df(engine, sql):
    try:
        # SQL query to select data
        query = sql
        # Read query result into a pandas DataFrame using SQLAlchemy engine
        df = pd.read_sql_query(query, engine)
        return df
    except Exception as e:
        logger.error(f"Error querying the database: {e}")
        return None


def csv_to_sql(engine, csv_file, table_name):
    try:
        df = pd.read_csv(csv_file)
        df.to_sql(table_name, con=engine, if_exists='append', index=False)
        logger.info(f"Source CSV: {csv_file}, Table Name: {table_name} Successfully Inserted")
    except Exception as e:
        logger.error(f"Error insert to database: {e}")


def save_to_csv(df, file_path, index=False, header=False):
    try:
        df.to_csv(file_path, index=False)
        logger.info(f"Data successfully saved to {file_path}")
    except Exception as e:
        logger.error(f"Error saving data to CSV: {e}")
