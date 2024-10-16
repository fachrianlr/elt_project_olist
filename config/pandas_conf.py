import pandas as pd

from config.logging_conf import logger


def select_to_df(sql, engine):
    try:
        # SQL query to select data
        query = sql
        # Read query result into a pandas DataFrame using SQLAlchemy engine
        df = pd.read_sql_query(query, engine)
        return df
    except Exception as e:
        logger.error(f"Error querying the database: {e}")
        return None


def save_to_csv(df, file_path):
    try:
        df.to_csv(file_path, index=False)
        logger.info(f"Data successfully saved to {file_path}")
    except Exception as e:
        logger.error(f"Error saving data to CSV: {e}")
