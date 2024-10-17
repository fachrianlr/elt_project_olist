import pandas as pd

from src.config.logging_conf import logger


def total_data_csv(file_path):
    df = pd.read_csv(file_path)
    return len(df)


def select_to_df(sql, engine):
    try:
        logger.info(f"select data from : {format(sql)}")
        df = pd.read_sql_query(sql, engine)
        return df
    except Exception as e:
        logger.error(f"Error querying the database: {e}")
        return None


def df_to_sql(df, table_name, engine):
    try:
        df.to_sql(table_name, con=engine, if_exists='append', index=False)
        logger.info(f"Table {table_name} successfully inserted")
    except Exception as e:
        logger.error(f"Error insert to database: {e}")


def csv_to_sql(csv_file, table_name, engine):
    try:
        df = pd.read_csv(csv_file)
        df.to_sql(table_name, con=engine, if_exists='append', index=False)
        logger.info(f"Source CSV: {csv_file}, Table Name: {table_name} Successfully Inserted")
    except Exception as e:
        logger.error(f"Error insert to database: {e}")


def save_to_csv(df, file_path):
    try:
        df.to_csv(file_path, index=False)
        logger.info(f"Data successfully saved to {file_path}")
    except Exception as e:
        logger.error(f"Error saving data to CSV: {e}")


def inner_join_df(df1, df2, column_id):
    merged_df = pd.merge(df1, df2, on=column_id, how='inner')
    return merged_df
