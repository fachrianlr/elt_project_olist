import os

from dotenv import load_dotenv

from src.config.common import PARENT_FOLDER
from src.config.db_conf import connect_to_db
from src.config.logging_conf import logger
from src.config.pandas_conf import select_to_df, inner_join_df, df_to_sql

load_dotenv()

SRC_DB_URI = os.getenv("SRC_DB_URI")
DWH_DB_URI = os.getenv("DWH_DB_URI")


def read_file(file_path):
    with open(file_path, "r") as file:
        return file.read()


def transform_data():
    src_engine = connect_to_db(SRC_DB_URI)
    dwh_engine = connect_to_db(DWH_DB_URI)
    if src_engine is None:
        logger.error("Could not connect to the database")
        return

    logger.info("start transform data")

    try:
        sql_folder = os.path.join(PARENT_FOLDER, "data", "sql")

        order_sql = os.path.join(sql_folder, "order.sql")
        order_df = select_to_df(read_file(order_sql), src_engine)

        dim_customer_sql = os.path.join(sql_folder, "dim_customer.sql")
        dim_customer_df = select_to_df(read_file(dim_customer_sql), dwh_engine)

        merge_order_df = inner_join_df(order_df, dim_customer_df, "customer_id")
        merge_order_df = merge_order_df.drop("customer_id", axis=1)
        df_to_sql(dwh_engine, merge_order_df, "fact_order")

        order_item_sql = os.path.join(sql_folder, "order_item.sql")
        order_item_df = select_to_df(read_file(order_item_sql), src_engine)

        fact_order_sql = os.path.join(sql_folder, "fact_order.sql")
        fact_order_df = select_to_df(read_file(fact_order_sql), dwh_engine)

        dim_product_sql = os.path.join(sql_folder, "dim_product.sql")
        dim_product_df = select_to_df(read_file(dim_product_sql), dwh_engine)

        dim_seller_sql = os.path.join(sql_folder, "dim_seller.sql")
        dim_seller_df = select_to_df(read_file(dim_seller_sql), dwh_engine)

        merge_order_item_df = inner_join_df(order_item_df, fact_order_df, "order_id")
        merge_order_item_df = inner_join_df(merge_order_item_df, dim_product_df, "product_id")
        merge_order_item_df = inner_join_df(merge_order_item_df, dim_seller_df, "seller_id")
        merge_order_item_df = merge_order_item_df.drop(['order_id', 'product_id', 'seller_id'], axis=1)
        df_to_sql(dwh_engine, merge_order_item_df, "fact_order_items")

    except Exception as e:
        logger.error(f"failed to load data: {e}")


if __name__ == '__main__':
    transform_data()
