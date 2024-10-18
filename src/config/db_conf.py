import xml.etree.ElementTree as ET

from sqlalchemy import create_engine, text

from src.config.logging_conf import logger


def connect_to_db(db_uri):
    try:
        # Create a SQLAlchemy engine
        engine = create_engine(db_uri)
        return engine
    except Exception as e:
        logger.error(f"Error connecting to the database: {e}")
        raise e


def load_queries_from_xml(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()

    queries = {}
    for query in root.findall('query'):
        query_id = query.get('id')
        query_text = query.text.strip()
        queries[query_id] = query_text
    return queries


def execute_raw_queries(str_sql, engine):
    with engine.connect() as conn:
        # Begin a transaction
        trans = conn.begin()
        try:
            logger.info(f"Executing raw queries for: {str_sql}")
            conn.execute(text(str_sql))
            # Commit the transaction
            trans.commit()
            logger.info("SQL executed successfully and changes committed.")
        except Exception as e:
            # Rollback in case of error
            trans.rollback()
            logger.error(f"Error executing SQL: {e}")
            raise e
