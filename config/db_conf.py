
from sqlalchemy import create_engine

from config.logging_conf import logger


def connect_to_db(db_uri):
    try:
        # Create a SQLAlchemy engine
        engine = create_engine(db_uri)
        return engine
    except Exception as e:
        logger.error(f"Error connecting to the database: {e}")
        return None
