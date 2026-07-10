import psycopg
from loguru import logger
from typing import List, Tuple


class PostgresManager:
    """
    A unified connection and execution manager for PostgreSQL operations.
    Implements the Context Manager protocol for safe resource handling.
    """

    def __init__(self, db_config: dict):
        self.db_config = db_config
        self.conn = None
        self.cur = None

    def __enter__(self):
        logger.info(
            f"Establishing connection to PostgreSQL ({self.db_config.get('dbname')})..."
        )
        try:
            self.conn = psycopg.connect(**self.db_config)
            self.cur = self.conn.cursor()
            return self
        except psycopg.Error as e:
            logger.critical(f"Failed to connect to the database: {e}")
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.cur:
            self.cur.close()
        if self.conn:
            if exc_type is not None:
                logger.warning(
                    f"Exception detected ({exc_type.__name__}). Rolling back transaction..."
                )
                self.conn.rollback()
            else:
                self.conn.commit()

            self.conn.close()
            logger.info("Database connection closed cleanly.")

    def execute_query(self, query: str, params: tuple = None) -> None:
        """Executes a single SQL query."""
        try:
            self.cur.execute(query, params)
        except psycopg.Error as e:
            logger.error(f"Error executing query: {query[:50]}... \nDetails: {e}")
            raise

    def fetch_one(self, query: str, params: tuple = None):
        """Executes a query and returns a single result."""
        self.execute_query(query, params)
        return self.cur.fetchone()

    def executemany_records(self, query: str, data: List[Tuple]) -> None:
        """Executes a bulk insert/upsert operation."""
        try:
            self.cur.executemany(query, data)
        except psycopg.Error as e:
            logger.error(f"Error executing bulk operation. Details: {e}")
            raise
