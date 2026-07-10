import os
from dotenv import load_dotenv


def get_db_config():
    load_dotenv()

    db_config = {
        "host": os.getenv("DATABASE_HOST"),
        "port": os.getenv("DATABASE_PORT"),
        "dbname": os.getenv("DATABASE_NAME"),
        "user": os.getenv("DATABASE_USER"),
        "password": os.getenv("DATABASE_PASS"),
    }

    return db_config
