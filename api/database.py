import psycopg2
from dotenv import load_dotenv
import os

load_dotenv(override=True)


def get_db_config():
    return {
        "DB_USER": os.getenv("DB_USER"),
        "DB_PASSWORD": os.getenv("DB_PASSWORD"),
        "DB_NAME": os.getenv("DB_NAME"),
        "DB_HOST": os.getenv("DB_HOST"),
        "DB_PORT": os.getenv("DB_PORT"),
    }


def create_db():
    try:
        cfg = get_db_config()
        conn = psycopg2.connect(
            dbname=cfg["DB_NAME"],
            user=cfg["DB_USER"],
            password=cfg["DB_PASSWORD"],
            host=cfg["DB_HOST"],
            port=cfg["DB_PORT"],
        )
        print("Connected to PostgreSQL successfully!")
        return conn
    except Exception as e:
        raise ConnectionError(f"Error connecting to PostgreSQL: {e}")
