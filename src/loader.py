import os
import json
from pathlib import Path
import sys

# Structure Path
current_file_path = Path(__file__).resolve()
project_root = current_file_path.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from api.database import create_db


class TelegramLoader:
    def __init__(self):
        """
        1. Connect to PostgreSQL using psycopg2
        2. Set up a cursor.
        """
        self.conn = create_db()
        self.cursor = self.conn.cursor()

    def create_tables(self):
        """
        Creates the 'raw' schema and the 'telegram_messages' table if they don't exist.
        """
        # 1. Create Schema
        create_schema_query = "CREATE SCHEMA IF NOT EXISTS raw;"

        # 2. Create Table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS raw.telegram_messages (
            id SERIAL PRIMARY KEY,
            message_id BIGINT NOT NULL,       
            channel_name VARCHAR(255),
            channel_title VARCHAR(255),
            message_date TIMESTAMP,
            message_text TEXT,
            has_media BOOLEAN DEFAULT FALSE,
            views INTEGER DEFAULT 0,          
            forwards INTEGER DEFAULT 0,       
            UNIQUE(channel_name, message_id) 
        );
        """
        try:
            self.cursor.execute(create_schema_query)
            self.cursor.execute(create_table_query)
            self.conn.commit()
            print("Schema and Table initialized.")
        except Exception as e:
            self.conn.rollback()
            print(f"Error creating schema: {e}")

    def read_json_files(self, raw_data_dir):
        """
        Iterates through the data lake directory and inserts data.
        """
        print(f"Scanning directory: {raw_data_dir}")
        files_processed = 0

        for roots, dirs, files in os.walk(raw_data_dir):
            for file in files:
                if file.endswith(".json"):
                    file_path = os.path.join(roots, file)
                    with open(file_path, "r", encoding="utf-8") as f:
                        try:
                            data = json.load(f)
                            self.insert_message(data)
                            files_processed += 1
                        except json.JSONDecodeError:
                            print(f"Skipping invalid JSON: {file}")
                        except Exception as e:
                            print(f"Error processing {file}: {e}")

        self.conn.commit()
        print(f"Processed {files_processed} files!")

    def insert_message(self, data):
        """
        Inserts a single message record into the database.
        """
        insert_query = """
        INSERT INTO raw.telegram_messages 
        (message_id, channel_name, channel_title, message_date, message_text, has_media, views, forwards)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (channel_name, message_id) DO NOTHING;
        """

        values = (
            data.get("message_id"),
            data.get("channel_name"),
            data.get("channel_title"),
            data.get("message_date"),
            data.get("message_text"),
            data.get("has_media"),
            data.get("views", 0),
            data.get("forwards", 0),
        )

        try:
            self.cursor.execute(insert_query, values)
        except Exception as e:
            self.conn.rollback()
            print(f"Insert Error: {e}")
            raise

    def close(self):
        """Close connection."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("Connection closed.")


if __name__ == "__main__":

    DATA_LAKE_PATH = "../data/raw/telegram_messages"

    # Execution
    loader = TelegramLoader()
    try:
        loader.create_tables()
        loader.read_json_files(DATA_LAKE_PATH)
    finally:
        loader.close()
