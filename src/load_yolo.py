from pathlib import Path
import sys

# Structure Path
current_file_path = Path(__file__).resolve()
project_root = current_file_path.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
from scripts.constants import YOLO_OUTPUT_CSV


# 1. Setup
load_dotenv()
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")


def load_yolo_to_postgres():
    # 2. Connect to DB
    connection_str = (
        f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    engine = create_engine(connection_str)

    print("Reading CSV...")
    if not os.path.exists(YOLO_OUTPUT_CSV):
        print(f"Error: File {YOLO_OUTPUT_CSV} not found. Did you run yolo_detect.py?")
        return

    df = pd.read_csv(YOLO_OUTPUT_CSV)

    # 3. Write to Postgres (Raw Layer)
    print(f"Loading {len(df)} rows to raw.image_detections...")
    df.to_sql("image_detections", engine, schema="raw", if_exists="append", index=False)

    print("Success! Data loaded.")


if __name__ == "__main__":
    load_yolo_to_postgres()
