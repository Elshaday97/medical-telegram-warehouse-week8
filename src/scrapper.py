import logging
import os
from pathlib import Path
import sys
import json

# Structure Path
current_file_path = Path(__file__).resolve()
project_root = current_file_path.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import asyncio
from datetime import datetime
from telethon import TelegramClient
from telethon.errors import FloodWaitError
from scripts.constants import (
    IMG_FILE_PATH,
    MSG_FILE_PATH,
    MAX_TELEGRAM_RETRIES,
)
from dotenv import load_dotenv

load_dotenv()

# Create the logs directory if it doesn't exist
os.makedirs("../logs", exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("../logs/scrapper.log"),
    ],
)

logger = logging.getLogger("telegram_scraper")


class TelegramScrapper:
    def __init__(self, app_id, app_hash):
        """
        Initialize the scraper with API credentials and setup the client.
        """
        self.client = TelegramClient("anon", api_id=app_id, api_hash=app_hash)

    def set_up_directories(self, channel_username):
        """
        Create the data lake directory structure for a specific channel.
        Requirement: data/raw/telegram_messages/YYYY-MM-DD/
        """

        today = datetime.now().strftime("%Y-%m-%d")

        msg_path = f"{MSG_FILE_PATH}{today}"
        img_path = f"{IMG_FILE_PATH}{channel_username}"

        os.makedirs(msg_path, exist_ok=True)
        os.makedirs(img_path, exist_ok=True)

        return msg_path, img_path

    async def scrape_channel(self, channel_username: str):
        """
        Main logic to scrape a single channel.
        """
        retries = 0
        async with self.client:
            try:
                # Get Channel Info
                entity = await self.client.get_entity(channel_username)
                channel_title = entity.title
                logging.info(f"Scrapping messages for {channel_title}")
                msg_path, img_path = self.set_up_directories(channel_username)
                data = {}
                async for message in self.client.iter_messages(
                    channel_username, limit=100
                ):
                    full_img_path = f"{img_path}/{message.id}"
                    # 1. Check for Photo
                    if message.photo:
                        path = await message.download_media(file=full_img_path)
                        data["image_path"] = path

                    # 2. Extract the data
                    data = {
                        "message_id": message.id,
                        "channel_name": channel_username,
                        "channel_title": channel_title,
                        "message_date": message.date.isoformat(),
                        "message_text": message.message or "",
                        "has_media": bool(message.photo),
                        "views": message.views or 0,
                        "forwards": message.forwards or 0,
                    }
                    # 3. Save JSON to Data Lake
                    filename = f"{msg_path}/{channel_username}_{message.id}.json"
                    with open(filename, "w", encoding="utf-8") as f:
                        json.dump(data, f, ensure_ascii=False, indent=4)

            except FloodWaitError as e:
                wait_seconds = int(getattr(e, "seconds", 0) or 0)
                wait_seconds = max(wait_seconds, 1)
                logger.warning(
                    f"FloodWaitError for {channel_username}: sleeping {wait_seconds}s"
                )
                await asyncio.sleep(wait_seconds)
                retries += 1
                if retries > MAX_TELEGRAM_RETRIES:
                    logger.error(
                        f"Too many FloodWait retries for {channel_username}. Skipping."
                    )
                    return 0
            except Exception as e:
                logger.error(f"Error scraping {channel_username}: {e}")
                return 0

    def run(self, channels: list):
        """
        Orchestrator to run the scraper over a list of channels.
        """

        with self.client:
            for channel_username in channels:
                self.client.loop.run_until_complete(
                    self.scrape_channel(channel_username=channel_username)
                )


if __name__ == "__main__":

    APP_ID = os.getenv("TELEGRAM_API_ID")
    APP_HASH = os.getenv("TELEGRAM_API_HASH")
    CHANNELS = [
        "@cheMed123",  # CheMed - Medical products
        "@lobelia4cosmetics",  # Lobelia - Cosmetics and health products
        "@tikvahpharma",
        "@tenamereja",
    ]

    scrapper = TelegramScrapper(APP_ID, APP_HASH)
    scrapper.run(CHANNELS)
