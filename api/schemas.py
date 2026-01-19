from pydantic import BaseModel


class Message(BaseModel):
    message_id: str
    channel_name: str
    channel_title: str
    message_date: str
    message_text: str
    has_media: bool
    image_path: str
    views: int
    forwards: int
