from pydantic import BaseModel


class Message(BaseModel):
    message_id: str
    channel_name: str
    message_date: str
    message_text: str
    has_media: bool
    image_path: str
    views: int
    forwards: int
