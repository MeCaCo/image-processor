from sqlalchemy import Column, String, DateTime, JSON
from database import Base
import datetime


class ImageTask(Base):
    __tablename__ = "image_tasks"

    id = Column(String, primary_key=True, index=True)
    filename = Column(String)
    status = Column(String, default="uploaded")
    operations = Column(JSON, nullable=True)
    original_path = Column(String)
    result_path = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)