from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, Date, Time, Boolean, ForeignKey, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship
from datetime import datetime
import uuid
from typing import Generator

from .config import settings

Base = declarative_base()

# Configure database engine with proper connection pooling for concurrent webhooks
engine = create_engine(
    settings.database_url,
    pool_size=20,          # Number of connections to maintain in pool
    max_overflow=30,       # Additional connections beyond pool_size  
    pool_timeout=30,       # Seconds to wait for connection
    pool_recycle=3600,     # Recycle connections after 1 hour
    pool_pre_ping=True,    # Validate connections before use
    echo=settings.debug    # SQL query logging based on debug mode
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db() -> Generator[Session, None, None]:
    """Get database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


class Project(Base):
    __tablename__ = "projects"
    
    id = Column(Integer, primary_key=True, index=True)
    project_id = Column(String, unique=True, index=True, nullable=False)
    name = Column(String, nullable=False)
    configuration = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    is_active = Column(Boolean, default=True)
    
    bookings = relationship("Booking", back_populates="project")
    messages = relationship("MessageQueue", back_populates="project")
    dialogues = relationship("Dialogue", back_populates="project")
    feedback_records = relationship("Feedback", back_populates="project")


class MessageQueue(Base):
    __tablename__ = "message_queue"
    
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    project_id = Column(String, ForeignKey("projects.project_id"), nullable=False)
    client_id = Column(String, nullable=False, index=True)
    original_message = Column(Text, nullable=False)
    aggregated_message = Column(Text, nullable=False)
    status = Column(String, nullable=False, default="pending")
    priority = Column(Integer, default=0)
    retry_count = Column(Integer, default=0)
    max_retries = Column(Integer, default=settings.message_retry_attempts)
    processing_timeout = Column(Integer, default=settings.message_processing_timeout)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    processed_at = Column(DateTime, nullable=True)
    
    project = relationship("Project", back_populates="messages")


class ClientLastActivity(Base):
    __tablename__ = "client_last_activity"
    
    id = Column(Integer, primary_key=True, index=True)
    project_id = Column(String, ForeignKey("projects.project_id"), nullable=False)
    client_id = Column(String, nullable=False, index=True)
    last_message_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    zip_history = Column(Text, nullable=True)  # Compressed dialogue history
    last_compression_at = Column(DateTime, nullable=True)  # When last compression happened
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class Booking(Base):
    __tablename__ = "bookings"
    
    id = Column(Integer, primary_key=True, index=True)
    project_id = Column(String, ForeignKey("projects.project_id"), nullable=False)
    client_id = Column(String, nullable=False, index=True)
    client_name = Column(String, nullable=False)
    client_phone = Column(String, nullable=True)
    specialist_name = Column(String, nullable=False)
    service_name = Column(String, nullable=False)
    appointment_date = Column(Date, nullable=False, index=True)
    appointment_time = Column(Time, nullable=False)
    duration_minutes = Column(Integer, default=settings.slot_duration_minutes)
    status = Column(String, nullable=False, default="active")
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    project = relationship("Project", back_populates="bookings")


class Dialogue(Base):
    __tablename__ = "dialogues"
    
    id = Column(Integer, primary_key=True, index=True)
    project_id = Column(String, ForeignKey("projects.project_id"), nullable=False)
    client_id = Column(String, nullable=False, index=True)
    role = Column(String, nullable=False)  # "client" or "claude"
    message = Column(Text, nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    is_archived = Column(Boolean, default=False)
    archive_hours = Column(Integer, default=settings.dialogue_archive_hours)
    compressed_content = Column(Text, nullable=True)
    
    project = relationship("Project", back_populates="dialogues")


class Feedback(Base):
    __tablename__ = "feedback"
    
    id = Column(Integer, primary_key=True, index=True)
    project_id = Column(String, ForeignKey("projects.project_id"), nullable=False)
    client_id = Column(String, nullable=False, index=True)
    booking_id = Column(Integer, ForeignKey("bookings.id"), nullable=True)
    rating = Column(Integer, nullable=True)
    comment = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)
    
    project = relationship("Project", back_populates="feedback_records")
    booking = relationship("Booking")


def create_tables():
    """Create all database tables"""
    Base.metadata.create_all(bind=engine)


def drop_tables():
    """Drop all database tables (use with caution!)"""
    if settings.debug:  # Only allow in debug mode
        Base.metadata.drop_all(bind=engine)
    else:
        raise RuntimeError("Cannot drop tables in production mode") 