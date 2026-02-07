"""
Database Models
===============
SQLAlchemy ORM models for the OSINT storage layer.
"""

from typing import Dict, Any, Optional
from datetime import datetime
import uuid

from sqlalchemy import Column, String, Integer, JSON, DateTime, Boolean, ForeignKey
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

class RawData(Base):
    """
    Model for raw collected data.
    Stores data exactly as received from collectors.
    """
    __tablename__ = "raw_data"
    
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    source = Column(String, index=True, nullable=False)
    source_id = Column(String, index=True) # ID from the original source if available (e.g., tweet ID)
    target = Column(String, index=True)    # The original target identifier
    
    data = Column(JSON, nullable=False)
    metadata = Column(JSON, nullable=True)
    
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    processed = Column(Boolean, default=False)
    
    def __repr__(self):
        return f"<RawData(id={self.id}, source={self.source}, timestamp={self.timestamp})>"

class ProcessedData(Base):
    """
    Model for processed analysis results.
    """
    __tablename__ = "processed_data"
    
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    analysis_type = Column(String, index=True, nullable=False)
    target_id = Column(String, index=True) # Link to RawData ID or target ID?
    
    result = Column(JSON, nullable=False)
    score = Column(Integer, nullable=True) # Risk score if applicable
    
    timestamp = Column(DateTime, default=datetime.utcnow)
    version = Column(String, default="1.0")
    
    def __repr__(self):
        return f"<ProcessedData(id={self.id}, type={self.analysis_type}, score={self.score})>"

class User(Base):
    """
    Model for API users.
    """
    __tablename__ = "users"
    
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    username = Column(String, unique=True, index=True, nullable=False)
    hashed_password = Column(String, nullable=False)
    is_active = Column(Boolean, default=True)
    is_superuser = Column(Boolean, default=False)
    
    def __repr__(self):
        return f"<User(username={self.username})>"
