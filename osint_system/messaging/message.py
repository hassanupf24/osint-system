"""
Message Definition Module
=========================
Core message structures and types for the OSINT system.

This module provides:
- Message dataclass with metadata
- Message type enumeration
- Priority levels
- Serialization utilities
"""

import json
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Optional, Dict, Any, List


class MessageType(Enum):
    """
    Message types used in the OSINT system.
    
    Each type corresponds to a specific data flow or command.
    """
    
    # Control messages
    COMMAND = "command"
    STATUS = "status"
    HEALTH_CHECK = "health_check"
    SHUTDOWN = "shutdown"
    
    # Data collection
    COLLECT_REQUEST = "collect_request"
    COLLECT_RESPONSE = "collect_response"
    RAW_DATA = "raw_data"
    
    # Processing
    PROCESS_REQUEST = "process_request"
    PROCESS_RESPONSE = "process_response"
    ENRICHED_DATA = "enriched_data"
    
    # Analysis
    ANALYSIS_REQUEST = "analysis_request"
    ANALYSIS_RESPONSE = "analysis_response"
    ANALYSIS_RESULT = "analysis_result"
    
    # Alerts
    ALERT = "alert"
    NOTIFICATION = "notification"
    
    # Fusion
    FUSION_REQUEST = "fusion_request"
    FUSION_RESPONSE = "fusion_response"
    CORRELATED_EVENT = "correlated_event"


class MessagePriority(Enum):
    """Message priority levels."""
    
    LOW = 1
    NORMAL = 5
    HIGH = 8
    CRITICAL = 10


@dataclass
class MessageMetadata:
    """
    Message metadata for tracking and routing.
    
    Contains information about message origin, routing,
    and processing history.
    """
    
    # Identifiers
    message_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None
    
    # Routing
    source_agent: Optional[str] = None
    target_agent: Optional[str] = None
    reply_to: Optional[str] = None
    
    # Timing
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    expiry: Optional[str] = None
    
    # Processing
    version: str = "1.0"
    priority: int = MessagePriority.NORMAL.value
    retry_count: int = 0
    max_retries: int = 3
    
    # Tracing
    trace_id: Optional[str] = None
    span_id: Optional[str] = None
    parent_span_id: Optional[str] = None
    
    # Audit
    processing_history: List[Dict[str, Any]] = field(default_factory=list)
    
    def add_processing_step(
        self,
        agent_id: str,
        action: str,
        timestamp: Optional[str] = None,
    ) -> None:
        """
        Record a processing step for audit trail.
        
        Args:
            agent_id: Agent that processed the message
            action: Action performed
            timestamp: Optional timestamp (defaults to now)
        """
        self.processing_history.append({
            "agent_id": agent_id,
            "action": action,
            "timestamp": timestamp or datetime.now(timezone.utc).isoformat(),
        })


@dataclass
class Message:
    """
    Core message structure for inter-agent communication.
    
    Messages are self-describing with type information,
    metadata for routing/tracking, and a flexible payload.
    """
    
    # Message type
    type: MessageType
    
    # Payload data
    payload: Dict[str, Any]
    
    # Metadata
    metadata: MessageMetadata = field(default_factory=MessageMetadata)
    
    # Schema validation
    schema_version: str = "1.0"
    schema_name: Optional[str] = None
    
    @classmethod
    def create(
        cls,
        message_type: MessageType,
        payload: Dict[str, Any],
        source_agent: Optional[str] = None,
        target_agent: Optional[str] = None,
        correlation_id: Optional[str] = None,
        priority: MessagePriority = MessagePriority.NORMAL,
        schema_name: Optional[str] = None,
    ) -> "Message":
        """
        Create a new message with common defaults.
        
        Args:
            message_type: Type of message
            payload: Message data
            source_agent: Sending agent ID
            target_agent: Receiving agent ID
            correlation_id: ID for correlating related messages
            priority: Message priority
            schema_name: Optional schema name for validation
            
        Returns:
            Message: New message instance
        """
        metadata = MessageMetadata(
            source_agent=source_agent,
            target_agent=target_agent,
            correlation_id=correlation_id,
            priority=priority.value,
        )
        
        return cls(
            type=message_type,
            payload=payload,
            metadata=metadata,
            schema_name=schema_name,
        )
    
    @classmethod
    def create_response(
        cls,
        request: "Message",
        response_type: MessageType,
        payload: Dict[str, Any],
        source_agent: str,
    ) -> "Message":
        """
        Create a response message linked to a request.
        
        Args:
            request: Original request message
            response_type: Type of response
            payload: Response data
            source_agent: Responding agent ID
            
        Returns:
            Message: Response message
        """
        metadata = MessageMetadata(
            correlation_id=request.metadata.correlation_id or request.metadata.message_id,
            causation_id=request.metadata.message_id,
            source_agent=source_agent,
            target_agent=request.metadata.source_agent,
            reply_to=request.metadata.reply_to,
            trace_id=request.metadata.trace_id,
            parent_span_id=request.metadata.span_id,
        )
        
        return cls(
            type=response_type,
            payload=payload,
            metadata=metadata,
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert message to dictionary for serialization.
        
        Returns:
            Dict[str, Any]: Message as dictionary
        """
        return {
            "type": self.type.value,
            "payload": self.payload,
            "metadata": asdict(self.metadata),
            "schema_version": self.schema_version,
            "schema_name": self.schema_name,
        }
    
    def to_json(self) -> str:
        """
        Serialize message to JSON string.
        
        Returns:
            str: JSON string
        """
        return json.dumps(self.to_dict(), default=str)
    
    def to_bytes(self) -> bytes:
        """
        Serialize message to bytes for transport.
        
        Returns:
            bytes: Message as bytes
        """
        return self.to_json().encode("utf-8")
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Message":
        """
        Create message from dictionary.
        
        Args:
            data: Dictionary with message data
            
        Returns:
            Message: Reconstructed message
        """
        # Parse metadata
        metadata_data = data.get("metadata", {})
        metadata = MessageMetadata(
            message_id=metadata_data.get("message_id", uuid.uuid4().hex),
            correlation_id=metadata_data.get("correlation_id"),
            causation_id=metadata_data.get("causation_id"),
            source_agent=metadata_data.get("source_agent"),
            target_agent=metadata_data.get("target_agent"),
            reply_to=metadata_data.get("reply_to"),
            timestamp=metadata_data.get("timestamp", datetime.now(timezone.utc).isoformat()),
            expiry=metadata_data.get("expiry"),
            version=metadata_data.get("version", "1.0"),
            priority=metadata_data.get("priority", MessagePriority.NORMAL.value),
            retry_count=metadata_data.get("retry_count", 0),
            max_retries=metadata_data.get("max_retries", 3),
            trace_id=metadata_data.get("trace_id"),
            span_id=metadata_data.get("span_id"),
            parent_span_id=metadata_data.get("parent_span_id"),
            processing_history=metadata_data.get("processing_history", []),
        )
        
        return cls(
            type=MessageType(data["type"]),
            payload=data.get("payload", {}),
            metadata=metadata,
            schema_version=data.get("schema_version", "1.0"),
            schema_name=data.get("schema_name"),
        )
    
    @classmethod
    def from_json(cls, json_str: str) -> "Message":
        """
        Parse message from JSON string.
        
        Args:
            json_str: JSON string
            
        Returns:
            Message: Parsed message
        """
        return cls.from_dict(json.loads(json_str))
    
    @classmethod
    def from_bytes(cls, data: bytes) -> "Message":
        """
        Parse message from bytes.
        
        Args:
            data: Bytes data
            
        Returns:
            Message: Parsed message
        """
        return cls.from_json(data.decode("utf-8"))
    
    def should_retry(self) -> bool:
        """
        Check if message should be retried on failure.
        
        Returns:
            bool: True if retry is allowed
        """
        return self.metadata.retry_count < self.metadata.max_retries
    
    def increment_retry(self) -> None:
        """Increment retry count."""
        self.metadata.retry_count += 1
    
    def is_expired(self) -> bool:
        """
        Check if message has expired.
        
        Returns:
            bool: True if message is expired
        """
        if not self.metadata.expiry:
            return False
        
        expiry_time = datetime.fromisoformat(self.metadata.expiry)
        return datetime.now(timezone.utc) > expiry_time
    
    def __repr__(self) -> str:
        return (
            f"Message(type={self.type.value}, "
            f"id={self.metadata.message_id[:8]}..., "
            f"source={self.metadata.source_agent})"
        )
