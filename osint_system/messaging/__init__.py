"""
Messaging Module
================
Message broker abstraction layer supporting RabbitMQ and Kafka.

This module provides:
- Unified messaging interface
- JSON schema validation
- Message serialization/deserialization
- Publisher and consumer abstractions
"""

from .broker import MessageBroker, get_message_broker
from .message import Message, MessageType, MessagePriority
from .publisher import Publisher
from .consumer import Consumer
from .schemas import validate_message, get_schema

__all__ = [
    "MessageBroker",
    "get_message_broker",
    "Message",
    "MessageType",
    "MessagePriority",
    "Publisher",
    "Consumer",
    "validate_message",
    "get_schema",
]
