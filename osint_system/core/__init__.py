"""
OSINT System Core Module
========================
Core infrastructure components for the OSINT multi-agent platform.

This module provides:
- Configuration management
- Logging infrastructure
- Base agent classes
- Common utilities
"""

from .config import Config, get_config
from .logging_config import setup_logging, get_logger
from .base_agent import BaseAgent, AgentStatus
from .exceptions import (
    OSINTException,
    AgentException,
    MessageException,
    StorageException,
    ValidationException,
)

__all__ = [
    "Config",
    "get_config",
    "setup_logging",
    "get_logger",
    "BaseAgent",
    "AgentStatus",
    "OSINTException",
    "AgentException",
    "MessageException",
    "StorageException",
    "ValidationException",
]
