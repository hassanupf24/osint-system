"""
Logging Configuration Module
============================
Structured JSON logging with agent context support.

This module provides:
- JSON formatted logging for production
- Text formatting for development
- Agent-aware logging with context
- Performance metrics logging
"""

import logging
import sys
import json
from datetime import datetime, timezone
from typing import Optional, Dict, Any
from pathlib import Path
from logging.handlers import RotatingFileHandler
import threading


class JSONFormatter(logging.Formatter):
    """
    JSON log formatter for structured logging.
    
    Produces JSON lines compatible with log aggregation systems
    like ELK, Splunk, or CloudWatch.
    """
    
    def __init__(
        self,
        include_timestamp: bool = True,
        include_agent_id: bool = True,
    ):
        """
        Initialize the JSON formatter.
        
        Args:
            include_timestamp: Include ISO8601 timestamp in output
            include_agent_id: Include agent_id if present in context
        """
        super().__init__()
        self.include_timestamp = include_timestamp
        self.include_agent_id = include_agent_id
    
    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record as JSON.
        
        Args:
            record: Log record to format
            
        Returns:
            str: JSON formatted log line
        """
        log_data: Dict[str, Any] = {
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        
        if self.include_timestamp:
            log_data["timestamp"] = datetime.now(timezone.utc).isoformat()
        
        # Add source location
        log_data["source"] = {
            "file": record.filename,
            "line": record.lineno,
            "function": record.funcName,
        }
        
        # Add agent context if available
        if self.include_agent_id and hasattr(record, "agent_id"):
            log_data["agent_id"] = record.agent_id
        if hasattr(record, "agent_type"):
            log_data["agent_type"] = record.agent_type
        
        # Add extra fields
        if hasattr(record, "extra_data"):
            log_data["data"] = record.extra_data
        
        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        # Add thread info for async debugging
        log_data["thread"] = {
            "id": record.thread,
            "name": record.threadName,
        }
        
        return json.dumps(log_data, default=str)


class TextFormatter(logging.Formatter):
    """
    Human-readable text formatter for development.
    
    Provides colorized output when running in a terminal.
    """
    
    COLORS = {
        "DEBUG": "\033[36m",     # Cyan
        "INFO": "\033[32m",      # Green
        "WARNING": "\033[33m",   # Yellow
        "ERROR": "\033[31m",     # Red
        "CRITICAL": "\033[35m",  # Magenta
        "RESET": "\033[0m",      # Reset
    }
    
    def __init__(self, use_colors: bool = True):
        """
        Initialize the text formatter.
        
        Args:
            use_colors: Use ANSI colors in output
        """
        super().__init__()
        self.use_colors = use_colors and sys.stdout.isatty()
    
    def format(self, record: logging.LogRecord) -> str:
        """
        Format log record as human-readable text.
        
        Args:
            record: Log record to format
            
        Returns:
            str: Formatted log line
        """
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        level = record.levelname
        
        if self.use_colors:
            color = self.COLORS.get(level, "")
            reset = self.COLORS["RESET"]
            level_str = f"{color}{level:8s}{reset}"
        else:
            level_str = f"{level:8s}"
        
        # Build agent context string
        context_parts = []
        if hasattr(record, "agent_id"):
            context_parts.append(f"agent={record.agent_id}")
        if hasattr(record, "agent_type"):
            context_parts.append(f"type={record.agent_type}")
        
        context_str = f" [{', '.join(context_parts)}]" if context_parts else ""
        
        # Format message
        message = record.getMessage()
        
        # Add extra data if present
        if hasattr(record, "extra_data") and record.extra_data:
            message += f" | {record.extra_data}"
        
        output = f"{timestamp} | {level_str} | {record.name}{context_str} | {message}"
        
        # Add exception info if present
        if record.exc_info:
            output += f"\n{self.formatException(record.exc_info)}"
        
        return output


class AgentLogger(logging.LoggerAdapter):
    """
    Logger adapter that adds agent context to all log messages.
    
    Automatically includes agent_id and agent_type in all log records.
    """
    
    def __init__(
        self,
        logger: logging.Logger,
        agent_id: str,
        agent_type: str,
    ):
        """
        Initialize the agent logger.
        
        Args:
            logger: Base logger instance
            agent_id: Unique agent identifier
            agent_type: Agent type/class name
        """
        super().__init__(logger, {})
        self.agent_id = agent_id
        self.agent_type = agent_type
    
    def process(
        self,
        msg: str,
        kwargs: Dict[str, Any],
    ) -> tuple:
        """
        Process log message to add agent context.
        
        Args:
            msg: Log message
            kwargs: Keyword arguments
            
        Returns:
            Tuple of processed message and kwargs
        """
        # Add agent context to the record
        extra = kwargs.get("extra", {})
        extra["agent_id"] = self.agent_id
        extra["agent_type"] = self.agent_type
        kwargs["extra"] = extra
        return msg, kwargs
    
    def with_context(self, **context: Any) -> "ContextLogger":
        """
        Create a context logger with additional fields.
        
        Args:
            **context: Additional context fields
            
        Returns:
            ContextLogger: Logger with added context
        """
        return ContextLogger(self, context)


class ContextLogger:
    """
    Logger wrapper that adds additional context to all messages.
    """
    
    def __init__(
        self,
        logger: AgentLogger,
        context: Dict[str, Any],
    ):
        """
        Initialize context logger.
        
        Args:
            logger: Parent agent logger
            context: Additional context to include
        """
        self._logger = logger
        self._context = context
    
    def _log(self, level: int, msg: str, *args, **kwargs) -> None:
        """Log with context."""
        extra = kwargs.get("extra", {})
        extra["extra_data"] = self._context
        kwargs["extra"] = extra
        self._logger.log(level, msg, *args, **kwargs)
    
    def debug(self, msg: str, *args, **kwargs) -> None:
        self._log(logging.DEBUG, msg, *args, **kwargs)
    
    def info(self, msg: str, *args, **kwargs) -> None:
        self._log(logging.INFO, msg, *args, **kwargs)
    
    def warning(self, msg: str, *args, **kwargs) -> None:
        self._log(logging.WARNING, msg, *args, **kwargs)
    
    def error(self, msg: str, *args, **kwargs) -> None:
        self._log(logging.ERROR, msg, *args, **kwargs)
    
    def critical(self, msg: str, *args, **kwargs) -> None:
        self._log(logging.CRITICAL, msg, *args, **kwargs)


# Thread-local storage for logging context
_log_context = threading.local()


def set_log_context(**kwargs: Any) -> None:
    """
    Set thread-local logging context.
    
    Args:
        **kwargs: Context fields to set
    """
    if not hasattr(_log_context, "data"):
        _log_context.data = {}
    _log_context.data.update(kwargs)


def clear_log_context() -> None:
    """Clear thread-local logging context."""
    if hasattr(_log_context, "data"):
        _log_context.data.clear()


def get_log_context() -> Dict[str, Any]:
    """
    Get current thread-local logging context.
    
    Returns:
        Dict[str, Any]: Current context
    """
    if not hasattr(_log_context, "data"):
        return {}
    return dict(_log_context.data)


def setup_logging(
    level: str = "INFO",
    log_format: str = "json",
    output: str = "stdout",
    file_path: Optional[str] = None,
    max_file_size: int = 10_000_000,
    backup_count: int = 5,
) -> None:
    """
    Configure global logging settings.
    
    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format: Output format (json or text)
        output: Output destination (stdout, file, both)
        file_path: Path to log file (required if output includes file)
        max_file_size: Maximum log file size in bytes
        backup_count: Number of backup files to keep
    """
    # Get root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper()))
    
    # Remove existing handlers
    root_logger.handlers.clear()
    
    # Create formatter
    if log_format == "json":
        formatter = JSONFormatter()
    else:
        formatter = TextFormatter()
    
    # Add stdout handler
    if output in ("stdout", "both"):
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setFormatter(formatter)
        root_logger.addHandler(stdout_handler)
    
    # Add file handler
    if output in ("file", "both") and file_path:
        # Ensure directory exists
        log_path = Path(file_path)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = RotatingFileHandler(
            file_path,
            maxBytes=max_file_size,
            backupCount=backup_count,
        )
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    
    # Suppress noisy third-party loggers
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)
    logging.getLogger("aio_pika").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance by name.
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        logging.Logger: Configured logger instance
    """
    return logging.getLogger(name)


def get_agent_logger(
    agent_id: str,
    agent_type: str,
    name: Optional[str] = None,
) -> AgentLogger:
    """
    Get a logger instance with agent context.
    
    Args:
        agent_id: Unique agent identifier
        agent_type: Agent type/class name
        name: Optional logger name (defaults to agent_type)
        
    Returns:
        AgentLogger: Logger with agent context
    """
    logger_name = name or f"osint.agents.{agent_type}"
    base_logger = logging.getLogger(logger_name)
    return AgentLogger(base_logger, agent_id, agent_type)
