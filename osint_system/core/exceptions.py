"""
Custom Exceptions Module
========================
Centralized exception definitions for the OSINT system.

This module defines a hierarchy of exceptions for:
- General system errors
- Agent-specific errors
- Messaging errors
- Storage errors
- Validation errors
"""

from typing import Optional, Dict, Any


class OSINTException(Exception):
    """
    Base exception for all OSINT system errors.
    
    Provides structured error information including:
    - Error code for programmatic handling
    - Additional context data
    - Cause tracking for exception chaining
    """
    
    def __init__(
        self,
        message: str,
        code: str = "OSINT_ERROR",
        details: Optional[Dict[str, Any]] = None,
        cause: Optional[Exception] = None,
    ):
        """
        Initialize the exception.
        
        Args:
            message: Human-readable error message
            code: Machine-readable error code
            details: Additional context data
            cause: Original exception that caused this error
        """
        super().__init__(message)
        self.message = message
        self.code = code
        self.details = details or {}
        self.cause = cause
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert exception to dictionary for logging/API responses.
        
        Returns:
            Dict[str, Any]: Exception data as dictionary
        """
        result = {
            "error": self.code,
            "message": self.message,
        }
        if self.details:
            result["details"] = self.details
        if self.cause:
            result["cause"] = str(self.cause)
        return result
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(code={self.code!r}, message={self.message!r})"


class AgentException(OSINTException):
    """Exception raised by agents during operation."""
    
    def __init__(
        self,
        message: str,
        agent_id: str,
        agent_type: str,
        code: str = "AGENT_ERROR",
        details: Optional[Dict[str, Any]] = None,
        cause: Optional[Exception] = None,
    ):
        """
        Initialize agent exception.
        
        Args:
            message: Human-readable error message
            agent_id: Unique identifier of the agent
            agent_type: Type/class of the agent
            code: Machine-readable error code
            details: Additional context data
            cause: Original exception that caused this error
        """
        super().__init__(message, code, details, cause)
        self.agent_id = agent_id
        self.agent_type = agent_type
    
    def to_dict(self) -> Dict[str, Any]:
        result = super().to_dict()
        result["agent_id"] = self.agent_id
        result["agent_type"] = self.agent_type
        return result


class AgentStartupError(AgentException):
    """Exception raised when an agent fails to start."""
    
    def __init__(
        self,
        message: str,
        agent_id: str,
        agent_type: str,
        details: Optional[Dict[str, Any]] = None,
        cause: Optional[Exception] = None,
    ):
        super().__init__(
            message, agent_id, agent_type, "AGENT_STARTUP_ERROR", details, cause
        )


class AgentShutdownError(AgentException):
    """Exception raised when an agent fails to shut down gracefully."""
    
    def __init__(
        self,
        message: str,
        agent_id: str,
        agent_type: str,
        details: Optional[Dict[str, Any]] = None,
        cause: Optional[Exception] = None,
    ):
        super().__init__(
            message, agent_id, agent_type, "AGENT_SHUTDOWN_ERROR", details, cause
        )


class AgentTimeoutError(AgentException):
    """Exception raised when an agent operation times out."""
    
    def __init__(
        self,
        message: str,
        agent_id: str,
        agent_type: str,
        timeout_seconds: float,
        details: Optional[Dict[str, Any]] = None,
        cause: Optional[Exception] = None,
    ):
        details = details or {}
        details["timeout_seconds"] = timeout_seconds
        super().__init__(
            message, agent_id, agent_type, "AGENT_TIMEOUT_ERROR", details, cause
        )


class MessageException(OSINTException):
    """Exception raised during message processing."""
    
    def __init__(
        self,
        message: str,
        message_id: Optional[str] = None,
        queue: Optional[str] = None,
        code: str = "MESSAGE_ERROR",
        details: Optional[Dict[str, Any]] = None,
        cause: Optional[Exception] = None,
    ):
        """
        Initialize message exception.
        
        Args:
            message: Human-readable error message
            message_id: ID of the message that caused the error
            queue: Name of the queue involved
            code: Machine-readable error code
            details: Additional context data
            cause: Original exception that caused this error
        """
        super().__init__(message, code, details, cause)
        self.message_id = message_id
        self.queue = queue
    
    def to_dict(self) -> Dict[str, Any]:
        result = super().to_dict()
        if self.message_id:
            result["message_id"] = self.message_id
        if self.queue:
            result["queue"] = self.queue
        return result


class MessageSerializationError(MessageException):
    """Exception raised when message serialization fails."""
    
    def __init__(
        self,
        message: str,
        message_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        cause: Optional[Exception] = None,
    ):
        super().__init__(
            message, message_id, None, "MESSAGE_SERIALIZATION_ERROR", details, cause
        )


class MessageDeliveryError(MessageException):
    """Exception raised when message delivery fails."""
    
    def __init__(
        self,
        message: str,
        message_id: Optional[str] = None,
        queue: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        cause: Optional[Exception] = None,
    ):
        super().__init__(
            message, message_id, queue, "MESSAGE_DELIVERY_ERROR", details, cause
        )


class MessageValidationError(MessageException):
    """Exception raised when message validation fails."""
    
    def __init__(
        self,
        message: str,
        validation_errors: Optional[Dict[str, Any]] = None,
        message_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        cause: Optional[Exception] = None,
    ):
        details = details or {}
        if validation_errors:
            details["validation_errors"] = validation_errors
        super().__init__(
            message, message_id, None, "MESSAGE_VALIDATION_ERROR", details, cause
        )


class StorageException(OSINTException):
    """Exception raised during storage operations."""
    
    def __init__(
        self,
        message: str,
        storage_type: str,
        operation: str,
        code: str = "STORAGE_ERROR",
        details: Optional[Dict[str, Any]] = None,
        cause: Optional[Exception] = None,
    ):
        """
        Initialize storage exception.
        
        Args:
            message: Human-readable error message
            storage_type: Type of storage involved (database, cache, etc.)
            operation: Operation that failed (read, write, delete, etc.)
            code: Machine-readable error code
            details: Additional context data
            cause: Original exception that caused this error
        """
        super().__init__(message, code, details, cause)
        self.storage_type = storage_type
        self.operation = operation
    
    def to_dict(self) -> Dict[str, Any]:
        result = super().to_dict()
        result["storage_type"] = self.storage_type
        result["operation"] = self.operation
        return result


class StorageConnectionError(StorageException):
    """Exception raised when storage connection fails."""
    
    def __init__(
        self,
        message: str,
        storage_type: str,
        details: Optional[Dict[str, Any]] = None,
        cause: Optional[Exception] = None,
    ):
        super().__init__(
            message, storage_type, "connect", "STORAGE_CONNECTION_ERROR", details, cause
        )


class StorageReadError(StorageException):
    """Exception raised when storage read fails."""
    
    def __init__(
        self,
        message: str,
        storage_type: str,
        key: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        cause: Optional[Exception] = None,
    ):
        details = details or {}
        if key:
            details["key"] = key
        super().__init__(
            message, storage_type, "read", "STORAGE_READ_ERROR", details, cause
        )


class StorageWriteError(StorageException):
    """Exception raised when storage write fails."""
    
    def __init__(
        self,
        message: str,
        storage_type: str,
        key: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        cause: Optional[Exception] = None,
    ):
        details = details or {}
        if key:
            details["key"] = key
        super().__init__(
            message, storage_type, "write", "STORAGE_WRITE_ERROR", details, cause
        )


class ValidationException(OSINTException):
    """Exception raised during data validation."""
    
    def __init__(
        self,
        message: str,
        field: Optional[str] = None,
        value: Optional[Any] = None,
        expected: Optional[str] = None,
        code: str = "VALIDATION_ERROR",
        details: Optional[Dict[str, Any]] = None,
        cause: Optional[Exception] = None,
    ):
        """
        Initialize validation exception.
        
        Args:
            message: Human-readable error message
            field: Name of the field that failed validation
            value: The invalid value (may be redacted for security)
            expected: Description of expected value/format
            code: Machine-readable error code
            details: Additional context data
            cause: Original exception that caused this error
        """
        super().__init__(message, code, details, cause)
        self.field = field
        self.value = value
        self.expected = expected
    
    def to_dict(self) -> Dict[str, Any]:
        result = super().to_dict()
        if self.field:
            result["field"] = self.field
        if self.expected:
            result["expected"] = self.expected
        return result


class SchemaValidationError(ValidationException):
    """Exception raised when schema validation fails."""
    
    def __init__(
        self,
        message: str,
        schema_name: str,
        errors: Optional[Dict[str, Any]] = None,
        details: Optional[Dict[str, Any]] = None,
        cause: Optional[Exception] = None,
    ):
        details = details or {}
        details["schema_name"] = schema_name
        if errors:
            details["validation_errors"] = errors
        super().__init__(
            message,
            field=None,
            value=None,
            expected=None,
            code="SCHEMA_VALIDATION_ERROR",
            details=details,
            cause=cause,
        )


class RateLimitError(OSINTException):
    """Exception raised when rate limit is exceeded."""
    
    def __init__(
        self,
        message: str,
        source: str,
        limit: int,
        window_seconds: int,
        retry_after: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
        cause: Optional[Exception] = None,
    ):
        """
        Initialize rate limit exception.
        
        Args:
            message: Human-readable error message
            source: The source/API that rate limited
            limit: The rate limit that was exceeded
            window_seconds: The time window for the limit
            retry_after: Seconds to wait before retrying
            details: Additional context data
            cause: Original exception that caused this error
        """
        details = details or {}
        details.update({
            "source": source,
            "limit": limit,
            "window_seconds": window_seconds,
        })
        if retry_after:
            details["retry_after"] = retry_after
        super().__init__(message, "RATE_LIMIT_ERROR", details, cause)
        self.source = source
        self.limit = limit
        self.window_seconds = window_seconds
        self.retry_after = retry_after


class SourceDisabledError(OSINTException):
    """Exception raised when a disabled source is accessed."""
    
    def __init__(
        self,
        message: str,
        source: str,
        reason: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        cause: Optional[Exception] = None,
    ):
        """
        Initialize source disabled exception.
        
        Args:
            message: Human-readable error message
            source: The disabled source
            reason: Reason for disabling
            details: Additional context data
            cause: Original exception that caused this error
        """
        details = details or {}
        details["source"] = source
        if reason:
            details["reason"] = reason
        super().__init__(message, "SOURCE_DISABLED_ERROR", details, cause)
        self.source = source
        self.reason = reason
