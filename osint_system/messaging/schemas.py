"""
Schema Validation Module
========================
JSON/Dict schema validation for message payloads.
"""

import json
from typing import Dict, Any, Optional

from ..core.logging_config import get_logger
from ..core.exceptions import MessageValidationError

from .message import Message, MessageType

logger = get_logger(__name__)

# Basic JSON schemas for standard message types
# Using jsonschema draft 7 format

_SCHEMAS: Dict[str, Dict[str, Any]] = {
    # Health check schema
    MessageType.HEALTH_CHECK.value: {
        "type": "object",
        "properties": {
            "agent_id": {"type": "string"},
            "timestamp": {"type": "string", "format": "date-time"},
        },
        "required": ["agent_id"]
    },
    
    # Status update schema
    MessageType.STATUS.value: {
        "type": "object",
        "properties": {
            "status": {"type": "string"},
            "state": {"type": "object"},
            "metrics": {"type": "object"},
        },
        "required": ["status"]
    },
    
    # Command schema
    MessageType.COMMAND.value: {
        "type": "object",
        "properties": {
            "command": {"type": "string"},
            "args": {"type": "object"},
            "kwargs": {"type": "object"},
        },
        "required": ["command"]
    },
    
    # Shutdown schema
    MessageType.SHUTDOWN.value: {
        "type": "object",
        "properties": {
            "reason": {"type": "string"},
            "force": {"type": "boolean"},
        },
    },
    
    # Alert schema
    MessageType.ALERT.value: {
        "type": "object",
        "properties": {
            "alert_type": {"type": "string"},
            "severity": {"type": "string", "enum": ["info", "warning", "error", "critical"]},
            "title": {"type": "string"},
            "description": {"type": "string"},
            "source": {"type": "string"},
            "data": {"type": "object"},
        },
        "required": ["alert_type", "severity", "title", "source"]
    },
}

def register_schema(name: str, schema: Dict[str, Any]) -> None:
    """
    Register a JSON schema for validation.
    
    Args:
        name: Schema name (usually message type or specific schema identifier)
        schema: JSON schema dict
    """
    _SCHEMAS[name] = schema
    logger.debug(f"Registered schema: {name}")

def get_schema(name: str) -> Optional[Dict[str, Any]]:
    """
    Get a registered schema by name.
    
    Args:
        name: Schema name
        
    Returns:
        Optional[Dict[str, Any]]: Schema dict or None if not found
    """
    return _SCHEMAS.get(name)

def validate_message(message: Message) -> None:
    """
    Validate a message payload against its schema.
    
    Validation logic:
    1. If message.schema_name is set, use that schema.
    2. Otherwise, look for a schema matching message.type.value.
    3. If no schema found, skip validation (or log warning).
    4. Validate payload against schema using jsonschema.
    
    Args:
        message: Message to validate
        
    Raises:
        MessageValidationError: If validation fails
    """
    import jsonschema
    from jsonschema.exceptions import ValidationError
    
    schema_name = message.schema_name or message.type.value
    schema = get_schema(schema_name)
    
    if not schema:
        # No schema found for this message type/name
        # Currently treating as "pass" but logging debug
        logger.debug(f"No schema found for {schema_name}, skipping validation")
        return

    try:
        jsonschema.validate(instance=message.payload, schema=schema)
    except ValidationError as e:
        logger.error(f"Schema validation failed for {schema_name}: {e.message}")
        raise MessageValidationError(
            f"Message payload failed validation against schema '{schema_name}': {e.message}",
            message_id=message.metadata.message_id,
            schema_name=schema_name,
            validation_error=str(e),
        )
    except Exception as e:
        logger.error(f"Error during validation: {e}")
        raise MessageValidationError(
            f"Unexpected error during validation: {str(e)}",
            message_id=message.metadata.message_id,
            schema_name=schema_name,
        )
