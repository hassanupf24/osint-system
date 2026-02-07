"""
Publisher Module
================
High-level message publishing with routing and batching support.
"""

import asyncio
from typing import Optional, List, Dict, Any

from ..core.logging_config import get_logger
from ..core.exceptions import MessageDeliveryError

from .broker import MessageBroker, get_message_broker
from .message import Message, MessageType, MessagePriority

logger = get_logger(__name__)


class Publisher:
    """
    High-level message publisher.
    
    Provides convenient methods for publishing messages with
    automatic routing, batching, and error handling.
    """
    
    def __init__(
        self,
        broker: Optional[MessageBroker] = None,
        default_exchange: Optional[str] = None,
        agent_id: Optional[str] = None,
    ):
        """
        Initialize the publisher.
        
        Args:
            broker: Message broker instance
            default_exchange: Default exchange for publishing
            agent_id: Source agent identifier
        """
        self.broker = broker or get_message_broker()
        self.default_exchange = default_exchange
        self.agent_id = agent_id
        self._pending_batch: List[tuple] = []
        self._batch_lock = asyncio.Lock()
    
    async def publish(
        self,
        message_type: MessageType,
        payload: Dict[str, Any],
        routing_key: str,
        target_agent: Optional[str] = None,
        correlation_id: Optional[str] = None,
        priority: MessagePriority = MessagePriority.NORMAL,
        exchange: Optional[str] = None,
    ) -> Message:
        """
        Publish a message.
        
        Args:
            message_type: Type of message
            payload: Message data
            routing_key: Routing key for the message
            target_agent: Target agent identifier
            correlation_id: Correlation ID for request/response
            priority: Message priority
            exchange: Exchange to publish to
            
        Returns:
            Message: Published message
        """
        message = Message.create(
            message_type=message_type,
            payload=payload,
            source_agent=self.agent_id,
            target_agent=target_agent,
            correlation_id=correlation_id,
            priority=priority,
        )
        
        await self.broker.publish(
            message,
            routing_key,
            exchange or self.default_exchange,
        )
        
        return message
    
    async def publish_message(
        self,
        message: Message,
        routing_key: str,
        exchange: Optional[str] = None,
    ) -> None:
        """
        Publish a pre-constructed message.
        
        Args:
            message: Message to publish
            routing_key: Routing key
            exchange: Exchange to publish to
        """
        # Set source agent if not set
        if not message.metadata.source_agent:
            message.metadata.source_agent = self.agent_id
        
        await self.broker.publish(
            message,
            routing_key,
            exchange or self.default_exchange,
        )
    
    async def publish_response(
        self,
        request: Message,
        response_type: MessageType,
        payload: Dict[str, Any],
        routing_key: Optional[str] = None,
    ) -> Message:
        """
        Publish a response to a request message.
        
        Args:
            request: Original request message
            response_type: Type of response
            payload: Response data
            routing_key: Override routing key (uses reply_to if not specified)
            
        Returns:
            Message: Response message
        """
        response = Message.create_response(
            request=request,
            response_type=response_type,
            payload=payload,
            source_agent=self.agent_id,
        )
        
        # Determine routing key
        key = routing_key or request.metadata.reply_to or request.metadata.source_agent
        if not key:
            raise MessageDeliveryError(
                "No routing key for response",
                message_id=response.metadata.message_id,
            )
        
        await self.broker.publish(response, key, self.default_exchange)
        return response
    
    async def add_to_batch(
        self,
        message_type: MessageType,
        payload: Dict[str, Any],
        routing_key: str,
        **kwargs,
    ) -> None:
        """
        Add a message to the pending batch.
        
        Args:
            message_type: Type of message
            payload: Message data
            routing_key: Routing key
            **kwargs: Additional message parameters
        """
        async with self._batch_lock:
            message = Message.create(
                message_type=message_type,
                payload=payload,
                source_agent=self.agent_id,
                **kwargs,
            )
            self._pending_batch.append((message, routing_key))
    
    async def flush_batch(self) -> int:
        """
        Publish all pending batch messages.
        
        Returns:
            int: Number of messages published
        """
        async with self._batch_lock:
            if not self._pending_batch:
                return 0
            
            count = 0
            errors = []
            
            for message, routing_key in self._pending_batch:
                try:
                    await self.broker.publish(
                        message,
                        routing_key,
                        self.default_exchange,
                    )
                    count += 1
                except Exception as e:
                    errors.append((message.metadata.message_id, str(e)))
            
            self._pending_batch.clear()
            
            if errors:
                logger.warning(f"Batch publish had {len(errors)} errors: {errors}")
            
            return count
    
    async def broadcast(
        self,
        message_type: MessageType,
        payload: Dict[str, Any],
        routing_keys: List[str],
        **kwargs,
    ) -> List[Message]:
        """
        Broadcast a message to multiple routing keys.
        
        Args:
            message_type: Type of message
            payload: Message data
            routing_keys: List of routing keys
            **kwargs: Additional message parameters
            
        Returns:
            List[Message]: Published messages
        """
        messages = []
        
        for routing_key in routing_keys:
            message = await self.publish(
                message_type=message_type,
                payload=payload,
                routing_key=routing_key,
                **kwargs,
            )
            messages.append(message)
        
        return messages
    
    async def publish_alert(
        self,
        alert_type: str,
        severity: str,
        title: str,
        description: str,
        source: str,
        data: Optional[Dict[str, Any]] = None,
        routing_key: str = "alerts",
    ) -> Message:
        """
        Publish an alert message.
        
        Args:
            alert_type: Type of alert
            severity: Alert severity (info, warning, error, critical)
            title: Alert title
            description: Alert description
            source: Alert source
            data: Additional alert data
            routing_key: Routing key for alerts
            
        Returns:
            Message: Alert message
        """
        payload = {
            "alert_type": alert_type,
            "severity": severity,
            "title": title,
            "description": description,
            "source": source,
            "data": data or {},
        }
        
        priority = {
            "info": MessagePriority.LOW,
            "warning": MessagePriority.NORMAL,
            "error": MessagePriority.HIGH,
            "critical": MessagePriority.CRITICAL,
        }.get(severity, MessagePriority.NORMAL)
        
        return await self.publish(
            message_type=MessageType.ALERT,
            payload=payload,
            routing_key=routing_key,
            priority=priority,
        )
