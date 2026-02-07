"""
Consumer Module
===============
High-level message consumer with automatic routing and error handling.
"""

import asyncio
from typing import Optional, Dict, Any, Callable, Awaitable, List

from ..core.logging_config import get_logger
from ..core.exceptions import MessageProcessingError

from .broker import MessageBroker, get_message_broker
from .message import Message, MessageType
from .schemas import validate_message

logger = get_logger(__name__)


class Consumer:
    """
    High-level message consumer.
    
    Provides message consumption with:
    - Automatic message routing based on type
    - Schema validation
    - Error handling and retries
    - Dead letter queue support
    """
    
    def __init__(
        self,
        agent_id: str,
        broker: Optional[MessageBroker] = None,
        queue_name: Optional[str] = None,
        prefetch_count: int = 1,
    ):
        """
        Initialize the consumer.
        
        Args:
            agent_id: Agent identifier (used for logging and queue naming)
            broker: Message broker instance
            queue_name: Queue to consume from (defaults to agent_id)
            prefetch_count: Number of messages to prefetch
        """
        self.agent_id = agent_id
        self.broker = broker or get_message_broker()
        self.queue_name = queue_name or agent_id
        self.prefetch_count = prefetch_count
        
        self._handlers: Dict[MessageType, Callable[[Message], Awaitable[None]]] = {}
        self._default_handler: Optional[Callable[[Message], Awaitable[None]]] = None
        self._running = False
        self._processing_lock = asyncio.Lock()
    
    def register_handler(
        self,
        message_type: MessageType,
        handler: Callable[[Message], Awaitable[None]],
    ) -> None:
        """
        Register a handler for a specific message type.
        
        Args:
            message_type: Type of message to handle
            handler: Async function to handle the message
        """
        self._handlers[message_type] = handler
        logger.debug(f"Registered handler for {message_type.value}")
    
    def set_default_handler(
        self,
        handler: Callable[[Message], Awaitable[None]],
    ) -> None:
        """
        Set a default handler for unknown message types.
        
        Args:
            handler: Async function to handle unknown messages
        """
        self._default_handler = handler
    
    async def start(self) -> None:
        """
        Start consuming messages.
        
        Subscribes to the configured queue and starts processing.
        """
        if self._running:
            return
        
        await self.broker.subscribe(
            queue_name=self.queue_name,
            handler=self._process_message,
            prefetch_count=self.prefetch_count,
        )
        
        self._running = True
        logger.info(f"Consumer started for queue: {self.queue_name}")
    
    async def stop(self) -> None:
        """Stop consuming messages."""
        if not self._running:
            return
        
        await self.broker.unsubscribe(self.queue_name)
        self._running = False
        logger.info(f"Consumer stopped for queue: {self.queue_name}")
    
    async def _process_message(self, message: Message) -> None:
        """
        Internal message processing loop.
        
        Args:
            message: Received message
        """
        try:
            # Check expiration
            if message.is_expired():
                logger.warning(
                    f"Message {message.metadata.message_id} expired, discarding"
                )
                await self.broker.ack(message)
                return
            
            # Request-level validation
            try:
                validate_message(message)
            except Exception as e:
                logger.error(
                    f"Message validation failed: {e}",
                    extra={"message_id": message.metadata.message_id}
                )
                # If validation fails, we can't process it. 
                # Depending on policy, we might dead-letter or ack it.
                # Here we'll nack without requeue (to DLQ if configured)
                await self.broker.nack(message, requeue=False)
                return
            
            # Find and execute handler
            handler = self._handlers.get(message.type) or self._default_handler
            
            if handler:
                start_time = asyncio.get_event_loop().time()
                try:
                    await handler(message)
                    elapsed = asyncio.get_event_loop().time() - start_time
                    
                    # Record processing metadata
                    message.metadata.add_processing_step(
                        agent_id=self.agent_id,
                        action="processed",
                    )
                    
                    await self.broker.ack(message)
                    logger.debug(
                        f"Processed message {message.metadata.message_id} "
                        f"in {elapsed:.3f}s"
                    )
                    
                except Exception as e:
                    elapsed = asyncio.get_event_loop().time() - start_time
                    logger.error(
                        f"Error processing message {message.metadata.message_id}: {e}",
                        exc_info=e
                    )
                    
                    # Retry logic
                    if message.should_retry():
                        message.increment_retry()
                        logger.info(
                            f"Retrying message {message.metadata.message_id} "
                            f"(attempt {message.metadata.retry_count})"
                        )
                        # Requeue (nack with requeue=True is one way, 
                        # but often better to republish to delay or use specific retry queue)
                        # For simple implementation, utilize broker's nack(requeue=True) 
                        # but this spins if no delay.
                        # Ideally, we should use a retry exchange or delay.
                        # For now, let's use nack with requeue which is standard for basic amqp
                        await self.broker.nack(message, requeue=True)
                    else:
                        logger.error(
                            f"Message {message.metadata.message_id} exceeded max retries"
                        )
                        await self.broker.nack(message, requeue=False)  # Send to DLQ
            else:
                logger.warning(
                    f"No handler for message type: {message.type.value}",
                    extra={"message_id": message.metadata.message_id}
                )
                # Ack unknown messages to clear them from queue
                await self.broker.ack(message)
                
        except Exception as e:
            # Catch-all for framework errors
            logger.critical(f"Critical error in message processor: {e}", exc_info=e)
            # Try to nack to avoid message loss, but might fail if connection is gone
            try:
                await self.broker.nack(message, requeue=True)
            except Exception:
                pass
