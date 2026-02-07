"""
Message Broker Module
=====================
Abstract broker interface with RabbitMQ and Kafka implementations.

This module provides:
- Abstract broker interface
- RabbitMQ implementation
- Kafka implementation
- Automatic broker selection
"""

import asyncio
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, Callable, Awaitable, List
from dataclasses import dataclass

from ..core.config import get_config, MessagingConfig
from ..core.logging_config import get_logger
from ..core.exceptions import MessageException, MessageDeliveryError

from .message import Message

logger = get_logger(__name__)


@dataclass
class QueueConfig:
    """Configuration for a message queue."""
    
    name: str
    durable: bool = True
    exclusive: bool = False
    auto_delete: bool = False
    max_priority: int = 10
    dead_letter_exchange: Optional[str] = None
    dead_letter_routing_key: Optional[str] = None
    message_ttl: Optional[int] = None  # milliseconds


@dataclass
class ExchangeConfig:
    """Configuration for a message exchange."""
    
    name: str
    exchange_type: str = "topic"  # direct, topic, fanout, headers
    durable: bool = True
    auto_delete: bool = False


class MessageBroker(ABC):
    """
    Abstract message broker interface.
    
    Provides a unified API for different message broker implementations.
    """
    
    def __init__(self, config: MessagingConfig):
        """
        Initialize the broker.
        
        Args:
            config: Messaging configuration
        """
        self.config = config
        self._connected = False
        self._subscriptions: Dict[str, Callable[[Message], Awaitable[None]]] = {}
    
    @property
    def is_connected(self) -> bool:
        """Check if broker is connected."""
        return self._connected
    
    @abstractmethod
    async def connect(self) -> None:
        """
        Establish connection to the broker.
        
        Raises:
            MessageException: If connection fails
        """
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """
        Close connection to the broker.
        """
        pass
    
    @abstractmethod
    async def create_queue(self, config: QueueConfig) -> None:
        """
        Create a queue.
        
        Args:
            config: Queue configuration
        """
        pass
    
    @abstractmethod
    async def create_exchange(self, config: ExchangeConfig) -> None:
        """
        Create an exchange.
        
        Args:
            config: Exchange configuration
        """
        pass
    
    @abstractmethod
    async def bind_queue(
        self,
        queue_name: str,
        exchange_name: str,
        routing_key: str,
    ) -> None:
        """
        Bind a queue to an exchange.
        
        Args:
            queue_name: Queue to bind
            exchange_name: Exchange to bind to
            routing_key: Routing pattern
        """
        pass
    
    @abstractmethod
    async def publish(
        self,
        message: Message,
        routing_key: str,
        exchange: Optional[str] = None,
    ) -> None:
        """
        Publish a message.
        
        Args:
            message: Message to publish
            routing_key: Routing key
            exchange: Exchange name (uses default if not specified)
        """
        pass
    
    @abstractmethod
    async def subscribe(
        self,
        queue_name: str,
        handler: Callable[[Message], Awaitable[None]],
        prefetch_count: int = 1,
    ) -> None:
        """
        Subscribe to a queue.
        
        Args:
            queue_name: Queue to subscribe to
            handler: Async function to handle messages
            prefetch_count: Number of messages to prefetch
        """
        pass
    
    @abstractmethod
    async def unsubscribe(self, queue_name: str) -> None:
        """
        Unsubscribe from a queue.
        
        Args:
            queue_name: Queue to unsubscribe from
        """
        pass
    
    @abstractmethod
    async def ack(self, message: Message) -> None:
        """
        Acknowledge a message.
        
        Args:
            message: Message to acknowledge
        """
        pass
    
    @abstractmethod
    async def nack(
        self,
        message: Message,
        requeue: bool = True,
    ) -> None:
        """
        Negatively acknowledge a message.
        
        Args:
            message: Message to reject
            requeue: Whether to requeue the message
        """
        pass


class RabbitMQBroker(MessageBroker):
    """
    RabbitMQ message broker implementation.
    
    Uses aio-pika for async RabbitMQ communication.
    """
    
    def __init__(self, config: MessagingConfig):
        """
        Initialize RabbitMQ broker.
        
        Args:
            config: Messaging configuration
        """
        super().__init__(config)
        self._connection = None
        self._channel = None
        self._exchanges: Dict[str, Any] = {}
        self._queues: Dict[str, Any] = {}
        self._consumers: Dict[str, Any] = {}
        self._delivery_tags: Dict[str, int] = {}
    
    async def connect(self) -> None:
        """Establish connection to RabbitMQ."""
        try:
            import aio_pika
            
            url = (
                f"amqp://{self.config.username}:{self.config.password}@"
                f"{self.config.host}:{self.config.port}{self.config.virtual_host}"
            )
            
            self._connection = await aio_pika.connect_robust(
                url,
                timeout=self.config.connection_timeout,
            )
            
            self._channel = await self._connection.channel()
            await self._channel.set_qos(prefetch_count=1)
            
            # Create default exchange
            await self.create_exchange(ExchangeConfig(
                name=self.config.exchange_name,
                exchange_type="topic",
            ))
            
            self._connected = True
            logger.info("Connected to RabbitMQ")
            
        except ImportError:
            raise MessageException(
                "aio-pika is required for RabbitMQ support",
                code="MISSING_DEPENDENCY",
            )
        except Exception as e:
            raise MessageException(
                f"Failed to connect to RabbitMQ: {str(e)}",
                code="CONNECTION_ERROR",
                cause=e,
            )
    
    async def disconnect(self) -> None:
        """Close RabbitMQ connection."""
        try:
            # Cancel all consumers
            for consumer_tag in list(self._consumers.keys()):
                await self.unsubscribe(consumer_tag)
            
            if self._channel and not self._channel.is_closed:
                await self._channel.close()
            
            if self._connection and not self._connection.is_closed:
                await self._connection.close()
            
            self._connected = False
            logger.info("Disconnected from RabbitMQ")
            
        except Exception as e:
            logger.error(f"Error disconnecting from RabbitMQ: {e}")
    
    async def create_queue(self, config: QueueConfig) -> None:
        """Create a RabbitMQ queue."""
        import aio_pika
        
        arguments = {}
        
        if config.max_priority > 0:
            arguments["x-max-priority"] = config.max_priority
        
        if config.dead_letter_exchange:
            arguments["x-dead-letter-exchange"] = config.dead_letter_exchange
            if config.dead_letter_routing_key:
                arguments["x-dead-letter-routing-key"] = config.dead_letter_routing_key
        
        if config.message_ttl:
            arguments["x-message-ttl"] = config.message_ttl
        
        queue = await self._channel.declare_queue(
            config.name,
            durable=config.durable,
            exclusive=config.exclusive,
            auto_delete=config.auto_delete,
            arguments=arguments if arguments else None,
        )
        
        self._queues[config.name] = queue
        logger.debug(f"Created queue: {config.name}")
    
    async def create_exchange(self, config: ExchangeConfig) -> None:
        """Create a RabbitMQ exchange."""
        import aio_pika
        
        exchange_type = getattr(aio_pika.ExchangeType, config.exchange_type.upper())
        
        exchange = await self._channel.declare_exchange(
            config.name,
            exchange_type,
            durable=config.durable,
            auto_delete=config.auto_delete,
        )
        
        self._exchanges[config.name] = exchange
        logger.debug(f"Created exchange: {config.name}")
    
    async def bind_queue(
        self,
        queue_name: str,
        exchange_name: str,
        routing_key: str,
    ) -> None:
        """Bind queue to exchange."""
        queue = self._queues.get(queue_name)
        exchange = self._exchanges.get(exchange_name)
        
        if not queue or not exchange:
            raise MessageException(
                f"Queue or exchange not found: {queue_name}, {exchange_name}"
            )
        
        await queue.bind(exchange, routing_key)
        logger.debug(f"Bound {queue_name} to {exchange_name} with key {routing_key}")
    
    async def publish(
        self,
        message: Message,
        routing_key: str,
        exchange: Optional[str] = None,
    ) -> None:
        """Publish message to RabbitMQ."""
        import aio_pika
        
        exchange_name = exchange or self.config.exchange_name
        exchange_obj = self._exchanges.get(exchange_name)
        
        if not exchange_obj:
            raise MessageDeliveryError(
                f"Exchange not found: {exchange_name}",
                message_id=message.metadata.message_id,
            )
        
        try:
            amqp_message = aio_pika.Message(
                body=message.to_bytes(),
                message_id=message.metadata.message_id,
                correlation_id=message.metadata.correlation_id,
                priority=message.metadata.priority,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                content_type="application/json",
            )
            
            await exchange_obj.publish(amqp_message, routing_key)
            logger.debug(f"Published message {message.metadata.message_id} to {routing_key}")
            
        except Exception as e:
            raise MessageDeliveryError(
                f"Failed to publish message: {str(e)}",
                message_id=message.metadata.message_id,
                queue=routing_key,
                cause=e,
            )
    
    async def subscribe(
        self,
        queue_name: str,
        handler: Callable[[Message], Awaitable[None]],
        prefetch_count: int = 1,
    ) -> None:
        """Subscribe to RabbitMQ queue."""
        queue = self._queues.get(queue_name)
        
        if not queue:
            raise MessageException(f"Queue not found: {queue_name}")
        
        await self._channel.set_qos(prefetch_count=prefetch_count)
        
        async def message_handler(amqp_message):
            async with amqp_message.process(ignore_processed=True):
                try:
                    message = Message.from_bytes(amqp_message.body)
                    # Store delivery tag for ack/nack
                    self._delivery_tags[message.metadata.message_id] = amqp_message.delivery_tag
                    await handler(message)
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    await amqp_message.nack(requeue=True)
        
        consumer_tag = await queue.consume(message_handler)
        self._consumers[queue_name] = consumer_tag
        self._subscriptions[queue_name] = handler
        
        logger.info(f"Subscribed to queue: {queue_name}")
    
    async def unsubscribe(self, queue_name: str) -> None:
        """Unsubscribe from RabbitMQ queue."""
        consumer_tag = self._consumers.get(queue_name)
        queue = self._queues.get(queue_name)
        
        if consumer_tag and queue:
            await queue.cancel(consumer_tag)
            del self._consumers[queue_name]
            if queue_name in self._subscriptions:
                del self._subscriptions[queue_name]
            
            logger.info(f"Unsubscribed from queue: {queue_name}")
    
    async def ack(self, message: Message) -> None:
        """Acknowledge message."""
        # In aio-pika, ack is handled by the context manager
        pass
    
    async def nack(self, message: Message, requeue: bool = True) -> None:
        """Negatively acknowledge message."""
        # In aio-pika, nack is handled by the context manager
        pass


class KafkaBroker(MessageBroker):
    """
    Kafka message broker implementation.
    
    Uses aiokafka for async Kafka communication.
    """
    
    def __init__(self, config: MessagingConfig):
        """
        Initialize Kafka broker.
        
        Args:
            config: Messaging configuration
        """
        super().__init__(config)
        self._producer = None
        self._consumers: Dict[str, Any] = {}
        self._consumer_tasks: Dict[str, asyncio.Task] = {}
        self._shutdown_events: Dict[str, asyncio.Event] = {}
    
    async def connect(self) -> None:
        """Establish connection to Kafka."""
        try:
            from aiokafka import AIOKafkaProducer
            
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self.config.kafka_bootstrap_servers,
                value_serializer=lambda v: v,
            )
            
            await self._producer.start()
            self._connected = True
            logger.info("Connected to Kafka")
            
        except ImportError:
            raise MessageException(
                "aiokafka is required for Kafka support",
                code="MISSING_DEPENDENCY",
            )
        except Exception as e:
            raise MessageException(
                f"Failed to connect to Kafka: {str(e)}",
                code="CONNECTION_ERROR",
                cause=e,
            )
    
    async def disconnect(self) -> None:
        """Close Kafka connections."""
        try:
            # Stop all consumers
            for topic in list(self._consumers.keys()):
                await self.unsubscribe(topic)
            
            if self._producer:
                await self._producer.stop()
            
            self._connected = False
            logger.info("Disconnected from Kafka")
            
        except Exception as e:
            logger.error(f"Error disconnecting from Kafka: {e}")
    
    async def create_queue(self, config: QueueConfig) -> None:
        """Create Kafka topic (queues are topics in Kafka)."""
        # Kafka topics are auto-created by default
        # For production, use kafka-admin-client to create topics
        logger.debug(f"Kafka topic will be auto-created: {config.name}")
    
    async def create_exchange(self, config: ExchangeConfig) -> None:
        """No-op for Kafka (no exchange concept)."""
        pass
    
    async def bind_queue(
        self,
        queue_name: str,
        exchange_name: str,
        routing_key: str,
    ) -> None:
        """No-op for Kafka (topics are self-contained)."""
        pass
    
    async def publish(
        self,
        message: Message,
        routing_key: str,
        exchange: Optional[str] = None,
    ) -> None:
        """Publish message to Kafka topic."""
        if not self._producer:
            raise MessageDeliveryError(
                "Producer not initialized",
                message_id=message.metadata.message_id,
            )
        
        try:
            await self._producer.send_and_wait(
                routing_key,
                message.to_bytes(),
                key=message.metadata.correlation_id.encode() if message.metadata.correlation_id else None,
            )
            logger.debug(f"Published message {message.metadata.message_id} to {routing_key}")
            
        except Exception as e:
            raise MessageDeliveryError(
                f"Failed to publish message: {str(e)}",
                message_id=message.metadata.message_id,
                queue=routing_key,
                cause=e,
            )
    
    async def subscribe(
        self,
        queue_name: str,
        handler: Callable[[Message], Awaitable[None]],
        prefetch_count: int = 1,
    ) -> None:
        """Subscribe to Kafka topic."""
        from aiokafka import AIOKafkaConsumer
        
        consumer = AIOKafkaConsumer(
            queue_name,
            bootstrap_servers=self.config.kafka_bootstrap_servers,
            group_id=self.config.kafka_group_id,
            enable_auto_commit=True,
        )
        
        await consumer.start()
        self._consumers[queue_name] = consumer
        self._subscriptions[queue_name] = handler
        
        # Create shutdown event
        shutdown_event = asyncio.Event()
        self._shutdown_events[queue_name] = shutdown_event
        
        # Start consumer task
        async def consume():
            try:
                async for msg in consumer:
                    if shutdown_event.is_set():
                        break
                    try:
                        message = Message.from_bytes(msg.value)
                        await handler(message)
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")
            except asyncio.CancelledError:
                pass
        
        self._consumer_tasks[queue_name] = asyncio.create_task(consume())
        logger.info(f"Subscribed to topic: {queue_name}")
    
    async def unsubscribe(self, queue_name: str) -> None:
        """Unsubscribe from Kafka topic."""
        # Signal shutdown
        if queue_name in self._shutdown_events:
            self._shutdown_events[queue_name].set()
        
        # Cancel consumer task
        if queue_name in self._consumer_tasks:
            self._consumer_tasks[queue_name].cancel()
            try:
                await self._consumer_tasks[queue_name]
            except asyncio.CancelledError:
                pass
            del self._consumer_tasks[queue_name]
        
        # Stop consumer
        if queue_name in self._consumers:
            await self._consumers[queue_name].stop()
            del self._consumers[queue_name]
        
        if queue_name in self._subscriptions:
            del self._subscriptions[queue_name]
        
        logger.info(f"Unsubscribed from topic: {queue_name}")
    
    async def ack(self, message: Message) -> None:
        """Kafka uses auto-commit, so ack is a no-op."""
        pass
    
    async def nack(self, message: Message, requeue: bool = True) -> None:
        """Kafka doesn't support nack, message is lost or needs dead letter handling."""
        logger.warning(f"Message nack not supported in Kafka: {message.metadata.message_id}")


class InMemoryBroker(MessageBroker):
    """
    In-memory message broker for testing.
    
    Provides a simple queue implementation without external dependencies.
    """
    
    def __init__(self, config: MessagingConfig):
        """
        Initialize in-memory broker.
        
        Args:
            config: Messaging configuration
        """
        super().__init__(config)
        self._queues: Dict[str, asyncio.Queue] = {}
        self._consumer_tasks: Dict[str, asyncio.Task] = {}
        self._shutdown_events: Dict[str, asyncio.Event] = {}
    
    async def connect(self) -> None:
        """No-op for in-memory broker."""
        self._connected = True
        logger.info("In-memory broker connected")
    
    async def disconnect(self) -> None:
        """Clean up in-memory broker."""
        for queue_name in list(self._consumer_tasks.keys()):
            await self.unsubscribe(queue_name)
        
        self._queues.clear()
        self._connected = False
        logger.info("In-memory broker disconnected")
    
    async def create_queue(self, config: QueueConfig) -> None:
        """Create in-memory queue."""
        if config.name not in self._queues:
            self._queues[config.name] = asyncio.Queue()
            logger.debug(f"Created in-memory queue: {config.name}")
    
    async def create_exchange(self, config: ExchangeConfig) -> None:
        """No-op for in-memory broker."""
        pass
    
    async def bind_queue(
        self,
        queue_name: str,
        exchange_name: str,
        routing_key: str,
    ) -> None:
        """No-op for in-memory broker."""
        pass
    
    async def publish(
        self,
        message: Message,
        routing_key: str,
        exchange: Optional[str] = None,
    ) -> None:
        """Publish to in-memory queue."""
        if routing_key not in self._queues:
            self._queues[routing_key] = asyncio.Queue()
        
        await self._queues[routing_key].put(message)
        logger.debug(f"Published message {message.metadata.message_id} to {routing_key}")
    
    async def subscribe(
        self,
        queue_name: str,
        handler: Callable[[Message], Awaitable[None]],
        prefetch_count: int = 1,
    ) -> None:
        """Subscribe to in-memory queue."""
        if queue_name not in self._queues:
            self._queues[queue_name] = asyncio.Queue()
        
        self._subscriptions[queue_name] = handler
        
        shutdown_event = asyncio.Event()
        self._shutdown_events[queue_name] = shutdown_event
        
        async def consume():
            queue = self._queues[queue_name]
            while not shutdown_event.is_set():
                try:
                    message = await asyncio.wait_for(queue.get(), timeout=0.5)
                    await handler(message)
                except asyncio.TimeoutError:
                    continue
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
        
        self._consumer_tasks[queue_name] = asyncio.create_task(consume())
        logger.info(f"Subscribed to in-memory queue: {queue_name}")
    
    async def unsubscribe(self, queue_name: str) -> None:
        """Unsubscribe from in-memory queue."""
        if queue_name in self._shutdown_events:
            self._shutdown_events[queue_name].set()
        
        if queue_name in self._consumer_tasks:
            self._consumer_tasks[queue_name].cancel()
            try:
                await self._consumer_tasks[queue_name]
            except asyncio.CancelledError:
                pass
            del self._consumer_tasks[queue_name]
        
        if queue_name in self._subscriptions:
            del self._subscriptions[queue_name]
        
        logger.info(f"Unsubscribed from in-memory queue: {queue_name}")
    
    async def ack(self, message: Message) -> None:
        """No-op for in-memory broker."""
        pass
    
    async def nack(self, message: Message, requeue: bool = True) -> None:
        """Requeue message if requested."""
        if requeue:
            routing_key = message.metadata.target_agent or "default"
            await self.publish(message, routing_key)


# Broker factory
_broker_instance: Optional[MessageBroker] = None


def get_message_broker(
    broker_type: Optional[str] = None,
    config: Optional[MessagingConfig] = None,
) -> MessageBroker:
    """
    Get or create a message broker instance.
    
    Args:
        broker_type: Broker type (rabbitmq, kafka, memory)
        config: Messaging configuration
        
    Returns:
        MessageBroker: Broker instance
    """
    global _broker_instance
    
    if _broker_instance is not None:
        return _broker_instance
    
    config = config or get_config().messaging
    broker_type = broker_type or config.broker_type
    
    if broker_type == "rabbitmq":
        _broker_instance = RabbitMQBroker(config)
    elif broker_type == "kafka":
        _broker_instance = KafkaBroker(config)
    elif broker_type == "memory":
        _broker_instance = InMemoryBroker(config)
    else:
        raise ValueError(f"Unknown broker type: {broker_type}")
    
    return _broker_instance
