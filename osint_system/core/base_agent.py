"""
Base Agent Module
=================
Abstract base class for all OSINT agents.

This module provides:
- Agent lifecycle management (start, stop, health checks)
- Message handling infrastructure
- Rate limiting and policy enforcement
- Standardized error handling
"""

import asyncio
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from enum import Enum
from typing import Optional, Dict, Any, List, Callable, Awaitable
from dataclasses import dataclass, field

from .logging_config import get_agent_logger, AgentLogger
from .config import get_config, Config
from .exceptions import (
    AgentException,
    AgentStartupError,
    AgentShutdownError,
    AgentTimeoutError,
    RateLimitError,
)
from ..messaging import (
    Publisher,
    Consumer,
    Message,
    MessageType,
    get_message_broker,
)


class AgentStatus(Enum):
    """Agent lifecycle status."""
    
    INITIALIZING = "initializing"
    STARTING = "starting"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERROR = "error"


@dataclass
class AgentHealth:
    """Agent health check result."""
    
    agent_id: str
    agent_type: str
    status: AgentStatus
    last_heartbeat: datetime
    uptime_seconds: float
    tasks_processed: int
    tasks_failed: int
    current_tasks: int
    errors: List[Dict[str, Any]] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert health check to dictionary."""
        return {
            "agent": self.agent_type,
            "agent_id": self.agent_id,
            "status": self.status.value,
            "last_heartbeat": self.last_heartbeat.isoformat(),
            "uptime_seconds": self.uptime_seconds,
            "tasks_processed": self.tasks_processed,
            "tasks_failed": self.tasks_failed,
            "current_tasks": self.current_tasks,
            "errors": self.errors,
            "metrics": self.metrics,
        }


@dataclass
class RateLimiter:
    """
    Token bucket rate limiter.
    
    Provides rate limiting with configurable limits and windows.
    """
    
    limit: int
    window_seconds: int
    tokens: float = field(init=False)
    last_update: float = field(init=False)
    
    def __post_init__(self) -> None:
        """Initialize token bucket."""
        self.tokens = float(self.limit)
        self.last_update = asyncio.get_event_loop().time()
    
    async def acquire(self, tokens: int = 1) -> bool:
        """
        Try to acquire tokens from the bucket.
        
        Args:
            tokens: Number of tokens to acquire
            
        Returns:
            bool: True if tokens were acquired
        """
        now = asyncio.get_event_loop().time()
        elapsed = now - self.last_update
        
        # Refill tokens based on elapsed time
        self.tokens = min(
            self.limit,
            self.tokens + (elapsed * self.limit / self.window_seconds)
        )
        self.last_update = now
        
        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False
    
    def time_until_available(self, tokens: int = 1) -> float:
        """
        Calculate time until tokens are available.
        
        Args:
            tokens: Number of tokens needed
            
        Returns:
            float: Seconds until tokens are available
        """
        if self.tokens >= tokens:
            return 0.0
        
        needed = tokens - self.tokens
        return (needed * self.window_seconds) / self.limit


class BaseAgent(ABC):
    """
    Abstract base class for all OSINT agents.
    
    Provides common functionality for:
    - Lifecycle management (start, stop, pause, resume)
    - Health monitoring and heartbeats
    - Rate limiting
    - Message handling
    - Error tracking
    
    Subclasses must implement:
    - _on_start(): Agent-specific startup logic
    - _on_stop(): Agent-specific shutdown logic
    - _process_task(): Main task processing logic
    """
    
    def __init__(
        self,
        agent_type: str,
        agent_id: Optional[str] = None,
        config: Optional[Config] = None,
    ):
        """
        Initialize the base agent.
        
        Args:
            agent_type: Type identifier for this agent class
            agent_id: Unique agent instance ID (auto-generated if not provided)
            config: Configuration object (uses global config if not provided)
        """
        self.agent_type = agent_type
        self.agent_id = agent_id or f"{agent_type}_{uuid.uuid4().hex[:8]}"
        self.config = config or get_config()
        
        self.logger: AgentLogger = get_agent_logger(
            agent_id=self.agent_id,
            agent_type=self.agent_type,
        )
        
        # Messaging
        self.broker = get_message_broker(config=self.config.messaging)
        
        self.publisher = Publisher(
            broker=self.broker,
            agent_id=self.agent_id,
            default_exchange=self.config.messaging.exchange_name,
        )
        
        self.consumer = Consumer(
            agent_id=self.agent_id,
            broker=self.broker,
            queue_name=self.agent_id,  # By default, agent listens on its own queue
            prefetch_count=self.config.agents.max_concurrent_tasks,
        )
        
        # State management
        self._status = AgentStatus.INITIALIZING
        self._start_time: Optional[datetime] = None
        self._last_heartbeat: Optional[datetime] = None
        
        # Task tracking
        self._tasks_processed = 0
        self._tasks_failed = 0
        self._current_tasks = 0
        self._task_semaphore: Optional[asyncio.Semaphore] = None
        
        # Error tracking
        self._recent_errors: List[Dict[str, Any]] = []
        self._max_error_history = 100
        
        # Rate limiting
        self._rate_limiters: Dict[str, RateLimiter] = {}
        self._default_rate_limiter = RateLimiter(
            limit=self.config.agents.default_rate_limit,
            window_seconds=self.config.agents.rate_limit_window,
        )
        
        # Background tasks
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()
        
        # Note: Message handlers are now managed by self.consumer
        
        self.logger.info(f"Agent initialized: {self.agent_id}")
    
    @property
    def status(self) -> AgentStatus:
        """Get current agent status."""
        return self._status
    
    @property
    def is_running(self) -> bool:
        """Check if agent is running."""
        return self._status == AgentStatus.RUNNING
    
    @property
    def uptime(self) -> float:
        """Get agent uptime in seconds."""
        if self._start_time is None:
            return 0.0
        return (datetime.now(timezone.utc) - self._start_time).total_seconds()
    
    async def start(self) -> None:
        """
        Start the agent.
        
        Initializes resources, starts background tasks, and calls
        agent-specific startup logic.
        
        Raises:
            AgentStartupError: If startup fails
        """
        if self._status not in (AgentStatus.INITIALIZING, AgentStatus.STOPPED):
            raise AgentStartupError(
                f"Cannot start agent in {self._status.value} state",
                agent_id=self.agent_id,
                agent_type=self.agent_type,
            )
        
        self.logger.info("Starting agent...")
        self._status = AgentStatus.STARTING
        
        try:
            # Initialize task semaphore
            self._task_semaphore = asyncio.Semaphore(
                self.config.agents.max_concurrent_tasks
            )
            
            # Call agent-specific startup
            await self._on_start()
            
            # Start consumer
            await self.consumer.start()
            
            # Start heartbeat task
            self._shutdown_event.clear()
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            
            # Update state
            self._start_time = datetime.now(timezone.utc)
            self._last_heartbeat = self._start_time
            self._status = AgentStatus.RUNNING
            
            self.logger.info("Agent started successfully")
            
        except Exception as e:
            self._status = AgentStatus.ERROR
            self._record_error(e, "startup")
            raise AgentStartupError(
                f"Failed to start agent: {str(e)}",
                agent_id=self.agent_id,
                agent_type=self.agent_type,
                cause=e,
            )
    
    async def stop(self, timeout: float = 30.0) -> None:
        """
        Stop the agent gracefully.
        
        Allows in-progress tasks to complete within timeout,
        then calls agent-specific shutdown logic.
        
        Args:
            timeout: Maximum seconds to wait for graceful shutdown
            
        Raises:
            AgentShutdownError: If shutdown fails
        """
        if self._status == AgentStatus.STOPPED:
            return
        
        self.logger.info("Stopping agent...")
        self._status = AgentStatus.STOPPING
        
        try:
            # Signal shutdown
            self._shutdown_event.set()
            
            # Stop heartbeat task
            if self._heartbeat_task:
                self._heartbeat_task.cancel()
                try:
                    await asyncio.wait_for(
                        self._heartbeat_task,
                        timeout=5.0
                    )
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
            
            # Wait for current tasks to complete
            if self._current_tasks > 0:
                self.logger.info(
                    f"Waiting for {self._current_tasks} tasks to complete..."
                )
                await self._wait_for_tasks(timeout)
            
            # Stop consumer
            await self.consumer.stop()
            
            # Call agent-specific shutdown
            await self._on_stop()
            
            self._status = AgentStatus.STOPPED
            self.logger.info("Agent stopped successfully")
            
        except Exception as e:
            self._status = AgentStatus.ERROR
            self._record_error(e, "shutdown")
            raise AgentShutdownError(
                f"Failed to stop agent cleanly: {str(e)}",
                agent_id=self.agent_id,
                agent_type=self.agent_type,
                cause=e,
            )
    
    async def pause(self) -> None:
        """Pause the agent (stop accepting new tasks)."""
        if self._status == AgentStatus.RUNNING:
            self._status = AgentStatus.PAUSED
            self.logger.info("Agent paused")
    
    async def resume(self) -> None:
        """Resume the agent."""
        if self._status == AgentStatus.PAUSED:
            self._status = AgentStatus.RUNNING
            self.logger.info("Agent resumed")
    
    def get_health(self) -> AgentHealth:
        """
        Get agent health status.
        
        Returns:
            AgentHealth: Current health information
        """
        return AgentHealth(
            agent_id=self.agent_id,
            agent_type=self.agent_type,
            status=self._status,
            last_heartbeat=self._last_heartbeat or datetime.now(timezone.utc),
            uptime_seconds=self.uptime,
            tasks_processed=self._tasks_processed,
            tasks_failed=self._tasks_failed,
            current_tasks=self._current_tasks,
            errors=self._recent_errors[-10:],  # Last 10 errors
            metrics=self._get_metrics(),
        )
    
    async def execute_task(
        self,
        task_data: Dict[str, Any],
        task_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Execute a task with rate limiting and error handling.
        
        Args:
            task_data: Task input data
            task_id: Optional task identifier
            
        Returns:
            Dict[str, Any]: Task result
            
        Raises:
            AgentException: If task execution fails
            RateLimitError: If rate limit is exceeded
        """
        task_id = task_id or uuid.uuid4().hex
        
        # Check if agent is accepting tasks
        if self._status != AgentStatus.RUNNING:
            raise AgentException(
                f"Agent not accepting tasks (status: {self._status.value})",
                agent_id=self.agent_id,
                agent_type=self.agent_type,
            )
        
        # Apply rate limiting
        if not await self._default_rate_limiter.acquire():
            wait_time = self._default_rate_limiter.time_until_available()
            raise RateLimitError(
                "Rate limit exceeded",
                source=self.agent_type,
                limit=self._default_rate_limiter.limit,
                window_seconds=self._default_rate_limiter.window_seconds,
                retry_after=int(wait_time) + 1,
            )
        
        # Acquire task slot
        async with self._task_semaphore:
            self._current_tasks += 1
            
            try:
                self.logger.debug(f"Executing task {task_id}")
                
                # Execute with timeout
                result = await asyncio.wait_for(
                    self._process_task(task_data, task_id),
                    timeout=self.config.agents.task_timeout,
                )
                
                self._tasks_processed += 1
                self.logger.debug(f"Task {task_id} completed successfully")
                
                return result
                
            except asyncio.TimeoutError:
                self._tasks_failed += 1
                self._record_error(
                    AgentTimeoutError(
                        f"Task {task_id} timed out",
                        agent_id=self.agent_id,
                        agent_type=self.agent_type,
                        timeout_seconds=self.config.agents.task_timeout,
                    ),
                    "task_timeout",
                )
                raise
                
            except Exception as e:
                self._tasks_failed += 1
                self._record_error(e, "task_execution")
                raise AgentException(
                    f"Task execution failed: {str(e)}",
                    agent_id=self.agent_id,
                    agent_type=self.agent_type,
                    details={"task_id": task_id},
                    cause=e,
                )
                
            finally:
                self._current_tasks -= 1
    
    async def publish(
        self,
        message_type: MessageType,
        payload: Dict[str, Any],
        routing_key: str,
        **kwargs,
    ) -> Message:
        """
        Publish a message.
        
        Args:
            message_type: Type of message
            payload: Message data
            routing_key: Destination routing key
            **kwargs: Additional arguments for Publisher.publish
            
        Returns:
            Message: The published message
        """
        return await self.publisher.publish(
            message_type=message_type,
            payload=payload,
            routing_key=routing_key,
            **kwargs,
        )

    def register_message_handler(
        self,
        message_type: MessageType,
        handler: Callable[[Message], Awaitable[None]],
    ) -> None:
        """
        Register a handler for a specific message type.
        
        Args:
            message_type: Type of message to handle (MessageType enum)
            handler: Async function to handle the message
        """
        self.consumer.register_handler(message_type, handler)
    
    def add_rate_limiter(
        self,
        name: str,
        limit: int,
        window_seconds: int,
    ) -> None:
        """
        Add a named rate limiter for specific sources.
        
        Args:
            name: Limiter name (e.g., source name)
            limit: Requests per window
            window_seconds: Window size in seconds
        """
        self._rate_limiters[name] = RateLimiter(
            limit=limit,
            window_seconds=window_seconds,
        )
    
    async def check_rate_limit(self, name: str, tokens: int = 1) -> bool:
        """
        Check if a request is within rate limits.
        
        Args:
            name: Rate limiter name
            tokens: Number of tokens to consume
            
        Returns:
            bool: True if within limits
        """
        limiter = self._rate_limiters.get(name, self._default_rate_limiter)
        return await limiter.acquire(tokens)
    
    @abstractmethod
    async def _on_start(self) -> None:
        """
        Agent-specific startup logic.
        
        Override this method to initialize agent-specific resources
        like database connections, API clients, etc.
        """
        pass
    
    @abstractmethod
    async def _on_stop(self) -> None:
        """
        Agent-specific shutdown logic.
        
        Override this method to cleanup agent-specific resources.
        """
        pass
    
    @abstractmethod
    async def _process_task(
        self,
        task_data: Dict[str, Any],
        task_id: str,
    ) -> Dict[str, Any]:
        """
        Process a single task.
        
        Override this method to implement the agent's main functionality.
        
        Args:
            task_data: Input data for the task
            task_id: Unique task identifier
            
        Returns:
            Dict[str, Any]: Task result
        """
        pass
    
    def _get_metrics(self) -> Dict[str, Any]:
        """
        Get agent-specific metrics.
        
        Override to add custom metrics.
        
        Returns:
            Dict[str, Any]: Metrics dictionary
        """
        return {
            "success_rate": (
                self._tasks_processed / max(1, self._tasks_processed + self._tasks_failed)
            ),
        }
    
    async def _heartbeat_loop(self) -> None:
        """Background heartbeat task."""
        interval = self.config.agents.heartbeat_interval
        
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(interval)
                self._last_heartbeat = datetime.now(timezone.utc)
                self.logger.debug("Heartbeat")
            except asyncio.CancelledError:
                break
    
    async def _wait_for_tasks(self, timeout: float) -> None:
        """Wait for current tasks to complete."""
        start = asyncio.get_event_loop().time()
        
        while self._current_tasks > 0:
            elapsed = asyncio.get_event_loop().time() - start
            if elapsed >= timeout:
                self.logger.warning(
                    f"Timeout waiting for tasks, {self._current_tasks} still running"
                )
                break
            await asyncio.sleep(0.5)
    
    def _record_error(
        self,
        error: Exception,
        context: str,
    ) -> None:
        """Record an error for monitoring."""
        error_entry = {
            "type": type(error).__name__,
            "message": str(error),
            "context": context,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        
        self._recent_errors.append(error_entry)
        
        # Trim error history
        if len(self._recent_errors) > self._max_error_history:
            self._recent_errors = self._recent_errors[-self._max_error_history:]
        
        self.logger.error(f"Error in {context}: {error}", exc_info=error)
