"""
Base Collector Agent
====================
Abstract base class for all data collection agents.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
import asyncio

from ..core.base_agent import BaseAgent
from ..messaging import Message, MessageType

class BaseCollectorAgent(BaseAgent, ABC):
    """
    Abstract Base Collector Agent.
    
    Responsibilities:
    - Listen for specific collection requests
    - Perform data collection
    - Standardize output format
    - Emit results as RAW_DATA messages
    """
    
    def __init__(self, agent_type: str, agent_id: Optional[str] = None):
        super().__init__(agent_type=agent_type, agent_id=agent_id)
    
    async def _on_start(self) -> None:
        """Initialize collector handlers."""
        # Listen for collection requests targeting this agent type or specific ID
        self.register_message_handler(
            MessageType.COLLECT_REQUEST,
            self._handle_collect_request
        )
        self.logger.info(f"{self.agent_type} collector started")

    async def _on_stop(self) -> None:
        """Cleanup collector resources."""
        self.logger.info(f"{self.agent_type} collector stopped")

    async def _process_task(
        self,
        task_data: Dict[str, Any],
        task_id: str,
    ) -> Dict[str, Any]:
        """
        Process internal tasks.
        
        Collectors are primarily message-driven, but this can be used 
        for scheduled collection jobs.
        """
        return {"status": "processed", "task_id": task_id}

    async def _handle_collect_request(self, message: Message) -> None:
        """
        Handle incoming collection request.
        
        This method:
        1. Validates the request (is it for me?)
        2. Acknowledges receipt
        3. Executes collection logic
        4. Publishes results or errors
        """
        # simplified check: if target_agent is specified and not us, ignore
        if message.metadata.target_agent and message.metadata.target_agent != self.agent_id:
            return

        # Also check if the request payload specifies a type we handle
        # For simplicity, assuming the routing key or topic handled dispatch
        
        self.logger.info(f"Received collection request: {message.metadata.message_id}")
        
        # Acknowledge processing
        await self.publisher.publish_response(
            request=message,
            response_type=MessageType.STATUS,
            payload={"status": "processing", "request_id": message.metadata.message_id},
        )
        
        try:
            # Execute actual collection logic
            results = await self.collect(message.payload)
            
            # Publish results
            for result in results:
                await self.publisher.publish_response(
                    request=message,
                    response_type=MessageType.RAW_DATA,
                    payload={
                        "source": self.agent_type,
                        "data": result,
                        "original_request_id": message.metadata.message_id
                    },
                )
                
            # Signal completion
            await self.publisher.publish_response(
                request=message,
                response_type=MessageType.STATUS,
                payload={
                    "status": "completed", 
                    "request_id": message.metadata.message_id,
                    "count": len(results)
                },
            )
            
        except Exception as e:
            self.logger.error(f"Collection failed: {e}", exc_info=True)
            await self.publisher.publish_response(
                request=message,
                response_type=MessageType.STATUS,
                payload={
                    "status": "error", 
                    "error": str(e),
                    "request_id": message.metadata.message_id
                },
            )

    @abstractmethod
    async def collect(self, target_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Perform the data collection.
        
        Args:
            target_data: The payload from the collection request.
            
        Returns:
            List[Dict[str, Any]]: A list of collected data items.
        """
        pass
