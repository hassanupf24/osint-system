"""
Manager Agent Module
====================
Orchestrator agent responsible for task distribution and result aggregation.
"""

import asyncio
from typing import Dict, Any, List

from ..core.base_agent import BaseAgent
from ..messaging.message import MessageType, Message
from ..core.config import get_config

class ManagerAgent(BaseAgent):
    """
    Manager Agent (Orchestrator).
    
    Responsibilities:
    - Receive high-level OSINT requests
    - Decompose requests into sub-tasks
    - Distribute tasks to specialized agents
    - Aggregate results
    - Monitor progress
    """
    
    def __init__(self, agent_id: str = "manager_01"):
        super().__init__(agent_type="manager", agent_id=agent_id)
        self.pending_requests: Dict[str, Dict[str, Any]] = {}

    async def _on_start(self) -> None:
        """Initialize handlers."""
        # Register handlers for different request types
        self.register_message_handler(
            MessageType.COLLECT_REQUEST,
            self._handle_collect_request
        )
        self.register_message_handler(
            MessageType.ANALYSIS_REQUEST,
            self._handle_analysis_request
        )
        # Handle results from other agents
        self.register_message_handler(
            MessageType.RAW_DATA,
            self._handle_agent_result
        )
        self.register_message_handler(
            MessageType.ANALYSIS_RESULT,
            self._handle_agent_result
        )
        self.logger.info("Manager agent started and listening for requests")

    async def _on_stop(self) -> None:
        """Cleanup."""
        self.logger.info("Manager agent stopped")

    async def _process_task(
        self,
        task_data: Dict[str, Any],
        task_id: str,
    ) -> Dict[str, Any]:
        """
        Process internal tasks if any.
        Mostly redundant for Manager if driven by messages, 
        but useful for periodic maintenance tasks.
        """
        return {"status": "processed", "task_id": task_id}

    async def _handle_collect_request(self, message: Message) -> None:
        """Handle a collection request."""
        self.logger.info(f"Received collection request: {message.metadata.message_id}")
        
        # Here we would decompose the request and send sub-tasks
        # For now, just acknowledge and maybe forward to a dummy collector
        
        request_id = message.metadata.message_id
        target = message.payload.get("target")
        
        if not target:
            # Send error response
            await self.publisher.publish_response(
                request=message,
                response_type=MessageType.STATUS,
                payload={"status": "error", "message": "Missing target"},
            )
            return

        # Simulate task distribution
        self.pending_requests[request_id] = {
            "status": "pending",
            "original_request": message,
            "subtasks": []
        }
        
        # Determine target agents based on request type
        target_type = message.payload.get("target_type", "").lower()
        subtasks = []
        
        if target_type == "social_media":
            # Dispatch to Social Media Collector
            subtask = await self.publisher.publish(
                message_type=MessageType.COLLECT_REQUEST,
                payload=message.payload,
                routing_key="collectors.social_media",
                target_agent="social_media_collector_01",
                correlation_id=request_id
            )
            subtasks.append(subtask.metadata.message_id)
            
        elif target_type == "domain":
             # Dispatch to Domain Collector
            subtask = await self.publisher.publish(
                message_type=MessageType.COLLECT_REQUEST,
                payload=message.payload,
                routing_key="collectors.domain",
                target_agent="domain_collector_01",
                correlation_id=request_id
            )
            subtasks.append(subtask.metadata.message_id)
            
        else:
            self.logger.warning(f"Unknown target type: {target_type}")
            
        # Update pending request state
        self.pending_requests[request_id]["subtasks"] = subtasks
        
        # Send working acknowledgement
        await self.publisher.publish_response(
            request=message,
            response_type=MessageType.STATUS,
            payload={"status": "processing", "request_id": request_id},
        )

    async def _handle_analysis_request(self, message: Message) -> None:
        """Handle analysis request."""
        self.logger.info(f"Received analysis request: {message.metadata.message_id}")
        # Implementation...

    async def _handle_agent_result(self, message: Message) -> None:
        """Handle results coming back from workers."""
        correlation_id = message.metadata.correlation_id or message.payload.get("original_request_id")
        
        if not correlation_id:
            self.logger.warning("Received result without correlation ID")
            return

        self.logger.info(f"Received result for request {correlation_id} (Type: {message.type.value})")

        # Update Request Status
        if correlation_id in self.pending_requests:
            # Mark subtask as complete if applicable
            # In a real impl, we'd track which specific subtask finished
            pass

        # Orchestration Logic:
        # If we received RAW_DATA, trigger Analysis
        if message.type == MessageType.RAW_DATA:
            data = message.payload.get("data")
            if data:
                self.logger.info(f"Triggering analysis for received data linked to {correlation_id}")
                
                await self.publisher.publish(
                    message_type=MessageType.ANALYSIS_REQUEST,
                    payload={
                        "data": data,
                        "data_id": message.metadata.message_id, # Link back to the raw data message
                        "original_request_id": correlation_id
                    },
                    routing_key="analysis",
                    target_agent="analysis_01",
                    correlation_id=correlation_id
                )
        
        # If we received ANALYSIS_RESULT, maybe notify user or finalize
        elif message.type == MessageType.ANALYSIS_RESULT:
            self.logger.info(f"Analysis complete for {correlation_id}. Risk Score: {message.payload.get('risk_score')}")
            # Could publish a final completion event here
