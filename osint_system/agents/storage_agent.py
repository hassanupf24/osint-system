"""
Storage Agent Module
====================
Agent responsible for persisting raw and processed data to the database.
"""

from typing import Dict, Any, List

from ..core.base_agent import BaseAgent
from ..messaging.message import MessageType, Message
from ..storage.database import DatabaseManager
from ..storage.models import RawData, ProcessedData
from sqlalchemy.future import select

class StorageAgent(BaseAgent):
    """
    Storage Agent.
    
    Responsibilities:
    - Listen for RAW_DATA and ANALYSIS_RESULT messages
    - Persist data to the permanent database
    """
    
    def __init__(self, agent_id: str = "storage_01"):
        super().__init__(agent_type="storage", agent_id=agent_id)
        self.db_manager = DatabaseManager()

    async def _on_start(self) -> None:
        """Initialize database connection and handlers."""
        # Connect to DB
        await self.db_manager.connect()
        # Ensure tables exist
        await self.db_manager.create_tables()
        
        # Register handlers
        self.register_message_handler(
            MessageType.RAW_DATA,
            self._handle_raw_data
        )
        self.register_message_handler(
            MessageType.ANALYSIS_RESULT,
            self._handle_analysis_result
        )
        self.logger.info("Storage agent started and connected to database")

    async def _on_stop(self) -> None:
        """Close database connection."""
        await self.db_manager.disconnect()
        self.logger.info("Storage agent stopped")

    async def _process_task(
        self,
        task_data: Dict[str, Any],
        task_id: str,
    ) -> Dict[str, Any]:
        """Process internal tasks."""
        return {"status": "processed", "task_id": task_id}

    async def _handle_raw_data(self, message: Message) -> None:
        """
        Handle incoming raw data.
        Saves data to 'raw_data' table.
        """
        self.logger.info(f"Received raw data from {message.payload.get('source')}")
        
        try:
            payload = message.payload
            
            # Using async context manager for session
            async for session in self.db_manager.get_session():
                data_entry = RawData(
                    source=payload.get("source", "unknown"),
                    data=payload.get("data", {}),
                    target=str(payload.get("original_request_id", "unknown")), # Using request ID as target linkage for now
                    metadata={"message_id": message.metadata.message_id}
                )
                session.add(data_entry)
                # Commit handled by context manager/generator but explicit here for safety is fine too
                # The generator commits automatically on success
            
            self.logger.info("Saved raw data to database")
            
        except Exception as e:
            self.logger.error(f"Failed to save raw data: {e}", exc_info=True)
            # Depending on policy, we might retry or DLQ

    async def _handle_analysis_result(self, message: Message) -> None:
        """
        Handle incoming analysis results.
        Saves to 'processed_data' table.
        """
        self.logger.info(f"Received analysis result for {message.metadata.correlation_id}")
        
        try:
            payload = message.payload
            
            async for session in self.db_manager.get_session():
                result_entry = ProcessedData(
                    analysis_type=payload.get("analysis_type", "unknown"),
                    target_id=message.metadata.correlation_id,
                    result=payload.get("result", {}),
                    score=payload.get("risk_score") # Optional field
                )
                session.add(result_entry)
            
            self.logger.info("Saved analysis result to database")
            
        except Exception as e:
            self.logger.error(f"Failed to save analysis result: {e}", exc_info=True)
