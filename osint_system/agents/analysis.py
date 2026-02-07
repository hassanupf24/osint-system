"""
Analysis Agent Module
=====================
Agent responsible for analyzing collected data to identify risks and correlations.
"""

from typing import Dict, Any, List
import re

from ..core.base_agent import BaseAgent
from ..messaging.message import MessageType, Message

class AnalysisAgent(BaseAgent):
    """
    Analysis Agent.
    
    Responsibilities:
    - Receive ANALYSIS_REQUEST with raw data
    - Perform risk scoring (keyword-based)
    - Identify correlations (e.g., same username across platforms)
    - Emit ANALYSIS_RESULT
    """
    
    def __init__(self, agent_id: str = "analysis_01"):
        super().__init__(agent_type="analysis", agent_id=agent_id)
        
        # Simple risk keywords for demonstration
        self.risk_keywords = [
            "password", "leak", "exploit", "vunerability", 
            "hack", "attack", "admin", "root", "database"
        ]

    async def _on_start(self) -> None:
        """Initialize analysis handlers."""
        self.register_message_handler(
            MessageType.ANALYSIS_REQUEST,
            self._handle_analysis_request
        )
        self.logger.info("Analysis agent started")

    async def _on_stop(self) -> None:
        self.logger.info("Analysis agent stopped")

    async def _process_task(
        self,
        task_data: Dict[str, Any],
        task_id: str,
    ) -> Dict[str, Any]:
         return {"status": "processed", "task_id": task_id}

    async def _handle_analysis_request(self, message: Message) -> None:
        """
        Handle analysis request.
        Request usually contains 'data' or 'data_id'.
        """
        self.logger.info(f"Received analysis request: {message.metadata.message_id}")
        
        payload = message.payload
        data_to_analyze = payload.get("data")
        
        if not data_to_analyze:
            self.logger.warning("No data provided for analysis")
            return

        try:
            # Perform analysis
            risk_score = self._calculate_risk_score(data_to_analyze)
            findings = self._identify_findings(data_to_analyze)
            
            result = {
                "risk_score": risk_score,
                "findings": findings,
                "analyzed_data_id": payload.get("data_id"),
                "timestamp": message.metadata.timestamp
            }
            
            # Publish result
            await self.publisher.publish_response(
                request=message,
                response_type=MessageType.ANALYSIS_RESULT,
                payload=result,
            )
            
        except Exception as e:
            self.logger.error(f"Analysis failed: {e}", exc_info=True)

    def _calculate_risk_score(self, data: Any) -> int:
        """
        Calculate risk score based on keyword matching.
        Returns integer 0-100.
        """
        if isinstance(data, dict):
            text = str(data.get("content") or data.get("bio") or data)
        elif isinstance(data, str):
            text = data
        else:
            text = str(data)
            
        text = text.lower()
        score = 0
        
        for keyword in self.risk_keywords:
            if keyword in text:
                score += 10
                
        return min(score, 100)

    def _identify_findings(self, data: Any) -> List[str]:
        """Identify specific findings."""
        findings = []
        if isinstance(data, dict):
            text = str(data.get("content") or data.get("bio") or str(data))
        else:
            text = str(data)
            
        text = text.lower()
        
        for keyword in self.risk_keywords:
            if keyword in text:
                findings.append(f"Found risky keyword: {keyword}")
                
        return findings
