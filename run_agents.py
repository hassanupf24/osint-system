import asyncio
import logging
import sys
from osint_system.core.logging_config import setup_logging
from osint_system.agents import (
    ManagerAgent,
    SocialMediaCollector,
    DomainCollector,
    StorageAgent,
    AnalysisAgent
)

# Setup logging to stdout
setup_logging()
logger = logging.getLogger("OSINT_System_Runner")

async def main():
    logger.info("Starting OSINT System Agents...")
    
    # Initialize agents
    manager = ManagerAgent(agent_id="manager_01")
    social_collector = SocialMediaCollector(agent_id="social_media_collector_01")
    domain_collector = DomainCollector(agent_id="domain_collector_01")
    storage = StorageAgent(agent_id="storage_01")
    analysis = AnalysisAgent(agent_id="analysis_01")
    
    agents = [manager, social_collector, domain_collector, storage, analysis]
    
    # Start all agents
    start_tasks = [agent.start() for agent in agents]
    await asyncio.gather(*start_tasks)
    
    logger.info("All agents started successfully. Press Ctrl+C to stop.")
    
    try:
        # Keep running until interrupted
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
    except KeyboardInterrupt:
        pass
    finally:
        logger.info("Shutting down agents...")
        stop_tasks = [agent.stop() for agent in agents]
        await asyncio.gather(*stop_tasks)
        logger.info("System shutdown complete.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Handle manual kill gracefully
        sys.exit(0)
