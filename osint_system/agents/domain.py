"""
Domain Collector Agent
======================
Specialized agent for collecting domain and network data using actual libraries.
"""

import asyncio
from typing import Dict, Any, List
import whois
import dns.resolver
import dns.asyncresolver
from datetime import datetime

from .collector import BaseCollectorAgent
from ..messaging.message import MessageType

class DomainCollector(BaseCollectorAgent):
    """
    Domain Collector Agent.
    
    Responsibilities:
    - WHOIS lookups
    - DNS record enumeration
    - Subdomain discovery
    """
    
    def __init__(self, agent_id: str = "domain_collector_01"):
        super().__init__(agent_type="domain_collector", agent_id=agent_id)

    async def collect(self, target_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Perform domain data collection using whois and dnspython.
        
        Args:
            target_data: Contains 'domain'.
        """
        domain = target_data.get("domain")
        
        if not domain:
            raise ValueError("Domain Collector requires 'domain'.")
        
        self.logger.info(f"collecting domain info for {domain}")
        
        results = []
        
        try:
            # 1. WHOIS Lookup (Blocking call run in executor)
            loop = asyncio.get_running_loop()
            whois_info = await loop.run_in_executor(None, whois.whois, domain)
            
            # Serialize dates
            def serialize_dates(obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                if isinstance(obj, list):
                    return [serialize_dates(i) for i in obj]
                return obj

            whois_data = {k: serialize_dates(v) for k, v in whois_info.items()}
            
            # 2. DNS Enumeration (Async)
            dns_records = {}
            resolver = dns.asyncresolver.Resolver()
            record_types = ["A", "AAAA", "MX", "NS", "txt"]
            
            for rtype in record_types:
                try:
                    answers = await resolver.resolve(domain, rtype)
                    dns_records[rtype] = [str(r) for r in answers]
                except Exception:
                    continue
            
            results.append({
                "domain": domain,
                "whois": whois_data,
                "dns": dns_records,
                "timestamp": datetime.utcnow().isoformat()
            })
            
        except Exception as e:
            self.logger.error(f"Error collecting domain data: {e}", exc_info=True)
            # Depending on error handling policy, might want to re-raise or return partial
            # For now, capturing partial failure as structured error result
            results.append({
                "domain": domain,
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat()
            })
            
        return results
