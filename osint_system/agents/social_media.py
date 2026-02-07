"""
Social Media Collector Agent
============================
Specialized agent for collecting social media data using actual scraping libraries.
"""

import asyncio
from typing import Dict, Any, List
from datetime import datetime

from .collector import BaseCollectorAgent
from ..messaging.message import MessageType

# Try importing snscrape, but provide fallback/instructions if missing
try:
    import snscrape.modules.twitter as sntwitter
except ImportError:
    sntwitter = None

class SocialMediaCollector(BaseCollectorAgent):
    """
    Social Media Collector Agent.
    
    Responsibilities:
    - Scrape or fetch data from platforms (Twitter via snscrape, etc.)
    - Handle rate limits specific to social platforms
    - Extract user profiles and posts
    """
    
    def __init__(self, agent_id: str = "social_media_collector_01"):
        super().__init__(agent_type="social_media_collector", agent_id=agent_id)

    async def collect(self, target_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Perform social media collection.
        Currently supports Twitter via snscrape.
        
        Args:
            target_data: Contains 'platform' and 'username' or 'query'.
        """
        platform = target_data.get("platform", "").lower()
        username = target_data.get("username")
        query = target_data.get("query") # Alternative to username
        limit = target_data.get("limit", 10)
        
        self.logger.info(f"Targeting {platform}: {username or query}")
        
        results = []
        
        if platform == "twitter":
            if not sntwitter:
                raise ImportError("snscrape is required for Twitter collection. Please install it.")
                
            search_query = f"from:{username}" if username else query
            if not search_query:
                raise ValueError("Must provide 'username' or 'query' for Twitter collection")
                
            # execute sync scraping in thread pool to avoid blocking async loop
            loop = asyncio.get_running_loop()
            tweets = await loop.run_in_executor(
                None, 
                self._scrape_twitter, 
                search_query, 
                limit
            )
            
            results.extend(tweets)
            
        else:
            self.logger.warning(f"Unsupported platform: {platform}")
            # Fallback or error
            
        return results

    def _scrape_twitter(self, query: str, limit: int) -> List[Dict[str, Any]]:
        """
        Helper method to run snscrape synchronously.
        """
        tweets = []
        try:
            scraper = sntwitter.TwitterSearchScraper(query)
            for i, tweet in enumerate(scraper.get_items()):
                if i >= limit:
                    break
                
                tweets.append({
                    "platform": "twitter",
                    "id": str(tweet.id),
                    "content": tweet.rawContent,
                    "date": tweet.date.isoformat(),
                    "username": tweet.user.username,
                    "url": tweet.url,
                    "likes": tweet.likeCount,
                    "retweets": tweet.retweetCount
                })
        except Exception as e:
            self.logger.error(f"Error scraping twitter: {e}")
            
        return tweets
