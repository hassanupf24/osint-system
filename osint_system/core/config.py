"""
Configuration Management Module
===============================
Centralized configuration management with environment variable support.

This module provides:
- Environment-based configuration
- Validation of required settings
- Default values for optional settings
- Configuration singleton pattern
"""

import os
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
from functools import lru_cache


@dataclass
class MessagingConfig:
    """Message broker configuration."""
    
    broker_type: str = "rabbitmq"  # rabbitmq or kafka
    host: str = "localhost"
    port: int = 5672
    username: str = "guest"
    password: str = "guest"
    virtual_host: str = "/"
    exchange_name: str = "osint_exchange"
    
    # Kafka-specific settings
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_group_id: str = "osint_consumers"
    
    # Connection settings
    connection_timeout: int = 30
    heartbeat_interval: int = 60
    retry_attempts: int = 3
    retry_delay: float = 1.0


@dataclass
class StorageConfig:
    """Storage backend configuration."""
    
    # Primary database (PostgreSQL)
    database_url: str = "postgresql://localhost:5432/osint"
    database_pool_size: int = 10
    database_max_overflow: int = 20
    
    # Raw data storage
    raw_storage_path: str = "./data/raw"
    raw_storage_type: str = "filesystem"  # filesystem, s3, minio
    
    # S3/MinIO settings
    s3_endpoint: Optional[str] = None
    s3_access_key: Optional[str] = None
    s3_secret_key: Optional[str] = None
    s3_bucket: str = "osint-raw-data"
    
    # Graph database (Neo4j)
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "password"
    
    # Time-series (InfluxDB)
    influxdb_url: str = "http://localhost:8086"
    influxdb_token: Optional[str] = None
    influxdb_org: str = "osint"
    influxdb_bucket: str = "metrics"
    
    # Redis cache
    redis_url: str = "redis://localhost:6379/0"
    redis_cache_ttl: int = 3600


@dataclass
class AgentConfig:
    """Agent-specific configuration."""
    
    # Rate limiting
    default_rate_limit: int = 100  # requests per minute
    rate_limit_window: int = 60  # seconds
    
    # Health checks
    heartbeat_interval: int = 30  # seconds
    health_check_timeout: int = 10  # seconds
    
    # Task processing
    max_concurrent_tasks: int = 10
    task_timeout: int = 300  # seconds
    max_retries: int = 3
    
    # Data retention
    raw_data_retention_days: int = 90
    processed_data_retention_days: int = 365
    
    # Kill switches
    enabled_sources: List[str] = field(default_factory=lambda: ["all"])
    disabled_sources: List[str] = field(default_factory=list)


@dataclass
class APIConfig:
    """API server configuration."""
    
    host: str = "0.0.0.0"
    port: int = 8000
    debug: bool = False
    workers: int = 4
    
    # Authentication
    auth_enabled: bool = False
    jwt_secret: Optional[str] = None
    jwt_algorithm: str = "HS256"
    jwt_expiry_hours: int = 24
    
    # CORS
    cors_origins: List[str] = field(default_factory=lambda: ["*"])
    
    # Rate limiting
    api_rate_limit: int = 100
    api_rate_limit_window: int = 60


@dataclass
class LoggingConfig:
    """Logging configuration."""
    
    level: str = "INFO"
    format: str = "json"  # json or text
    output: str = "stdout"  # stdout, file, both
    file_path: str = "./logs/osint.log"
    max_file_size: int = 10_000_000  # 10MB
    backup_count: int = 5
    include_timestamp: bool = True
    include_agent_id: bool = True


@dataclass
class Config:
    """
    Main configuration class aggregating all settings.
    
    Configuration is loaded from environment variables with sensible defaults.
    All sensitive values should be provided via environment variables in production.
    """
    
    # Environment
    environment: str = "development"
    debug: bool = False
    
    # Component configs
    messaging: MessagingConfig = field(default_factory=MessagingConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    agents: AgentConfig = field(default_factory=AgentConfig)
    api: APIConfig = field(default_factory=APIConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    
    @classmethod
    def from_env(cls) -> "Config":
        """
        Create configuration from environment variables.
        
        Environment variables follow the pattern:
        OSINT_{SECTION}_{SETTING} (e.g., OSINT_MESSAGING_HOST)
        
        Returns:
            Config: Populated configuration instance
        """
        messaging = MessagingConfig(
            broker_type=os.getenv("OSINT_MESSAGING_BROKER_TYPE", "rabbitmq"),
            host=os.getenv("OSINT_MESSAGING_HOST", "localhost"),
            port=int(os.getenv("OSINT_MESSAGING_PORT", "5672")),
            username=os.getenv("OSINT_MESSAGING_USERNAME", "guest"),
            password=os.getenv("OSINT_MESSAGING_PASSWORD", "guest"),
            virtual_host=os.getenv("OSINT_MESSAGING_VHOST", "/"),
            exchange_name=os.getenv("OSINT_MESSAGING_EXCHANGE", "osint_exchange"),
            kafka_bootstrap_servers=os.getenv(
                "OSINT_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
            ),
            kafka_group_id=os.getenv("OSINT_KAFKA_GROUP_ID", "osint_consumers"),
        )
        
        storage = StorageConfig(
            database_url=os.getenv(
                "OSINT_DATABASE_URL", "postgresql://localhost:5432/osint"
            ),
            database_pool_size=int(os.getenv("OSINT_DATABASE_POOL_SIZE", "10")),
            raw_storage_path=os.getenv("OSINT_RAW_STORAGE_PATH", "./data/raw"),
            raw_storage_type=os.getenv("OSINT_RAW_STORAGE_TYPE", "filesystem"),
            s3_endpoint=os.getenv("OSINT_S3_ENDPOINT"),
            s3_access_key=os.getenv("OSINT_S3_ACCESS_KEY"),
            s3_secret_key=os.getenv("OSINT_S3_SECRET_KEY"),
            s3_bucket=os.getenv("OSINT_S3_BUCKET", "osint-raw-data"),
            neo4j_uri=os.getenv("OSINT_NEO4J_URI", "bolt://localhost:7687"),
            neo4j_user=os.getenv("OSINT_NEO4J_USER", "neo4j"),
            neo4j_password=os.getenv("OSINT_NEO4J_PASSWORD", "password"),
            influxdb_url=os.getenv("OSINT_INFLUXDB_URL", "http://localhost:8086"),
            influxdb_token=os.getenv("OSINT_INFLUXDB_TOKEN"),
            influxdb_org=os.getenv("OSINT_INFLUXDB_ORG", "osint"),
            influxdb_bucket=os.getenv("OSINT_INFLUXDB_BUCKET", "metrics"),
            redis_url=os.getenv("OSINT_REDIS_URL", "redis://localhost:6379/0"),
        )
        
        agents = AgentConfig(
            default_rate_limit=int(os.getenv("OSINT_AGENT_RATE_LIMIT", "100")),
            heartbeat_interval=int(os.getenv("OSINT_AGENT_HEARTBEAT_INTERVAL", "30")),
            max_concurrent_tasks=int(os.getenv("OSINT_AGENT_MAX_TASKS", "10")),
            task_timeout=int(os.getenv("OSINT_AGENT_TASK_TIMEOUT", "300")),
            raw_data_retention_days=int(
                os.getenv("OSINT_RAW_DATA_RETENTION_DAYS", "90")
            ),
            processed_data_retention_days=int(
                os.getenv("OSINT_PROCESSED_DATA_RETENTION_DAYS", "365")
            ),
        )
        
        api = APIConfig(
            host=os.getenv("OSINT_API_HOST", "0.0.0.0"),
            port=int(os.getenv("OSINT_API_PORT", "8000")),
            debug=os.getenv("OSINT_API_DEBUG", "false").lower() == "true",
            workers=int(os.getenv("OSINT_API_WORKERS", "4")),
            auth_enabled=os.getenv("OSINT_AUTH_ENABLED", "false").lower() == "true",
            jwt_secret=os.getenv("OSINT_JWT_SECRET"),
            cors_origins=os.getenv("OSINT_CORS_ORIGINS", "*").split(","),
        )
        
        logging_config = LoggingConfig(
            level=os.getenv("OSINT_LOG_LEVEL", "INFO"),
            format=os.getenv("OSINT_LOG_FORMAT", "json"),
            output=os.getenv("OSINT_LOG_OUTPUT", "stdout"),
            file_path=os.getenv("OSINT_LOG_FILE", "./logs/osint.log"),
        )
        
        return cls(
            environment=os.getenv("OSINT_ENVIRONMENT", "development"),
            debug=os.getenv("OSINT_DEBUG", "false").lower() == "true",
            messaging=messaging,
            storage=storage,
            agents=agents,
            api=api,
            logging=logging_config,
        )
    
    def validate(self) -> List[str]:
        """
        Validate configuration and return list of issues.
        
        Returns:
            List[str]: List of validation error messages
        """
        issues = []
        
        # Check for required production settings
        if self.environment == "production":
            if not self.api.jwt_secret:
                issues.append("JWT secret is required in production")
            if self.api.cors_origins == ["*"]:
                issues.append("CORS origins should be restricted in production")
            if self.messaging.password == "guest":
                issues.append("Default messaging password should not be used in production")
        
        return issues
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert configuration to dictionary (excluding sensitive values).
        
        Returns:
            Dict[str, Any]: Configuration as dictionary
        """
        return {
            "environment": self.environment,
            "debug": self.debug,
            "messaging": {
                "broker_type": self.messaging.broker_type,
                "host": self.messaging.host,
                "port": self.messaging.port,
                "exchange_name": self.messaging.exchange_name,
            },
            "storage": {
                "database_pool_size": self.storage.database_pool_size,
                "raw_storage_type": self.storage.raw_storage_type,
            },
            "agents": {
                "default_rate_limit": self.agents.default_rate_limit,
                "max_concurrent_tasks": self.agents.max_concurrent_tasks,
                "heartbeat_interval": self.agents.heartbeat_interval,
            },
            "api": {
                "host": self.api.host,
                "port": self.api.port,
                "workers": self.api.workers,
                "auth_enabled": self.api.auth_enabled,
            },
            "logging": {
                "level": self.logging.level,
                "format": self.logging.format,
                "output": self.logging.output,
            },
        }


# Singleton config instance
_config: Optional[Config] = None


@lru_cache(maxsize=1)
def get_config() -> Config:
    """
    Get the global configuration instance.
    
    Returns:
        Config: Global configuration singleton
    """
    global _config
    if _config is None:
        _config = Config.from_env()
    return _config


def reload_config() -> Config:
    """
    Reload configuration from environment.
    
    Returns:
        Config: New configuration instance
    """
    global _config
    get_config.cache_clear()
    _config = Config.from_env()
    return _config
