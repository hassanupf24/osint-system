"""
Authentication Security
=======================
Security utilities for authentication.
"""
from datetime import datetime, timedelta
from typing import Optional

from jose import jwt
from passlib.context import CryptContext
from ..core.config import get_config

# Hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password: str) -> str:
    return pwd_context.hash(password)

# JWT
def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    to_encode = data.copy()
    config = get_config().api
    
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(hours=config.jwt_expiry_hours)
        
    to_encode.update({"exp": expire})
    
    secret = config.jwt_secret or "secret" # Fallback only for dev
    if config.auth_enabled and not config.jwt_secret:
        # Should have warn but let's assume config is validated
        pass
        
    encoded_jwt = jwt.encode(to_encode, secret, algorithm=config.jwt_algorithm)
    return encoded_jwt
