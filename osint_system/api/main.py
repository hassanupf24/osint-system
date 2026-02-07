"""
OSINT API
==========
REST API for interacting with the OSINT System.
"""

from contextlib import asynccontextmanager
from typing import Dict, Any, Optional, List
from datetime import timedelta

from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel, HttpUrl
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession
from jose import JWTError, jwt

from ..messaging.publisher import Publisher
from ..messaging.broker import get_message_broker
from ..messaging.message import MessageType, MessagePriority
from ..storage.database import get_db, DatabaseManager
from ..storage.models import RawData, ProcessedData, User
from ..core.config import get_config
from .security import verify_password, create_access_token, get_password_hash

# --- Models ---
class CollectRequest(BaseModel):
    """
    Data model for collection requests.
    """
    target_type: str  # e.g., 'social_media', 'domain'
    target: str       # e.g., 'johndoe', 'example.com'
    platform: Optional[str] = None # e.g., 'twitter' for social_media
    priority: str = "normal"      # low, normal, high, critical

class AnalysisRequest(BaseModel):
    """
    Data model for analysis requests.
    """
    target_id: str
    analysis_type: str

class Token(BaseModel):
    access_token: str
    token_type: str

class TokenData(BaseModel):
    username: Optional[str] = None

class DataResponse(BaseModel):
    id: str
    source: str
    target: str
    timestamp: str
    data: Dict[str, Any]

class AnalysisResponse(BaseModel):
    id: str
    analysis_type: str
    score: Optional[int]
    result: Dict[str, Any]
    timestamp: str

# --- Components ---
# Global publisher instance
publisher: Optional[Publisher] = None
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Startup and shutdown logic.
    """
    global publisher
    try:
        # Initialize DB
        db_manager = DatabaseManager()
        await db_manager.connect()
        await db_manager.create_tables()
        
        # Initialize messaging
        broker = get_message_broker()
        await broker.connect()
        publisher = Publisher(broker=broker, agent_id="api_gateway_01")
        
        # Create default user if not exists
        async for session in db_manager.get_session():
            result = await session.execute(select(User).where(User.username == "admin"))
            user = result.scalars().first()
            if not user:
                hashed_pw = get_password_hash("admin") # Default password
                new_user = User(username="admin", hashed_password=hashed_pw, is_superuser=True)
                session.add(new_user)
                await session.commit()
                
        yield
    finally:
        if broker:
            await broker.disconnect()
        await db_manager.disconnect()

app = FastAPI(
    title="OSINT Agent System API",
    description="API for managing OSINT collection and analysis tasks.",
    version="0.1.0",
    lifespan=lifespan
)

# --- Dependency ---
async def get_current_user(token: str = Depends(oauth2_scheme), db: AsyncSession = Depends(get_db)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        config = get_config().api
        secret = config.jwt_secret or "secret"
        payload = jwt.decode(token, secret, algorithms=[config.jwt_algorithm])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
    except JWTError:
        raise credentials_exception
        
    result = await db.execute(select(User).where(User.username == token_data.username))
    user = result.scalars().first()
    if user is None:
        raise credentials_exception
    return user

# --- Endpoints ---

@app.post("/token", response_model=Token)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends(), db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(User).where(User.username == form_data.username))
    user = result.scalars().first()
    
    if not user or not verify_password(form_data.password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
        
    access_token_expires = timedelta(hours=24)
    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "ok", "service": "osint-api"}

@app.post("/api/v1/collect", status_code=202)
async def submit_collection_task(
    request: CollectRequest, 
    current_user: User = Depends(get_current_user)
):
    """
    Submit a new collection task (Authenticated).
    """
    if not publisher:
        raise HTTPException(status_code=503, detail="Message broker not connected")
    
    priority_map = {
        "low": MessagePriority.LOW,
        "normal": MessagePriority.NORMAL,
        "high": MessagePriority.HIGH,
        "critical": MessagePriority.CRITICAL
    }
    
    priority = priority_map.get(request.priority.lower(), MessagePriority.NORMAL)
    
    payload = {
        "target_type": request.target_type,
        "target": request.target,
        "platform": request.platform,
        "coordinator": "manager_01",
        "requested_by": current_user.username
    }
    
    try:
        routing_key = "requests.collect"
        
        message = await publisher.publish(
            message_type=MessageType.COLLECT_REQUEST,
            payload=payload,
            routing_key=routing_key,
            priority=priority,
            target_agent="manager_01" 
        )
        
        return {
            "status": "accepted",
            "task_id": message.metadata.message_id,
            "message": "Collection task submitted successfully"
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to submit task: {str(e)}")

@app.get("/api/v1/data", response_model=List[DataResponse])
async def get_raw_data(
    skip: int = 0, 
    limit: int = 100, 
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get raw data entries from storage.
    """
    result = await db.execute(select(RawData).offset(skip).limit(limit).order_by(RawData.timestamp.desc()))
    data_items = result.scalars().all()
    
    return [
        DataResponse(
            id=item.id,
            source=item.source,
            target=item.target or "unknown",
            timestamp=item.timestamp.isoformat(),
            data=item.data
        ) for item in data_items
    ]

@app.get("/api/v1/analysis", response_model=List[AnalysisResponse])
async def get_analysis_results(
    skip: int = 0, 
    limit: int = 100, 
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get analysis results from storage.
    """
    result = await db.execute(select(ProcessedData).offset(skip).limit(limit).order_by(ProcessedData.timestamp.desc()))
    items = result.scalars().all()
    
    return [
        AnalysisResponse(
            id=item.id,
            analysis_type=item.analysis_type,
            score=item.score,
            result=item.result,
            timestamp=item.timestamp.isoformat()
        ) for item in items
    ]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
