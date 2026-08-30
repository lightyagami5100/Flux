import os
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import Column, Integer, String, Float, DateTime

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://flux_user:flux_password@localhost:5432/flux_db")

engine = create_async_engine(DATABASE_URL, echo=False)
AsyncSessionLocal = sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)

Base = declarative_base()

class Detection(Base):
    __tablename__ = "detections"
    
    id = Column(Integer, primary_key=True, index=True)
    class_name = Column(String, index=True)
    confidence = Column(Float)
    severity = Column(String, index=True)
    lat = Column(Float)
    lon = Column(Float)
    timestamp = Column(DateTime)
    city = Column(String, index=True)
    area = Column(String)

class Notification(Base):
    __tablename__ = "notifications"
    
    id = Column(Integer, primary_key=True, index=True)
    message = Column(String)
    city = Column(String)
    area = Column(String)
    lat = Column(Float)
    lon = Column(Float)
    timestamp = Column(DateTime)
    read = Column(Integer, default=0)

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

# Dependency to get DB session
async def get_db():
    async with AsyncSessionLocal() as session:
        yield session
