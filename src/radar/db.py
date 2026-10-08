"""SQLAlchemy engine and session factory."""

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from radar.config import get_settings


def create_db_engine(database_url: str) -> Engine:
    return create_engine(database_url)


def create_session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine)


engine = create_db_engine(get_settings().database_url)
SessionLocal = create_session_factory(engine)
