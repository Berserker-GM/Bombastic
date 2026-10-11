"""SQLAlchemy engine and session factory."""

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from radar.config import get_settings


def normalize_database_url(database_url: str) -> str:
    """Use psycopg v3 for Neon and Supabase URLs.

    Those hosts provide ``postgres://`` or ``postgresql://``. SQLAlchemy would
    otherwise import psycopg2, which this project does not ship.
    """

    scheme, separator, rest = database_url.partition("://")
    if separator != "://":
        return database_url
    if scheme in ("postgres", "postgresql"):
        return f"postgresql+psycopg://{rest}"
    return database_url


def create_db_engine(database_url: str) -> Engine:
    return create_engine(normalize_database_url(database_url))


def create_session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine)


engine = create_db_engine(get_settings().database_url)
SessionLocal = create_session_factory(engine)
