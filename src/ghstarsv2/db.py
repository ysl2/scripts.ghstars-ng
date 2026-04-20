from __future__ import annotations

from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from src.ghstarsv2.config import get_settings


class Base(DeclarativeBase):
    pass


_engine: Engine | None = None
_session_maker: sessionmaker[Session] | None = None


def configure_database(database_url: str | None = None) -> None:
    global _engine, _session_maker

    if _engine is not None:
        _engine.dispose()

    url = database_url or get_settings().database_url
    connect_args: dict[str, object] = {}
    if url.startswith("sqlite"):
        connect_args["check_same_thread"] = False

    _engine = create_engine(
        url,
        pool_pre_ping=True,
        connect_args=connect_args,
    )
    _session_maker = sessionmaker(bind=_engine, autoflush=False, autocommit=False, expire_on_commit=False)


def get_engine() -> Engine:
    if _engine is None or _session_maker is None:
        configure_database()
    assert _engine is not None
    return _engine


def get_session_maker() -> sessionmaker[Session]:
    if _engine is None or _session_maker is None:
        configure_database()
    assert _session_maker is not None
    return _session_maker


def get_db() -> Generator[Session, None, None]:
    db = get_session_maker()()
    try:
        yield db
    finally:
        db.close()


@contextmanager
def session_scope() -> Generator[Session, None, None]:
    db = get_session_maker()()
    try:
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
