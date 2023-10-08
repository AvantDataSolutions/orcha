from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime as dt
from enum import IntEnum
from uuid import uuid4

from sqlalchemy import Column, DateTime
from sqlalchemy import Enum as SQL_Enum
from sqlalchemy import String, exc
from sqlalchemy.dialects.postgresql import JSON as PG_JSON
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.sql import text as sql

from utils.sqlalchemy import sqlalchemy_build, sqlalchemy_scaffold, get_latest_versions

from ..credentials import *

print('Loading dh:',__name__)


(Base, engine, Session, CUR_SCHEMA) = sqlalchemy_scaffold('orcha')


class RunStatus():
    QUEUED = 'queued'
    RUNNING = 'running'
    SUCCESS = 'success'
    FAILED = 'failed'
    CANCELLED = 'cancelled'


class ServiceStatusRecord(Base):
    __tablename__ = 'service_status'

    service_type = Column(String, primary_key=True)
    service_idf = Column(String, primary_key=True)
    status_time = Column(DateTime(timezone=False))


@dataclass
class ServiceStatusRecordItem():
    service_type: str
    service_idf: str
    status_time: dt

    @staticmethod
    def update_status(service_type: str, service_idf: str):
        status_time = dt.utcnow()
        with Session.begin() as session:
            session.add(ServiceStatusRecord(
                service_type=service_type,
                service_idf=service_idf,
                status_time=status_time
            ))
            return True

    @staticmethod
    def get_latest(service_type: str, service_idf: str):
        with Session.begin() as session:
            latest = session.query(ServiceStatusRecord).filter(
                ServiceStatusRecord.service_type == service_type,
                ServiceStatusRecord.service_idf == service_idf
            ).order_by(ServiceStatusRecord.status_time.desc()).first()
            return latest


sqlalchemy_build(Base, engine, CUR_SCHEMA)
