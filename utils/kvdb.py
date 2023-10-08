
from __future__ import annotations
import json
from datetime import datetime as dt, timedelta as td
from typing import Any, Type, TypeVar, Union

import pandas as pd
from sqlalchemy import Column, DateTime, String

from utils.sqlalchemy import sqlalchemy_build, sqlalchemy_scaffold

from orcha.core.credentials import *

print('Loading page:',__name__)


(Base, engine, Session, CUR_SCHEMA) = sqlalchemy_scaffold('kvdb')

T = TypeVar('T')

class Kvdb(Base):
    __tablename__ = 'kvdb'
    key = Column(String, primary_key=True)
    value = Column(String)
    type = Column(String)
    expiry = Column(DateTime)


def store_kvp(key: str, value: Any, expiry: td | None = None):
    with Session.begin() as db:
        kvp = db.query(Kvdb).filter_by(key=key).first()
        if isinstance(value, dt):
            value = value.isoformat()
            value_type = 'datetime'
        elif isinstance(value, dict):
            value = json.dumps(value)
            value_type = 'dict'
        elif isinstance(value, pd.DataFrame):
            value = value.to_json()
            value_type = 'dataframe'
        else:
            value_type = type(value).__name__

        if kvp:
            exp_dt = dt.utcnow() + expiry if expiry else None
            kvp.value = value
            kvp.type = value_type
            kvp.expiry = exp_dt
        else:
            exp_dt = dt.utcnow() + expiry if expiry else None
            kvp = Kvdb(key=key, value=value, type=value_type, expiry=exp_dt)
            db.add(kvp)


def get_kvp(key: str, as_type: Type[T]) -> Union[T, None]:
    with Session.begin() as db:
        result = db.query(Kvdb).filter_by(key=key).first()
        if result:
            if result.expiry is None or result.expiry < dt.utcnow():
                return None
            if result.type == 'datetime':
                val = dt.fromisoformat(result.value)
            elif result.type == 'dict':
                val = json.loads(result.value)
            elif result.type == 'dataframe':
                val = pd.read_json(result.value)
            else:
                val = result.value
            if isinstance(val, as_type):
                return val
            else:
                raise TypeError(f'Expected {as_type} but got {type(val)}')
        else:
            return None


# Create the schema and tables if needed
sqlalchemy_build(Base, engine, CUR_SCHEMA)