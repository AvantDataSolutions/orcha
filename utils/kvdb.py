
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime as dt, timedelta as td
import threading
from typing import Any, Literal, Type, TypeVar, Union

from workspace.credentials import *

print('Loading page:',__name__)

T = TypeVar('T')

store_threaded = {}
store_global = {}


@dataclass
class KvdbItem():
    storage_type: Literal['postgres', 'local', 'global']
    key: str
    value: Any
    type: str
    expiry: dt | None


def store(
        storage_type: Literal['postgres', 'local', 'global'],
        key: str, value: Any, expiry: td | None = None
    ):
    if storage_type == 'postgres':
        raise NotImplementedError
    elif storage_type == 'local':
        exp_time = dt.utcnow() + expiry if expiry else None
        item = KvdbItem(storage_type, key, value, type(value).__name__, exp_time)
        if threading.current_thread().ident not in store_threaded:
            store_threaded[threading.current_thread().ident] = {}
        store_threaded[threading.current_thread().ident][key] = item
    elif storage_type == 'global':
        raise NotImplementedError


def get(key: str, as_type: Type[T]) -> Union[T, None]:
    result = None
    if threading.current_thread().ident not in store_threaded:
        store_threaded[threading.current_thread().ident] = {}
    if key in store_threaded[threading.current_thread().ident]:
        result = store_threaded[threading.current_thread().ident][key]
        if result is None:
            return None
        if result.expiry is not None and result.expiry < dt.utcnow():
            return None
        return result.value
    elif key in store_global:
        raise NotImplementedError
    else:
        raise NotImplementedError