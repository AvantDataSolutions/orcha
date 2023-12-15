
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
        raise Exception('Postgres kvdb storage not implemented')
    elif storage_type == 'local':
        exp_time = dt.utcnow() + expiry if expiry else None
        item = KvdbItem(storage_type, key, value, type(value).__name__, exp_time)
        if threading.current_thread().ident not in store_threaded:
            store_threaded[threading.current_thread().ident] = {}
        store_threaded[threading.current_thread().ident][key] = item
    elif storage_type == 'global':
        raise Exception('Global kvdb storage not implemented')


def get(
        key: str, as_type: Type[T],
        storage_type: Literal['postgres', 'local', 'global'],
        no_key_return: Literal['none', 'exception'] = 'none'
    ) -> Union[T, None]:
    result = None
    if threading.current_thread().ident not in store_threaded:
        store_threaded[threading.current_thread().ident] = {}
    if storage_type == 'postgres':
        raise Exception('Postgres kvdb storage not implemented')
    elif storage_type == 'local':
        if key in store_threaded[threading.current_thread().ident]:
            result = store_threaded[threading.current_thread().ident][key]
            if result is None:
                return None
            if result.expiry is not None and result.expiry < dt.utcnow():
                return None
            return result.value
        else:
            if no_key_return == 'none':
                return None
            elif no_key_return == 'exception':
                raise Exception(f'Key {key} not found in store')
            else:
                raise Exception(f'Invalid no_key_return: {no_key_return}')
    elif storage_type == 'global':
        raise Exception('Global kvdb storage not implemented')
    else:
        raise Exception(f'Key {key} not found in store')