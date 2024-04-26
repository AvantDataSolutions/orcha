
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime as dt, timedelta as td
import threading
from typing import Any, Literal, Type, TypeVar, Union

print('Loading:',__name__)

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
        key: str, value: Any,
        thread_name: str | None = None,
        expiry: td | None = None,
    ):
    """
    Store a value in the store.
    #### Arguments
    - `storage_type`: The type of storage to store the value in.
    - `key`: The key to store the value under.
    - `value`: The value to store.
    - `expiry`: The time to expire the value.
    - `thread_name`: The name of the thread to store the value in.
        Defaults to the current thread's name. However
        can be used to share a common store between threads.
    """
    if thread_name is None:
        thread_name = threading.current_thread().name
    if storage_type == 'postgres':
        raise Exception('Postgres kvdb storage not implemented')
    elif storage_type == 'local':
        exp_time = dt.utcnow() + expiry if expiry else None
        item = KvdbItem(storage_type, key, value, type(value).__name__, exp_time)
        if thread_name not in store_threaded:
            store_threaded[thread_name] = {}
        store_threaded[thread_name][key] = item
    elif storage_type == 'global':
        raise Exception('Global kvdb storage not implemented')


def get(
        key: str, as_type: Type[T],
        storage_type: Literal['postgres', 'local', 'global'],
        thread_name: str | None = None,
        no_key_return: Literal['none', 'exception'] = 'none',
    ) -> Union[T, None]:
    """
    Get a value from the store.
    #### Arguments
    - `key`: The key to get from the store.
    - `as_type`: The type to cast the value to.
    - `storage_type`: The type of storage to get the value from.
    - `no_key_return`: What to do if the key is not found in the store. Options are:
        - `'none'`: Return `None`.
        - `'exception'`: Raise an exception.
    - `thread_name`: The name of the thread to get the value from.
        Defaults to the current thread's name. However
        can be used to share a common store between threads.
    #### Returns
    The value from the store in the specified type.
    """
    if thread_name is None:
        thread_name = threading.current_thread().name
    result = None
    if thread_name not in store_threaded:
        store_threaded[thread_name] = {}
    if storage_type == 'postgres':
        raise Exception('Postgres kvdb storage not implemented')
    elif storage_type == 'local':
        if key in store_threaded[thread_name]:
            result = store_threaded[thread_name][key]
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