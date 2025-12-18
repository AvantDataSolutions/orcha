
from __future__ import annotations

import base64
import os
import pickle
import tempfile
import threading
from dataclasses import dataclass
from datetime import datetime as dt
from datetime import timedelta as td
from typing import Any, Literal, Type, TypeVar, Union

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives.kdf.scrypt import Scrypt
from sqlalchemy import DateTime, LargeBinary, String
from sqlalchemy.orm import Mapped, Session, mapped_column, sessionmaker

from orcha.utils.sqlalchemy import postgres_scaffold, sqlalchemy_build

print('Loading:',__name__)

T = TypeVar('T')

_store_threaded = {}
_store_global = {}


is_initialised = False
_sessionmaker: sessionmaker[Session] | None = None

_kvdb_folder = os.path.join(tempfile.gettempdir(), 'orcha_kvdb')


@dataclass
class _KvdbFileRecord():
    key: str
    type: str
    expiry: dt | None
    salt: bytes | None
    value: bytes
    encrypted: bool
    stored_at: dt
    storage_type: str


def _validate_key_name(key_name: str) -> None:
    if not key_name or key_name in ('.', '..'):
        raise Exception('Invalid kvdb key for file storage')
    if os.path.basename(key_name) != key_name:
        raise Exception('Invalid kvdb key for file storage')
    if os.sep in key_name:
        raise Exception('Invalid kvdb key for file storage')
    if os.altsep and os.altsep in key_name:
        raise Exception('Invalid kvdb key for file storage')


def _write_record(key_name: str, record: _KvdbFileRecord) -> None:
    _validate_key_name(key_name)
    data = pickle.dumps(record)
    os.makedirs(_kvdb_folder, exist_ok=True)
    with open(os.path.join(_kvdb_folder, key_name), 'wb') as f:
        f.write(data)


def _read_record(key_name: str) -> _KvdbFileRecord | None:
    _validate_key_name(key_name)
    path = os.path.join(_kvdb_folder, key_name)
    if not os.path.exists(path):
        return None
    with open(path, 'rb') as f:
        data = f.read()
    return pickle.loads(data)


def _delete_record(key_name: str) -> None:
    _validate_key_name(key_name)
    path = os.path.join(_kvdb_folder, key_name)
    try:
        os.remove(path)
    except FileNotFoundError:
        pass


def _get_fernet(password: str, salt: bytes) -> Fernet:
    kdf = Scrypt(
        salt=salt,
        length=32,
        n=2**14,
        r=8,
        p=1,
    )
    key = kdf.derive(password.encode())
    b64_key = base64.urlsafe_b64encode(key)
    return Fernet(b64_key)


def initialise(
        postgres_user: str,
        postgres_pass: str,
        postgres_server: str,
        postgres_db: str,
        postgres_schema: str,
    ):
    """
    Initialise the kvdb module with a Postgres connection.
    """

    global is_initialised, _sessionmaker, KvdbItemModel
    if is_initialised:
        return

    _Base, _engine, _sessionmaker = postgres_scaffold(
        user=postgres_user,
        passwd=postgres_pass,
        server=postgres_server,
        db=postgres_db,
        schema=postgres_schema,
        application_name='orcha_kvdb'
    )

    class KvdbItemModel(_Base):
        __tablename__ = 'kvdb_items'

        key: Mapped[str] = mapped_column(String, primary_key=True)
        value: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)
        type: Mapped[str] = mapped_column(String, nullable=False)
        expiry: Mapped[dt] = mapped_column(DateTime)
        salt: Mapped[bytes] = mapped_column(LargeBinary, nullable=True)

    sqlalchemy_build(base=_Base, engine=_engine, schema_name=postgres_schema)

    is_initialised = True

@dataclass
class _KvdbItem():
    storage_type: Literal['postgres', 'local', 'global', 'file']
    key: str
    value: Any
    type: str
    expiry: dt | None


def store(
        storage_type: Literal['postgres', 'local', 'global', 'file'],
        key: str, value: Any,
        thread_name: str | None = None,
        expiry: td | None = None,
        encryption_key: str | None = None,
    ):
    """
    Store a value in the store.
    #### Arguments
    - `storage_type`:
        - 'local': Store the value in a thread-local store.
        - 'global': Store the value in a global store.
        - 'postgres': Store the value in the Orcha Postgres database.
    - `key`: The key to store the value under.
    - `value`: The value to store.
    - `expiry`: The time to expire the value.
    - `thread_name`: The name of the thread to store the value in.
        Defaults to the current thread's name. However
        can be used to share a common store between threads.
    - `encryption_key`: An optional encryption key to encrypt when using Postgres.
    """
    salt = os.urandom(16)
    def _encr_data(data: bytes) -> bytes:
        if not encryption_key:
            return data
        f = _get_fernet(encryption_key, salt)
        value_bytes = pickle.dumps(value)
        return f.encrypt(value_bytes)

    if thread_name is None:
        thread_name = threading.current_thread().name
    if storage_type == 'postgres':
        if not is_initialised or _sessionmaker is None:
            raise Exception('KVDB module not initialised for Postgres storage.')
        with _sessionmaker.begin() as tx:
            data = pickle.dumps(value)
            item = KvdbItemModel(
                key=key,
                value=_encr_data(data) if encryption_key else data,
                type=type(value).__name__,
                expiry=(dt.now() + expiry) if expiry else None,
                salt=salt if encryption_key else None
            )
            tx.merge(item)
    elif storage_type == 'file':
        exp_time = dt.now() + expiry if expiry else None
        payload_value = pickle.dumps(value)
        value_bytes = _encr_data(payload_value) if encryption_key else payload_value
        record = _KvdbFileRecord(
            key=key,
            type=type(value).__name__,
            expiry=exp_time,
            salt=salt if encryption_key else None,
            value=value_bytes,
            encrypted=bool(encryption_key),
            stored_at=dt.now(),
            storage_type='file',
        )
        _write_record(key, record)
    elif storage_type == 'local':
        exp_time = dt.now() + expiry if expiry else None
        item = _KvdbItem(storage_type, key, value, type(value).__name__, exp_time)
        if thread_name not in _store_threaded:
            _store_threaded[thread_name] = {}
        _store_threaded[thread_name][key] = item
    elif storage_type == 'global':
        raise Exception('Global kvdb storage not implemented')


def get(
        key: str, as_type: Type[T],
        storage_type: Literal['postgres', 'local', 'global', 'file'],
        thread_name: str | None = None,
        no_key_return: Literal['none', 'exception'] = 'none',
        encryption_key: str | None = None,
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
    if thread_name not in _store_threaded:
        _store_threaded[thread_name] = {}
    if storage_type == 'postgres':
        if not is_initialised or _sessionmaker is None:
            raise Exception('KVDB module not initialised for Postgres storage.')
        with _sessionmaker.begin() as tx:
            item = tx.get(KvdbItemModel, key)
            if item is None:
                if no_key_return == 'none':
                    return None
                elif no_key_return == 'exception':
                    raise Exception(f'Key {key} not found in store')
                else:
                    raise Exception(f'Invalid no_key_return: {no_key_return}')
            if item.expiry is not None and item.expiry < dt.now():
                return None
            data = item.value
            if item.salt is None and encryption_key is not None:
                raise Exception(f'Key {key} is not encrypted, but encryption_key was provided')
            if item.salt and not encryption_key:
                raise Exception(f'Key {key} is encrypted, but no encryption_key was provided')
            if encryption_key:
                f = _get_fernet(encryption_key, item.salt)
                try:
                    data = f.decrypt(data)
                except Exception:
                    raise Exception(f'Failed to decrypt kvdb key {key}.')
            result = pickle.loads(data)
            if not isinstance(result, as_type):
                raise Exception(f'Key {key} found but type mismatch: expected {as_type.__name__}, got {type(result).__name__}')
        return result
    elif storage_type == 'file':
        if not os.path.exists(os.path.join(_kvdb_folder, key)):
            if no_key_return == 'none':
                return None
            if no_key_return == 'exception':
                raise Exception(f'Key {key} not found in store')
            raise Exception(f'Invalid no_key_return: {no_key_return}')

        record = _read_record(key)

        if record is None:
            if no_key_return == 'none':
                return None
            if no_key_return == 'exception':
                raise Exception(f'Key {key} not found in store')
            raise Exception(f'Invalid no_key_return: {no_key_return}')

        expiry_at = record.expiry
        if expiry_at is not None and expiry_at < dt.now():
            return None

        data = record.value
        salt_bytes = record.salt
        if salt_bytes is None and encryption_key is not None:
            raise Exception(f'Key {key} is not encrypted, but encryption_key was provided')
        if encryption_key:
            if not salt_bytes:
                raise Exception(f'Key {key} is not encrypted, but encryption_key was provided')
            f = _get_fernet(encryption_key, salt_bytes)
            try:
                data = f.decrypt(data)
            except Exception:
                raise Exception(f'Failed to decrypt kvdb key {key}.')

        result = pickle.loads(data)
        if not isinstance(result, as_type):
            raise Exception(
                f'Key {key} found but type mismatch: expected {as_type.__name__}, got {type(result).__name__}'
            )
        return result
    elif storage_type == 'local':
        if key in _store_threaded[thread_name]:
            result = _store_threaded[thread_name][key]
            if result is None:
                return None
            if result.expiry is not None and result.expiry < dt.now():
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


def list_items(
        storage_type: Literal['postgres'],
        limit: int = 200,
        search: str | None = None,
        include_expired: bool = False,
    ) -> list[dict[str, Any]]:
    """Return snapshots of kvdb entries for inspection tools."""
    if storage_type != 'postgres':
        raise NotImplementedError('Only postgres kvdb listing is supported')
    if not is_initialised or _sessionmaker is None or KvdbItemModel is None:
        raise Exception('KVDB module not initialised for Postgres storage.')

    limit = max(1, min(limit or 200, 500))
    with _sessionmaker.begin() as tx:
        query = tx.query(KvdbItemModel)
        if search:
            like_query = f'%{search}%'
            query = query.filter(KvdbItemModel.key.ilike(like_query))
        query = query.order_by(KvdbItemModel.key.asc()).limit(limit)
        rows = query.all()

    now = dt.now()
    items: list[dict[str, Any]] = []
    for row in rows:
        is_expired = row.expiry is not None and row.expiry < now
        if is_expired and not include_expired:
            continue

        is_encrypted = row.salt is not None
        preview = ''
        load_error = None
        if is_encrypted:
            preview = 'Encrypted value (key required)'
        else:
            try:
                value_obj = pickle.loads(row.value)
                preview = repr(value_obj)
            except Exception as exc:
                load_error = str(exc)
                preview = f'<unreadable: {exc}>'

        ttl_seconds = None
        if row.expiry is not None:
            ttl_seconds = int((row.expiry - now).total_seconds())

        items.append({
            'key': row.key,
            'type': row.type,
            'expiry': row.expiry,
            'is_expired': is_expired,
            'ttl_seconds': ttl_seconds,
            'size_bytes': len(row.value) if row.value else 0,
            'is_encrypted': is_encrypted,
            'value_preview': preview,
            'load_error': load_error,
        })

    return items


def delete(
        storage_type: Literal['postgres', 'local', 'file'],
        key: str,
        thread_name: str | None = None,
    ) -> bool:
    """Delete a value from the kvdb store."""
    if storage_type == 'postgres':
        if not is_initialised or _sessionmaker is None or KvdbItemModel is None:
            raise Exception('KVDB module not initialised for Postgres storage.')
        with _sessionmaker.begin() as tx:
            deleted = tx.query(KvdbItemModel).filter(KvdbItemModel.key == key).delete()
        return deleted > 0
    elif storage_type == 'local':
        if thread_name is None:
            thread_name = threading.current_thread().name
        if thread_name not in _store_threaded:
            return False
        return _store_threaded[thread_name].pop(key, None) is not None
    elif storage_type == 'file':
        _delete_record(key)
        return True
    else:
        raise NotImplementedError('Global kvdb storage not implemented')