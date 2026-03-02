"""
Encrypted Pickle Store for Orcha.

Provides encrypted pickle storage in the Orcha database.
All pickled objects (task functions, sources, sinks, transforms, entities)
are encrypted using Fernet symmetric encryption before being stored.

The master encryption key is set via the ORCHA_PICKLE_KEY environment variable.
"""
from __future__ import annotations

import base64
import hashlib
import os
import pickle
from dataclasses import dataclass
from datetime import datetime as dt
from typing import Any, Literal
from uuid import uuid4

from cryptography.fernet import Fernet
from sqlalchemy import Column, DateTime, LargeBinary, String, Text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.sql import text as sql

from orcha import current_time
from orcha.utils.log import LogManager
from orcha.utils.sqlalchemy import postgres_scaffold, sqlalchemy_build

_pickle_log = LogManager('pickle_store')

Base: DeclarativeMeta
engine: Engine
s_maker: sessionmaker[Session]
PickleRecord: type
_fernet: Fernet | None = None
is_initialised = False


PickleType = Literal[
    'entity', 'source', 'sink', 'transform',
    'validation', 'task_function', 'task'
]


def _derive_fernet_key(master_key: str) -> bytes:
    """
    Derive a valid 32-byte Fernet key from an arbitrary master key string
    using SHA-256 and base64 encoding.
    """
    digest = hashlib.sha256(master_key.encode()).digest()
    return base64.urlsafe_b64encode(digest)


def initialise(
        orcha_user: str, orcha_pass: str,
        orcha_server: str, orcha_db: str,
        orcha_schema: str,
        master_key: str | None = None
    ):
    """
    Initialise the pickle store with database connection and encryption key.
    """
    global is_initialised, Base, engine, s_maker, PickleRecord, _fernet

    if master_key is None:
        master_key = os.environ.get('ORCHA_PICKLE_KEY')
    if master_key is None:
        _pickle_log.add_entry(
            actor='pickle_store', category='init',
            text='No ORCHA_PICKLE_KEY set - pickle store will not be available',
            json={}
        )
        return

    _fernet = Fernet(_derive_fernet_key(master_key))

    Base, engine, s_maker = postgres_scaffold(
        user=orcha_user,
        passwd=orcha_pass,
        server=orcha_server,
        db=orcha_db,
        schema=orcha_schema,
        application_name='pickle_store'
    )

    class _PickleRecord(Base):
        __tablename__ = 'pickle_store'

        pickle_idk = Column(String, primary_key=True)
        pickle_type = Column(String, nullable=False)
        name = Column(String, nullable=False)
        description = Column(Text, nullable=True)
        module_idk = Column(String, nullable=True)
        pickle_data = Column(LargeBinary, nullable=False)
        source_code = Column(Text, nullable=True)
        metadata_json = Column(Text, nullable=True)
        created_at = Column(DateTime(timezone=False), nullable=False)
        updated_at = Column(DateTime(timezone=False), nullable=False)
        created_by = Column(String, nullable=True)
        environment_id = Column(String, nullable=True)

    PickleRecord = _PickleRecord
    sqlalchemy_build(Base, engine, orcha_schema)
    is_initialised = True

    _pickle_log.add_entry(
        actor='pickle_store', category='init',
        text='Pickle store initialised',
        json={}
    )


def _check_initialised():
    if not is_initialised:
        raise RuntimeError('Pickle store not initialised. Ensure ORCHA_PICKLE_KEY is set.')
    if _fernet is None:
        raise RuntimeError('Encryption key not available.')


def store_task_definition(
        task_idk: str,
        name: str,
        description: str,
        source_code: str,
        schedule_sets: list[dict],
        thread_group: str = 'pickle_tasks',
        task_tags: list[str] | None = None,
        task_metadata: dict | None = None,
        created_by: str | None = None,
        environment_id: str | None = None,
    ) -> str:
    """
    Store a task definition (source code + metadata) in the pickle store.
    Unlike encrypt_and_store, this does NOT pickle a Python object.
    Instead it stores the source code and a JSON metadata blob as
    encrypted data, so the function can be recompiled on each load.
    """
    _check_initialised()

    import json as _json

    task_def = {
        'task_idk': task_idk,
        'name': name,
        'description': description,
        'source_code': source_code,
        'schedule_sets': schedule_sets,
        'thread_group': thread_group,
        'task_tags': task_tags or ['pickle'],
        'task_metadata': task_metadata or {},
    }
    raw_bytes = _json.dumps(task_def).encode('utf-8')
    encrypted_bytes = _fernet.encrypt(raw_bytes)  # type: ignore

    now = current_time()
    idk = f'task_{task_idk}'

    full_metadata = {
        'pickle_task': True,
        'schedule_sets': schedule_sets,
        'thread_group': thread_group,
        'task_tags': task_tags or ['pickle'],
        **(task_metadata or {}),
    }
    metadata_str = _json.dumps(full_metadata)

    with s_maker.begin() as session:
        record = {
            'pickle_idk': idk,
            'pickle_type': 'task',
            'name': name,
            'description': description,
            'module_idk': task_idk,
            'pickle_data': encrypted_bytes,
            'source_code': None,  # source is inside the encrypted blob only
            'metadata_json': metadata_str,
            'created_at': now,
            'updated_at': now,
            'created_by': created_by,
            'environment_id': environment_id,
        }
        insert_stmt = insert(PickleRecord).values(record)
        update_dict = {k: v for k, v in record.items() if k != 'pickle_idk' and k != 'created_at'}
        update_stmt = insert_stmt.on_conflict_do_update(
            index_elements=['pickle_idk'],
            set_=update_dict
        )
        session.execute(update_stmt)

    _pickle_log.add_entry(
        actor='pickle_store', category='store_task',
        text=f'Stored task definition: {name}',
        json={'pickle_idk': idk, 'task_idk': task_idk, 'name': name}
    )
    return idk


def load_task_definitions(
        environment_id: str | None = None
    ) -> list[dict]:
    """
    Load all task definitions from the pickle store.
    Decrypts the stored JSON blobs and returns them as dicts.
    Each dict contains: task_idk, name, description, source_code,
    schedule_sets, thread_group, task_tags, task_metadata.
    """
    _check_initialised()
    import json as _json

    results = []
    with s_maker.begin() as session:
        query = session.query(PickleRecord).filter(
            PickleRecord.pickle_type == 'task'
        )
        if environment_id:
            query = query.filter(PickleRecord.environment_id == environment_id)
        records = query.all()

        for record in records:
            try:
                raw_bytes = _fernet.decrypt(record.pickle_data)  # type: ignore
                task_def = _json.loads(raw_bytes.decode('utf-8'))
                task_def['pickle_idk'] = record.pickle_idk
                results.append(task_def)
            except Exception as e:
                _pickle_log.add_entry(
                    actor='pickle_store', category='load_task_error',
                    text=f'Failed to load task definition {record.pickle_idk}: {str(e)}',
                    json={'pickle_idk': record.pickle_idk, 'error': str(e)}
                )
    return results


def encrypt_and_store(
        pickle_type: PickleType,
        name: str,
        obj: Any,
        description: str = '',
        module_idk: str | None = None,
        source_code: str | None = None,
        metadata: dict | None = None,
        created_by: str | None = None,
        environment_id: str | None = None,
        pickle_idk: str | None = None,
    ) -> str:
    """
    Pickle an object, encrypt it, and store it in the database.
    Returns the pickle_idk.
    """
    _check_initialised()

    # Pickle the object
    raw_bytes = pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
    # Encrypt
    encrypted_bytes = _fernet.encrypt(raw_bytes)  # type: ignore

    now = current_time()
    idk = pickle_idk or str(uuid4())

    import json
    metadata_str = json.dumps(metadata) if metadata else None

    with s_maker.begin() as session:
        record = {
            'pickle_idk': idk,
            'pickle_type': pickle_type,
            'name': name,
            'description': description,
            'module_idk': module_idk,
            'pickle_data': encrypted_bytes,
            'source_code': source_code,
            'metadata_json': metadata_str,
            'created_at': now,
            'updated_at': now,
            'created_by': created_by,
            'environment_id': environment_id,
        }
        insert_stmt = insert(PickleRecord).values(record)
        update_dict = {k: v for k, v in record.items() if k != 'pickle_idk' and k != 'created_at'}
        update_stmt = insert_stmt.on_conflict_do_update(
            index_elements=['pickle_idk'],
            set_=update_dict
        )
        session.execute(update_stmt)

    _pickle_log.add_entry(
        actor='pickle_store', category='store',
        text=f'Stored pickle: {name}',
        json={'pickle_idk': idk, 'type': pickle_type, 'name': name}
    )
    return idk


def load_and_decrypt(pickle_idk: str) -> Any:
    """
    Load an encrypted pickle from the database, decrypt it, and return the object.
    """
    _check_initialised()

    with s_maker.begin() as session:
        record = session.query(PickleRecord).filter(
            PickleRecord.pickle_idk == pickle_idk
        ).first()
        if record is None:
            raise ValueError(f'Pickle not found: {pickle_idk}')

        encrypted_bytes = record.pickle_data
        raw_bytes = _fernet.decrypt(encrypted_bytes)  # type: ignore
        return pickle.loads(raw_bytes)


def get_all(
        pickle_type: PickleType | None = None,
        environment_id: str | None = None
    ) -> list[PickleInfo]:
    """
    Get all pickle entries (metadata only, not the actual data).
    """
    _check_initialised()
    with s_maker.begin() as session:
        query = session.query(PickleRecord)
        if pickle_type:
            query = query.filter(PickleRecord.pickle_type == pickle_type)
        if environment_id:
            query = query.filter(PickleRecord.environment_id == environment_id)
        records = query.all()
        return [PickleInfo.from_record(r) for r in records]


def delete(pickle_idk: str):
    """
    Delete a pickle from the store.
    """
    _check_initialised()
    with s_maker.begin() as session:
        session.execute(sql('''
            DELETE FROM orcha.pickle_store
            WHERE pickle_idk = :idk
        '''), {'idk': pickle_idk})

    _pickle_log.add_entry(
        actor='pickle_store', category='delete',
        text=f'Deleted pickle: {pickle_idk}',
        json={'pickle_idk': pickle_idk}
    )


@dataclass
class PickleInfo:
    """
    Metadata about a pickle entry (without the actual pickled data).
    """
    pickle_idk: str
    pickle_type: str
    name: str
    description: str
    module_idk: str | None
    source_code: str | None
    metadata_json: str | None
    created_at: dt
    updated_at: dt
    created_by: str | None
    environment_id: str | None

    @staticmethod
    def from_record(record) -> PickleInfo:
        return PickleInfo(
            pickle_idk=record.pickle_idk,
            pickle_type=record.pickle_type,
            name=record.name,
            description=record.description or '',
            module_idk=record.module_idk,
            source_code=record.source_code,
            metadata_json=record.metadata_json,
            created_at=record.created_at,
            updated_at=record.updated_at,
            created_by=record.created_by,
            environment_id=record.environment_id,
        )

    def to_dict(self) -> dict:
        import json
        return {
            'pickle_idk': self.pickle_idk,
            'pickle_type': self.pickle_type,
            'name': self.name,
            'description': self.description,
            'module_idk': self.module_idk,
            'source_code': self.source_code,
            'metadata': json.loads(self.metadata_json) if self.metadata_json else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
            'created_by': self.created_by,
            'environment_id': self.environment_id,
        }
