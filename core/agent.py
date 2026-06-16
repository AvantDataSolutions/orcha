"""
Orcha Agent - FastAPI-based agent that runs alongside workspace/task_runner
containers to expose introspection and management endpoints for the UI.

The agent provides:
- Module listing (entities, sources, sinks, transforms, validations)
- Secret name listing (names only, not values)
- Task listing and management
- Pickle deployment (deploy code-created objects to the store)
- Environment info

The agent runs unauthenticated for now; future versions will add
user/key authentication.
"""
from __future__ import annotations

import contextlib
import inspect
import io
import threading
import traceback
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from orcha.common.modules.filesystem import FileSystemSink, FileSystemSource, SmbEntity
from orcha.common.modules.mssql import MssqlEntity
from orcha.common.modules.postgres import PostgresEntity
from orcha.common.modules.sqlite import SQLiteEntity
from orcha.common.modules.web import RestEntity as WebRestEntity
from orcha.common.modules.web import RestSink as WebRestSink
from orcha.common.modules.web import RestSource as WebRestSource
from orcha.core import tasks, pickle_store
from orcha.core.module_base import (
    BinarySink,
    DatabaseEntity,
    DatabaseSink,
    DatabaseSource,
    EntityBase,
    ModuleBase,
    PythonEntity,
    PythonSource,
    RestEntity,
    SinkBase,
    SourceBase,
    TransformBase,
    ValidationBase,
)
from orcha.utils.log import LogManager

_agent_log = LogManager('agent')


class AgentConfig:
    """
    Configuration for the agent.
    """
    def __init__(
            self,
            environment_id: str,
            environment_name: str,
            host: str = '0.0.0.0',
            port: int = 5555,
            secrets: dict[str, str] | None = None,
    ):
        self.environment_id = environment_id
        self.environment_name = environment_name
        self.host = host
        self.port = port
        self.secrets = secrets or {}


############################################################################
# Module Registry
############################################################################

class ModuleRegistry:
    """
    Registry of all modules available in this environment.
    Modules are registered when they are created or explicitly added.
    """
    _entities: dict[str, EntityBase] = {}
    _sources: dict[str, SourceBase] = {}
    _sinks: dict[str, SinkBase] = {}
    _transforms: dict[str, TransformBase] = {}
    _validations: dict[str, ValidationBase] = {}

    @classmethod
    def reset(cls):
        cls._entities = {}
        cls._sources = {}
        cls._sinks = {}
        cls._transforms = {}
        cls._validations = {}

    @classmethod
    def register_entity(cls, entity: EntityBase):
        cls._entities[entity.module_idk] = entity

    @classmethod
    def register_source(cls, source: SourceBase):
        cls._sources[source.module_idk] = source
        if source.data_entity:
            cls.register_entity(source.data_entity)

    @classmethod
    def register_sink(cls, sink: SinkBase):
        cls._sinks[sink.module_idk] = sink
        if hasattr(sink, 'data_entity') and sink.data_entity:
            cls.register_entity(sink.data_entity)

    @classmethod
    def register_transform(cls, transform: TransformBase):
        cls._transforms[transform.module_idk] = transform

    @classmethod
    def register_validation(cls, validation: ValidationBase):
        cls._validations[validation.module_idk] = validation

    @classmethod
    def register_all(cls, *modules):
        """
        Register any number of modules. Automatically detects the type.
        """
        for module in modules:
            if isinstance(module, TransformBase):
                cls.register_transform(module)
            elif isinstance(module, ValidationBase):
                cls.register_validation(module)
            elif isinstance(module, SourceBase):
                cls.register_source(module)
            elif isinstance(module, (SinkBase, BinarySink)):
                cls.register_sink(module)
            elif isinstance(module, EntityBase):
                cls.register_entity(module)

    @classmethod
    def get_entity(cls, module_idk: str) -> EntityBase | None:
        return cls._entities.get(module_idk)

    @classmethod
    def get_source(cls, module_idk: str) -> SourceBase | None:
        return cls._sources.get(module_idk)

    @classmethod
    def get_sink(cls, module_idk: str) -> SinkBase | None:
        return cls._sinks.get(module_idk)

    @classmethod
    def get_transform(cls, module_idk: str) -> TransformBase | None:
        return cls._transforms.get(module_idk)


############################################################################
# Introspection helpers
############################################################################

def _get_entity_type_name(entity: EntityBase) -> str:
    return type(entity).__name__

def _get_entity_inputs(entity: EntityBase) -> list[dict]:
    """
    Reflect on an entity to determine its constructor inputs.
    Returns a list of dicts with name, type, required, default.
    """
    inputs = []
    cls = type(entity)
    try:
        sig = inspect.signature(cls.__init__)
        for name, param in sig.parameters.items():
            if name == 'self':
                continue
            param_type = 'str'
            if param.annotation != inspect.Parameter.empty:
                param_type = _annotation_to_str(param.annotation)
            has_default = param.default != inspect.Parameter.empty
            inputs.append({
                'name': name,
                'type': param_type,
                'required': not has_default,
                'default': str(param.default) if has_default else None,
            })
    except (ValueError, TypeError):
        pass
    return inputs


def _get_source_inputs(source: SourceBase) -> list[dict]:
    """Reflect on a source to get its constructor/config inputs."""
    inputs = []
    cls = type(source)
    try:
        # Get dataclass fields or init signature
        if hasattr(cls, '__dataclass_fields__'):
            for fname, fld in cls.__dataclass_fields__.items():
                if fname.startswith('_'):
                    continue
                ftype = _annotation_to_str(fld.type) if fld.type else 'Any'
                has_default = fld.default is not fld.default_factory if hasattr(fld, 'default_factory') else fld.default is not fld.default
                inputs.append({
                    'name': fname,
                    'type': ftype,
                    'required': fld.default is fld.default_factory if hasattr(fld, 'default_factory') else True,
                    'default': None,
                })
        else:
            sig = inspect.signature(cls.__init__)
            for name, param in sig.parameters.items():
                if name == 'self':
                    continue
                param_type = _annotation_to_str(param.annotation) if param.annotation != inspect.Parameter.empty else 'Any'
                has_default = param.default != inspect.Parameter.empty
                inputs.append({
                    'name': name,
                    'type': param_type,
                    'required': not has_default,
                    'default': str(param.default) if has_default else None,
                })
    except (ValueError, TypeError):
        pass
    return inputs


def _get_module_inputs(module: ModuleBase) -> list[dict]:
    """Get inputs for any module type via reflection."""
    inputs = []
    cls = type(module)
    try:
        if hasattr(cls, '__dataclass_fields__'):
            for fname, fld in cls.__dataclass_fields__.items():
                if fname.startswith('_'):
                    continue
                ftype = _annotation_to_str(fld.type) if fld.type else 'Any'
                inputs.append({
                    'name': fname,
                    'type': ftype,
                    'required': True,
                    'default': None,
                    'current_value': _safe_serialize(getattr(module, fname, None)),
                })
        else:
            sig = inspect.signature(cls.__init__)
            for name, param in sig.parameters.items():
                if name == 'self':
                    continue
                param_type = _annotation_to_str(param.annotation) if param.annotation != inspect.Parameter.empty else 'Any'
                has_default = param.default != inspect.Parameter.empty
                inputs.append({
                    'name': name,
                    'type': param_type,
                    'required': not has_default,
                    'default': str(param.default) if has_default else None,
                    'current_value': _safe_serialize(getattr(module, name, None)),
                })
    except (ValueError, TypeError):
        pass
    return inputs


def _annotation_to_str(annotation) -> str:
    """Convert a type annotation to a readable string."""
    if annotation is None:
        return 'None'
    if isinstance(annotation, str):
        return annotation
    if hasattr(annotation, '__name__'):
        return annotation.__name__
    return str(annotation)


def _safe_serialize(value: Any) -> Any:
    """Safely serialize a value to JSON-compatible format."""
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (list, tuple)):
        return [_safe_serialize(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _safe_serialize(v) for k, v in value.items()}
    # For complex objects, return type name
    return f'<{type(value).__name__}>'


def _describe_entity(entity: EntityBase) -> dict:
    """Create a detailed description of an entity."""
    info = {
        'module_idk': entity.module_idk,
        'description': entity.description,
        'type': type(entity).__name__,
        'base_type': 'EntityBase',
        'inputs': _get_entity_inputs(entity),
    }
    if isinstance(entity, DatabaseEntity):
        info['host'] = entity.host
        info['port'] = entity.port
        info['database_name'] = entity.database_name
        info['base_type'] = 'DatabaseEntity'
        info['tables'] = [
            {'name': t.name, 'schema': t.schema,
             'columns': [{'name': c.name, 'type': str(c.type)} for c in t.columns]}
            for t in entity._tables
        ]
    elif isinstance(entity, RestEntity):
        info['url'] = entity.url
        info['base_type'] = 'RestEntity'
    elif isinstance(entity, PythonEntity):
        info['base_type'] = 'PythonEntity'
    return info


def _describe_source(source: SourceBase) -> dict:
    """Create a detailed description of a source."""
    from orcha.common.modules.web import RestSource
    info = {
        'module_idk': source.module_idk,
        'description': source.description,
        'type': type(source).__name__,
        'base_type': 'SourceBase',
        'entity_idk': source.data_entity.module_idk if source.data_entity else None,
        'inputs': _get_module_inputs(source),
    }
    if isinstance(source, DatabaseSource):
        info['base_type'] = 'DatabaseSource'
        info['query'] = source.query
        info['tables'] = [t.name for t in source.tables] if source.tables else []
    elif isinstance(source, PythonSource):
        info['base_type'] = 'PythonSource'
    elif isinstance(source, RestSource):
        info['base_type'] = 'RestSource'
        info['request_type'] = source.request_type
        info['sub_path'] = source.sub_path
    return info


def _describe_sink(sink: SinkBase) -> dict:
    """Create a detailed description of a sink."""
    from orcha.common.modules.web import RestSink
    info = {
        'module_idk': sink.module_idk,
        'description': sink.description,
        'type': type(sink).__name__,
        'base_type': 'SinkBase',
        'entity_idk': sink.data_entity.module_idk if hasattr(sink, 'data_entity') and sink.data_entity else None,
        'inputs': _get_module_inputs(sink),
    }
    if isinstance(sink, DatabaseSink):
        info['base_type'] = 'DatabaseSink'
        info['table_name'] = sink.table.name
        info['if_exists'] = sink.if_exists
    elif isinstance(sink, RestSink):
        info['base_type'] = 'RestSink'
        info['request_type'] = sink.request_type
    return info


def _describe_transform(transform: TransformBase) -> dict:
    """Create a detailed description of a transform."""
    info = {
        'module_idk': transform.module_idk,
        'description': transform.description,
        'type': type(transform).__name__,
        'base_type': 'TransformBase',
        'inputs': _get_module_inputs(transform),
        'input_type': _annotation_to_str(transform.create_inputs),
    }
    return info


def _describe_validation(validation: ValidationBase) -> dict:
    """Create a detailed description of a validation."""
    info = {
        'module_idk': validation.module_idk,
        'description': validation.description,
        'type': type(validation).__name__,
        'base_type': 'ValidationBase',
        'inputs': _get_module_inputs(validation),
        'input_type': _annotation_to_str(validation.create_inputs),
    }
    return info


############################################################################
# Pydantic request/response models
############################################################################

class DeployPickleRequest(BaseModel):
    """Request to deploy a pickled object."""
    name: str
    pickle_type: str
    source_code: str
    description: str = ''
    module_idk: str | None = None
    metadata: dict | None = None
    created_by: str | None = None
    pickle_idk: str | None = None  # if set, overwrite this existing pickle


class DeployPickleTaskRequest(BaseModel):
    """Request to deploy a pickled task."""
    task_idk: str
    name: str
    description: str
    source_code: str
    schedule_sets: list[dict]
    thread_group: str = 'pickle_tasks'
    task_tags: list[str] = []
    task_metadata: dict = {}
    created_by: str | None = None
    environment_id: str | None = None  # target environment; defaults to agent's own


class CheckCodeRequest(BaseModel):
    """Request to syntax-check or test-run code."""
    source_code: str
    pickle_type: str = 'task'  # 'task' expects task_function, others expect 'result'


############################################################################
# Agent Application
############################################################################

class Agent:
    """
    The Orcha Agent provides a FastAPI server for remote
    introspection and management of a workspace environment.
    """

    def __init__(self, config: AgentConfig):
        self.config = config
        self.app = FastAPI(
            title=f'Orcha Agent - {config.environment_name}',
            description='Orcha Agent for environment introspection and management',
            version='1.0.0',
        )
        self.app.add_middleware(
            CORSMiddleware,
            allow_origins=['*'],
            allow_credentials=True,
            allow_methods=['*'],
            allow_headers=['*'],
        )
        self._setup_routes()
        self._task_runner = None
        self._tasks: dict[str, Any] = {}

    def set_task_runner(self, task_runner):
        """Link the agent to the task runner for introspection."""
        self._task_runner = task_runner

    def register_tasks(self, tasks: dict[str, Any]):
        """Register task items for introspection."""
        self._tasks = tasks

    def _setup_routes(self):
        app = self.app
        config = self.config

        @app.get('/agent/info')
        def get_info():
            """Get agent/environment information."""
            return {
                'environment_id': config.environment_id,
                'environment_name': config.environment_name,
                'status': 'running',
            }

        @app.get('/agent/secrets')
        def get_secrets():
            """
            Get the names of available secrets (NOT values).
            The UI uses these names to reference secrets when building
            task functions.
            """
            return {
                'secret_names': list(config.secrets.keys()),
                'environment_id': config.environment_id,
            }

        @app.get('/agent/modules')
        def get_modules():
            """Get all registered modules with detailed introspection."""
            return {
                'environment_id': config.environment_id,
                'entities': [
                    _describe_entity(e)
                    for e in ModuleRegistry._entities.values()
                ],
                'sources': [
                    _describe_source(s)
                    for s in ModuleRegistry._sources.values()
                ],
                'sinks': [
                    _describe_sink(s)
                    for s in ModuleRegistry._sinks.values()
                ],
                'transforms': [
                    _describe_transform(t)
                    for t in ModuleRegistry._transforms.values()
                ],
                'validations': [
                    _describe_validation(v)
                    for v in ModuleRegistry._validations.values()
                ],
            }

        @app.get('/agent/modules/entities')
        def get_entities():
            return [_describe_entity(e) for e in ModuleRegistry._entities.values()]

        @app.get('/agent/modules/sources')
        def get_sources():
            return [_describe_source(s) for s in ModuleRegistry._sources.values()]

        @app.get('/agent/modules/sinks')
        def get_sinks():
            return [_describe_sink(s) for s in ModuleRegistry._sinks.values()]

        @app.get('/agent/modules/transforms')
        def get_transforms():
            return [_describe_transform(t) for t in ModuleRegistry._transforms.values()]

        @app.get('/agent/modules/validations')
        def get_validations():
            return [_describe_validation(v) for v in ModuleRegistry._validations.values()]

        @app.get('/agent/tasks')
        def get_tasks():
            """Get all registered tasks."""
            from orcha.core.tasks import TaskItem
            all_tasks = TaskItem.get_all()
            result = []
            for task in all_tasks:
                result.append({
                    'task_idk': task.task_idk,
                    'name': task.name,
                    'description': task.description,
                    'status': task.status,
                    'thread_group': task.thread_group,
                    'task_tags': task.task_tags,
                    'task_metadata': task.task_metadata,
                    'schedule_sets': [s.to_dict() for s in task.schedule_sets],
                    'last_active': task.last_active.isoformat() if task.last_active else None,
                    'task_config': task.task_config if hasattr(task, 'task_config') else {},
                    'source': task.source,
                })
            return {
                'environment_id': config.environment_id,
                'tasks': result,
            }

        @app.get('/agent/task_runners')
        def get_task_runners():
            """Get task runner thread groups and their status."""
            if self._task_runner is None:
                return {'environment_id': config.environment_id, 'runners': []}

            runners = []
            for group_name, handler in self._task_runner.handlers.items():
                runners.append({
                    'thread_group': group_name,
                    'is_running': handler.is_running,
                    'task_count': len(handler.tasks),
                    'tasks': [t.task_idk for t in handler.tasks],
                    'thread_alive': handler.thread.is_alive() if handler.thread else False,
                })
            return {
                'environment_id': config.environment_id,
                'runners': runners,
            }

        @app.get('/agent/pickles')
        def get_pickles():
            """Get all pickled objects in the store."""
            try:
                pickles = pickle_store.get_all(environment_id=config.environment_id)
                return {
                    'environment_id': config.environment_id,
                    'pickles': [p.to_dict() for p in pickles],
                }
            except Exception as e:
                return {
                    'environment_id': config.environment_id,
                    'pickles': [],
                    'error': str(e),
                }

        @app.get('/agent/pickle/{pickle_idk}')
        def get_pickle(pickle_idk: str):
            """Get a single pickle by ID, including decrypted source for task defs."""
            try:
                info = pickle_store.get_by_idk(pickle_idk)
                if info is None:
                    raise HTTPException(status_code=404, detail=f'Pickle not found: {pickle_idk}')
                return info.to_dict()
            except HTTPException:
                raise
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))

        @app.post('/agent/pickle/deploy')
        def deploy_pickle(request: DeployPickleRequest):
            """
            Deploy a pickle to the store by compiling source code.
            The source code is compiled and the resulting object is pickled.
            """
            try:
                # Compile the source code
                local_ns: dict = {}
                exec(request.source_code, {
                    '__builtins__': __builtins__,
                    'secrets': config.secrets,
                }, local_ns)

                # The compiled code should define a variable named 'result'
                if 'result' not in local_ns:
                    raise ValueError(
                        "Source code must define a variable named 'result' "
                        "containing the object to pickle."
                    )

                obj = local_ns['result']
                idk = pickle_store.encrypt_and_store(
                    pickle_type=request.pickle_type,  # type: ignore
                    name=request.name,
                    obj=obj,
                    description=request.description,
                    module_idk=request.module_idk,
                    source_code=request.source_code,
                    metadata=request.metadata,
                    created_by=request.created_by,
                    environment_id=config.environment_id,
                    pickle_idk=request.pickle_idk,
                )
                return {'status': 'success', 'pickle_idk': idk}
            except Exception as e:
                _agent_log.add_entry(
                    actor='agent', category='deploy_pickle_error',
                    text=f'Failed to deploy pickle: {str(e)}',
                    json={'error': traceback.format_exc()}
                )
                raise HTTPException(status_code=400, detail=str(e))

        @app.post('/agent/pickle/deploy_task')
        def deploy_pickle_task(request: DeployPickleTaskRequest):
            """
            Deploy a task definition to the pickle store.
            This stores the source code and metadata only — the function
            is NOT compiled or pickled here. Instead, load_pickle_tasks()
            is called to recompile from source and register with the runner.
            This means the same code path handles both first deploy and
            every subsequent restart.
            """
            try:
                print(f'Deploying pickle task {request.name} with task_idk {request.task_idk} to environment {config.environment_id}')
                target_env = request.environment_id or config.environment_id
                idk = pickle_store.store_task_definition(
                    task_idk=request.task_idk,
                    name=request.name,
                    description=request.description,
                    source_code=request.source_code,
                    schedule_sets=request.schedule_sets,
                    thread_group=request.thread_group,
                    task_tags=request.task_tags,
                    task_metadata=request.task_metadata,
                    created_by=request.created_by,
                    environment_id=target_env,
                )
                # Reload pickle tasks for this environment so the new task
                # is picked up — same code path as startup.
                print(f'Pickle task deployed to store with idk {idk}, reloading pickle tasks for environment {target_env}')
                if self._task_runner is not None:
                    print('Task runner found, loading pickle tasks...')
                    self._task_runner.load_pickle_tasks(
                        secrets=config.secrets,
                        environment_id=config.environment_id,
                    )

                return {
                    'status': 'success',
                    'pickle_idk': idk,
                    'task_idk': request.task_idk,
                    'environment_id': target_env,
                }
            except Exception as e:
                _agent_log.add_entry(
                    actor='agent', category='deploy_task_error',
                    text=f'Failed to deploy task: {str(e)}',
                    json={'error': traceback.format_exc()}
                )
                raise HTTPException(status_code=400, detail=str(e))

        @app.post('/agent/check_code')
        def check_code(request: CheckCodeRequest):
            """
            Compile/syntax-check source code without executing the
            resulting function. Returns success or a detailed error.
            """
            try:
                # Phase 1: syntax check via compile()
                compile(request.source_code, '<editor>', 'exec')
            except SyntaxError as e:
                return {
                    'status': 'error',
                    'error_type': 'SyntaxError',
                    'line': e.lineno,
                    'offset': e.offset,
                    'message': str(e.msg),
                    'text': e.text,
                }

            try:
                # Phase 2: exec to check imports / name errors
                local_ns: dict = {}
                exec(request.source_code, {
                    '__builtins__': __builtins__,
                    'secrets': {k: '***' for k in config.secrets},
                    'registry': ModuleRegistry,
                }, local_ns)

                # Verify expected symbol exists
                if request.pickle_type == 'task':
                    if 'task_function' not in local_ns:
                        return {
                            'status': 'error',
                            'error_type': 'DefinitionError',
                            'message': "Code must define a function named 'task_function(task, run, config)'",
                        }
                    if not callable(local_ns['task_function']):
                        return {
                            'status': 'error',
                            'error_type': 'DefinitionError',
                            'message': "'task_function' is not callable",
                        }
                else:
                    if 'result' not in local_ns:
                        return {
                            'status': 'error',
                            'error_type': 'DefinitionError',
                            'message': "Code must define a variable named 'result'",
                        }

                return {'status': 'success', 'message': 'Code compiled successfully'}
            except Exception as e:
                return {
                    'status': 'error',
                    'error_type': type(e).__name__,
                    'message': str(e),
                    'traceback': traceback.format_exc(),
                }

        @app.post('/agent/test_code')
        def test_code(request: CheckCodeRequest):
            """
            Compile AND execute the source code, running the task_function
            (for tasks) with None task/run/config to test for runtime errors.
            For non-tasks, just compiles and checks the 'result' object.
            """

            try:
                compile(request.source_code, '<editor>', 'exec')
            except SyntaxError as e:
                return {
                    'status': 'error',
                    'error_type': 'SyntaxError',
                    'line': e.lineno,
                    'message': str(e.msg),
                }

            stdout_capture = io.StringIO()
            try:
                local_ns: dict = {}
                # Not a security risk - the UI editor process
                # offers the same security as the user writing
                # python directly in the environment; i.e. they can
                # already run arbitrary code.
                exec(request.source_code, {
                    '__builtins__': __builtins__,
                    'secrets': config.secrets,
                    'registry': ModuleRegistry,
                }, local_ns)

                if request.pickle_type == 'task':
                    task_func = local_ns.get('task_function')
                    if task_func is None:
                        return {
                            'status': 'error',
                            'error_type': 'DefinitionError',
                            'message': "Code must define 'task_function'",
                        }
                    with contextlib.redirect_stdout(stdout_capture):
                        dummy_run = tasks.RunItem.create_dummy()
                        task_func(None, dummy_run, {})
                else:
                    result_obj = local_ns.get('result')
                    if result_obj is None:
                        return {
                            'status': 'error',
                            'error_type': 'DefinitionError',
                            'message': "Code must define 'result'",
                        }

                captured = stdout_capture.getvalue()
                return {
                    'status': 'success',
                    'message': 'Code executed successfully',
                    'dummy_run': str(dummy_run) if request.pickle_type == 'task' else None,
                    'stdout': captured[:5000] if captured else '',
                }
            except Exception as e:
                captured = stdout_capture.getvalue()
                return {
                    'status': 'error',
                    'error_type': type(e).__name__,
                    'message': str(e),
                    'traceback': traceback.format_exc(),
                    'stdout': captured[:5000] if captured else '',
                }

        @app.delete('/agent/pickle/{pickle_idk}')
        def delete_pickle(pickle_idk: str):
            """Delete a pickle from the store."""
            try:
                pickle_store.delete(pickle_idk)
                return {'status': 'success'}
            except Exception as e:
                raise HTTPException(status_code=400, detail=str(e))

        @app.get('/agent/module_types')
        def get_module_types():
            """
            Get all available module types with their constructor signatures.
            This is used by the WYSIWYG editor to know what types are available.
            """
            type_map = {
                'entities': {
                    'PostgresEntity': _get_class_inputs(PostgresEntity),
                    'MssqlEntity': _get_class_inputs(MssqlEntity),
                    'SQLiteEntity': _get_class_inputs(SQLiteEntity),
                    'SmbEntity': _get_class_inputs(SmbEntity),
                    'RestEntity': _get_class_inputs(WebRestEntity),
                },
                'sources': {
                    'DatabaseSource': _get_class_inputs(DatabaseSource),
                    'PythonSource': _get_class_inputs(PythonSource),
                    'RestSource': _get_class_inputs(WebRestSource),
                },
                'sinks': {
                    'DatabaseSink': _get_class_inputs(DatabaseSink),
                    'RestSink': _get_class_inputs(WebRestSink),
                },
                'transforms': {
                    'TransformBase': [
                        {'name': 'module_idk', 'type': 'str', 'required': True, 'default': None},
                        {'name': 'description', 'type': 'str', 'required': True, 'default': None},
                        {'name': 'transform_func', 'type': 'Callable', 'required': True, 'default': None, 'is_code': True},
                        {'name': 'create_inputs', 'type': 'type', 'required': True, 'default': 'pd.DataFrame'},
                    ],
                },
                'validations': {
                    'ValidationBase': [
                        {'name': 'module_idk', 'type': 'str', 'required': True, 'default': None},
                        {'name': 'description', 'type': 'str', 'required': True, 'default': None},
                        {'name': 'validate_func', 'type': 'Callable', 'required': True, 'default': None, 'is_code': True},
                        {'name': 'create_inputs', 'type': 'type', 'required': True, 'default': 'bool'},
                    ],
                },
            }
            return type_map

    def start(self, in_thread: bool = True):
        """Start the agent FastAPI server."""
        _agent_log.add_entry(
            actor='agent', category='start',
            text=f'Starting agent on {self.config.host}:{self.config.port}',
            json={
                'environment_id': self.config.environment_id,
                'environment_name': self.config.environment_name,
                'port': self.config.port,
            }
        )
        if in_thread:
            thread = threading.Thread(
                target=self._run_server,
                name=f'agent_{self.config.environment_id}',
                daemon=True
            )
            thread.start()
            return thread
        else:
            self._run_server()

    def _run_server(self):
        uvicorn.run(
            self.app,
            host=self.config.host,
            port=self.config.port,
            log_level='warning',
        )


def _get_class_inputs(cls: type) -> list[dict]:
    """Get the constructor inputs for a class via reflection."""
    inputs = []
    try:
        sig = inspect.signature(cls.__init__)
        for name, param in sig.parameters.items():
            if name == 'self':
                continue
            param_type = 'str'
            if param.annotation != inspect.Parameter.empty:
                param_type = _annotation_to_str(param.annotation)
            has_default = param.default != inspect.Parameter.empty
            is_secret = name in ('password', 'user_name', 'api_key', 'client_secret')
            inputs.append({
                'name': name,
                'type': param_type,
                'required': not has_default,
                'default': str(param.default) if has_default else None,
                'is_secret': is_secret,
            })
    except (ValueError, TypeError):
        pass
    return inputs
