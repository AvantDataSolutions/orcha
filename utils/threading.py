from __future__ import annotations
import threading
import uuid


function_exceptions: dict[str, Exception] = {}

def run_function_store_exception(exec: Exception):
    function_exceptions[threading.current_thread().name] = exec


def run_function_get_exception(thread: threading.Thread | None):
    if thread is None:
        return function_exceptions.get(threading.current_thread().name, None)
    return function_exceptions.get(thread.name, None)


def run_function_clear_exception(thread: threading.Thread | None):
    if thread is None:
        function_exceptions.pop(threading.current_thread().name, None)
    else:
        function_exceptions.pop(thread.name, None)


def run_function_with_timeout(timeout, message, func, *args, **kwargs):
    thread = threading.Thread(
        name=f'TimeoutThread_{func.__name__}_{str(uuid.uuid4())}',
        target=func,
        args=args,
        kwargs=kwargs
    )
    thread.start()
    thread.join(timeout)

    if thread.is_alive():
        raise Exception(message)

    exec = run_function_get_exception(thread)

    if exec is not None:
        raise exec
