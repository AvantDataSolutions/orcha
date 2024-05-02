from __future__ import annotations
import threading


_function_exceptions: dict[str, Exception] = {}
_timeout_remainders: dict[str, int] = {}


def expire_timeout_remainder(thread_name):
    """
    Expires the timeout remainder for a thread.
    """
    _timeout_remainders[thread_name] = 0


def run_function_store_exception(exec: Exception):
    _function_exceptions[threading.current_thread().name] = exec


def run_function_get_exception(thread: threading.Thread | None):
    """
    Gets the exception from a function that was run with a timeout and
    clears the exception from the global store.
    """
    # Must pop the exception from the global store otherwise
    # every time this thread runs again it'll return this exception.
    if thread is None:
        return _function_exceptions.pop(threading.current_thread().name, None)
    return _function_exceptions.pop(thread.name, None)


def run_function_with_timeout(timeout, message, func, thread_name = None, *args, **kwargs):
    """
    Runs a function with a timeout.
    #### Arguments
    - `timeout`: The time to wait before raising an exception.
    - `message`: The message to raise when the timeout is reached.
    - `func`: The function to run.
    - `thread_name`: The name of the thread to run the function in.
        Defaults to the current thread's name. This is typicalled
        used to 'impersonate' the parent thread.
    - `*args`: The arguments to pass to the function.
    - `**kwargs`: The keyword arguments to pass to the function.
    """
    t_name = thread_name or threading.current_thread().name
    thread = threading.Thread(
        name=t_name,
        target=func,
        args=args,
        kwargs=kwargs
    )
    thread.start()
    timeout_chunk = 1
    _timeout_remainders[t_name] = timeout
    while thread.is_alive() and _timeout_remainders[t_name] > 0:
        thread.join(timeout_chunk)
        _timeout_remainders[t_name] -= timeout_chunk

    if thread.is_alive():
        raise Exception(message)

    exec = run_function_get_exception(thread)

    if exec is not None:
        raise exec
