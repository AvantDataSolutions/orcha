from __future__ import annotations
import threading


_function_exceptions: dict[str, Exception] = {}
_timeout_remainders: dict[str, int] = {}


def expire_timeout(thread_name):
    """
    Expires the timeout remainder for a thread.
    """
    _timeout_remainders[thread_name] = 0


def store_exception(exec: Exception):
    """
    Stores an exception in the global store.
    """
    _function_exceptions[threading.current_thread().name] = exec


def clear_exception(thread_name: str | None):
    """
    Clears the exception from the global store. Any cleared
    exceptions are not returned and are lost.
    - Consider using `get_exception` to get the exception which
    will also clear it.
    """
    t_name = thread_name or threading.current_thread().name
    _function_exceptions.pop(t_name, None)


def get_exception(
        thread: threading.Thread | None,
        and_clear_exception: bool = True
    ):
    """
    Gets the exception from a function that was run with a timeout and
    clears the exception from the global store.
    """
    # Must pop the exception from the global store otherwise
    # every time this thread runs again it'll return this exception.
    t_name = thread.name if thread else threading.current_thread().name
    exec = _function_exceptions.get(t_name)
    if and_clear_exception:
        clear_exception(t_name)
    return exec


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
    # Wrap the function to catch any exceptions
    # to avoid the temp thread from crashing.
    # This exception will be stored and 'raised'
    # up to the parent thread when needed.
    def _wrap(func, *args, **kwargs):
        try:
            func(*args, **kwargs)
        except Exception as e:
            store_exception(e)

    t_name = thread_name or threading.current_thread().name
    thread = threading.Thread(
        name=t_name,
        target=_wrap,
        args=args,
        kwargs={'func': func, **kwargs}
    )
    # clear any previous exceptions
    clear_exception(t_name)
    thread.start()
    timeout_chunk = 1
    _timeout_remainders[t_name] = timeout
    while thread.is_alive() and _timeout_remainders[t_name] > 0:
        thread.join(timeout_chunk)
        _timeout_remainders[t_name] -= timeout_chunk

    if thread.is_alive():
        raise Exception(message)

    exec = get_exception(thread)

    if exec is not None:
        raise exec
