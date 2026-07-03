from __future__ import annotations
import threading
from uuid import uuid4


class TaskTimeoutException(Exception):
    """
    Raised by ``run_function_with_timeout`` when the worker thread does not
    finish within the timeout (or is manually expired). Distinct from any
    exception raised by the wrapped function itself so callers can tell a timeout
    apart from a normal failure (e.g. to fence the abandoned run).
    """
    pass


# Exception/timeout state is keyed by an explicit per-call ``token`` (not by
# thread name). Thread names are reused (the runner deliberately gives the helper
# thread the parent's name so they share the kvdb 'local' store), so keying on
# them let concurrent runs cross-contaminate each other's stored state. A token
# defaults to a fresh uuid4, and callers that need isolation (the task runner)
# pass a stable per-run token (the run_idk).
_function_exceptions: dict[str, Exception] = {}
_timeout_remainders: dict[str, int] = {}
_manually_expired_int = -9999999


def expire_timeout(token):
    """
    Expires the timeout remainder for a run/token, so ``run_function_with_timeout``
    stops waiting on it. No-ops if the token is unknown (e.g. already cleaned up).
    """
    # Guard on membership so a late call (after the token has been cleaned up)
    # doesn't re-create a leaked entry.
    if token in _timeout_remainders:
        _timeout_remainders[token] = _manually_expired_int


def store_exception(exec: Exception, token: str):
    """
    Stores an exception in the global store under the given token.
    """
    _function_exceptions[token] = exec


def clear_exception(token: str):
    """
    Clears the exception from the global store for the given token. Any cleared
    exceptions are not returned and are lost.
    - Consider using `get_exception` to get the exception which
    will also clear it.
    """
    _function_exceptions.pop(token, None)


def get_exception(token: str, and_clear_exception: bool = True):
    """
    Gets the exception stored for the given token (by a function run with a
    timeout) and, by default, clears it from the global store.
    """
    # Must pop the exception from the global store otherwise it lingers and can
    # be misread by a later run reusing the token.
    exec = _function_exceptions.get(token)
    if and_clear_exception:
        clear_exception(token)
    return exec


def run_function_with_timeout(
        timeout, message, func, token: str | None = None,
        thread_name=None, *args, **kwargs
    ):
    """
    Runs a function with a timeout.
    #### Arguments
    - `timeout`: The time to wait before raising an exception.
    - `message`: The message to raise when the timeout is reached.
    - `func`: The function to run.
    - `token`: A unique key used to isolate this call's stored exception/timeout
        state from any other concurrent call. Defaults to a fresh uuid4. The task
        runner passes the run_idk so a run's state can't be read by another run.
    - `thread_name`: The name of the thread to run the function in.
        Defaults to the current thread's name. This is typically
        used to 'impersonate' the parent thread (e.g. so the worker and its
        progress helper share the kvdb 'local' store).
    - `*args`: The arguments to pass to the function.
    - `**kwargs`: The keyword arguments to pass to the function.

    Note: on timeout the worker thread cannot be forcibly stopped -- it keeps
    running to completion. Callers that need the abandoned thread's later writes
    to be rejected should fence the underlying resource (see ``RunItem.fence``).
    """
    token = token or str(uuid4())

    # Wrap the function to catch any exceptions
    # to avoid the temp thread from crashing.
    # This exception will be stored and 'raised'
    # up to the parent thread when needed.
    def _wrap(func, *args, **kwargs):
        try:
            func(*args, **kwargs)
        except Exception as e:
            store_exception(e, token)

    t_name = thread_name or threading.current_thread().name
    thread = threading.Thread(
        name=t_name,
        target=_wrap,
        args=args,
        kwargs={'func': func, **kwargs}
    )
    # clear any previous exceptions
    clear_exception(token)
    thread.start()
    timeout_chunk = 1
    _timeout_remainders[token] = timeout
    try:
        while thread.is_alive() and _timeout_remainders[token] > 0:
            thread.join(timeout_chunk)
            _timeout_remainders[token] -= timeout_chunk

        if _timeout_remainders[token] == _manually_expired_int:
            raise TaskTimeoutException(message + ' (manually expired)')

        if thread.is_alive():
            raise TaskTimeoutException(message)

        exec = get_exception(token)

        if exec is not None:
            raise exec
    finally:
        # Keyed by a unique token per call, so unlike the old thread-name keying
        # this would otherwise grow without bound; clean it up.
        _timeout_remainders.pop(token, None)
