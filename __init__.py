
from datetime import datetime
from typing import Callable, Union

# When set, current_time() delegates to this instead of datetime.now(). This is
# the single clock seam for the whole package: every module calls current_time()
# (importing it via `from orcha import current_time`), and because the override
# is read from this module global at call time rather than captured at import,
# setting it here affects all callers regardless of how they imported the name.
# Intended for tests (a controllable/frozen clock) and, potentially, alternate
# time zones. Left as None in normal operation.
_time_override: Union[Callable[[], datetime], None] = None


def current_time():
    """
    The current time function used throughout the orcha package.

    Returns the override clock if one has been set via set_time(), otherwise the
    real wall-clock time. Callers should always go through this function so a
    test can make time deterministic (see set_time / reset_time).
    """
    if _time_override is not None:
        return _time_override()
    return datetime.now()


def set_time(value: Union[Callable[[], datetime], datetime]):
    """
    Override the clock returned by current_time(), typically in tests.

    ### Args
    - value: either a fixed ``datetime`` (time is frozen at that instant) or a
      zero-argument callable returning a ``datetime`` (e.g. a controllable fake
      clock that can be advanced between assertions).
    """
    global _time_override
    if isinstance(value, datetime):
        frozen = value
        _time_override = lambda: frozen #noqa: E731
    else:
        _time_override = value


def reset_time():
    """
    Clear any override set via set_time() so current_time() returns the real
    wall-clock time again. Call this in test teardown to avoid leaking a frozen
    clock into other tests.
    """
    global _time_override
    _time_override = None
