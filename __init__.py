
from datetime import datetime


def current_time():
    """
    This is the current time function that is used throughout the orcha package.
    This function can be overridden to provide a custom time function, typically
    for testing purposes or for using a different time zone.
    """
    return datetime.now()