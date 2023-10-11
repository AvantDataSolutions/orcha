from __future__ import annotations
import inspect
import os
import sys


def check_credentials():
    """
    Raises an exception if any of the credentials are missing
    else returns True
    """
    for name, value in inspect.getmembers(sys.modules[__name__]):
        if name.startswith('__') or name == 'check_credentials':
            continue
        if value == '':
            raise ValueError(f'Missing credential: {name}')
    return True


ORCHA_CORE_USER = os.environ['ORCHA_CORE_USER']
ORCHA_CORE_PASSWORD = os.environ['ORCHA_CORE_PASSWORD']
ORCHA_CORE_SERVER = os.environ['ORCHA_CORE_SERVER']
ORCHA_CORE_DB = os.environ['ORCHA_CORE_DB']

SES_SMTP_USERNAME = os.getenv('SES_SMTP_USERNAME')
SES_SMTP_PASSWORD = os.getenv('SES_SMTP_PASSWORD')

AUTH_PERMITTED_DOMAINS = os.getenv('AUTH_PERMITTED_DOMAINS')
O365_IS_ENABLED = os.getenv('O365_IS_ENABLED')
O365_PERMITTED_DOMAINS = os.getenv('O365_PERMITTED_DOMAINS')
