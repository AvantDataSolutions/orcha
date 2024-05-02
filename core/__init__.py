from orcha.core import tasks, scheduler
from orcha.utils.log import LogManager

_ORCHA_SCHEMA = 'orcha'

def initialise(
        orcha_user: str, orcha_pass: str,
        orcha_server: str, orcha_db: str
    ):
    """
    This function must be called before any other functions in the orcha package.
    This function does the following:
    - Sets up the sqlalchemy database connection
    - Sets up the logging database
    #### Returns
    - LogManager: The orcha log manager to be used for custom logging
    """
    tasks._setup_sqlalchemy(
        orcha_user=orcha_user,
        orcha_pass=orcha_pass,
        orcha_server=orcha_server,
        orcha_db=orcha_db,
        orcha_schema=_ORCHA_SCHEMA
    )

    scheduler._setup_sqlalchemy(
        orcha_user=orcha_user,
        orcha_pass=orcha_pass,
        orcha_server=orcha_server,
        orcha_db=orcha_db,
        orcha_schema=_ORCHA_SCHEMA
    )

    LogManager._setup_sqlalchemy(
        user=orcha_user,
        passwd=orcha_pass,
        server=orcha_server,
        db=orcha_db
    )

    lm = LogManager('orcha')
    lm.add_entry('orcha', 'info', 'Initialised orcha', {})
    return LogManager('orcha_custom')

