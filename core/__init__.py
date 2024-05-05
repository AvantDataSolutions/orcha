from orcha.core import tasks, scheduler, monitors
from orcha.utils.log import LogManager

_ORCHA_SCHEMA = 'orcha'

def initialise(
        orcha_user: str, orcha_pass: str,
        orcha_server: str, orcha_db: str,
        application_name: str,
        monitor_config: monitors.Config | None = None
    ):
    """
    This function must be called before any other functions in the orcha package.
    This function does the following:
    - Sets up the sqlalchemy database connection
    - Sets up the logging database
    - (Optional) Sets up the monitor config if using monitors and alerts
    #### Returns
    - LogManager: The orcha log manager to be used for custom logging
    """
    tasks._setup_sqlalchemy(
        orcha_user=orcha_user,
        orcha_pass=orcha_pass,
        orcha_server=orcha_server,
        orcha_db=orcha_db,
        orcha_schema=_ORCHA_SCHEMA,
        application_name=f'{application_name}_tasks'
    )

    scheduler._setup_sqlalchemy(
        orcha_user=orcha_user,
        orcha_pass=orcha_pass,
        orcha_server=orcha_server,
        orcha_db=orcha_db,
        orcha_schema=_ORCHA_SCHEMA,
        application_name=f'{application_name}_scheduler'
    )

    LogManager._setup_sqlalchemy(
        user=orcha_user,
        passwd=orcha_pass,
        server=orcha_server,
        db=orcha_db,
        application_name=f'{application_name}_logs'
    )

    monitors.MONITOR_CONFIG = monitor_config

    lm = LogManager('orcha')
    lm.add_entry('orcha', 'info', 'Initialised orcha', {})
    return LogManager('orcha_custom')


