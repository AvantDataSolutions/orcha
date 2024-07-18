from __future__ import annotations

from orcha.core import monitors, scheduler, tasks
from orcha.utils.log import LogManager
from orcha.utils.mqueue import Broker, Consumer, Producer

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

    # Do the monitor config first, so the mqueue is set up correctly
    # for the tasks and schedulers to use
    monitors.MONITOR_CONFIG = monitor_config
    if monitor_config:
        # remove any leading or trailing slashes from urls
        if monitor_config.orcha_ui_base_url:
            monitor_config.orcha_ui_base_url = monitor_config.orcha_ui_base_url.strip('/')
        if monitor_config.mqueue_config:
            monitor_config.mqueue_config.broker_host = monitor_config.mqueue_config.broker_host.strip('/')
            monitor_config.mqueue_config.consumer_host = monitor_config.mqueue_config.consumer_host.strip('/')
        if monitor_config.mqueue_config:
            if monitor_config.mqueue_config.broker_bind_ip:
                Broker.setup(
                    mqueue_pg_host=orcha_server,
                    mqeue_pg_port=5432,
                    mqueue_pg_name=orcha_db,
                    mqueue_pg_user=orcha_user,
                    mqueue_pg_pass=orcha_pass,
                )
                if monitor_config.mqueue_config.start_broker:
                    Broker.run(
                        bind_ip=monitor_config.mqueue_config.broker_bind_ip,
                        bind_port=monitor_config.mqueue_config.broker_port,
                    )
            Consumer.setup(
                broker_host=monitor_config.mqueue_config.broker_host,
                broker_port=monitor_config.mqueue_config.broker_port,
                consumer_host=monitor_config.mqueue_config.consumer_host,
                consumer_port=monitor_config.mqueue_config.consumer_port
            )
            Producer.default_broker_host = monitor_config.mqueue_config.broker_host
            Producer.default_broker_port = monitor_config.mqueue_config.broker_port
        if not Consumer.broker_host:
            raise Exception('mqueue must be configured if using monitors and alerts')

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

    lm = LogManager('orcha')
    lm.add_entry('orcha', 'info', 'Initialised orcha', {})
    return LogManager('orcha_custom')


