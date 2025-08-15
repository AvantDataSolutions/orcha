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

    # Initialise the log manager first for use later on
    LogManager._setup_sqlalchemy(
        user=orcha_user,
        passwd=orcha_pass,
        server=orcha_server,
        db=orcha_db,
        application_name=f'{application_name}_logs'
    )

    lm = LogManager('orcha')

    # Do the monitor config first, so the mqueue is set up correctly
    # for the tasks and schedulers to use
    lm.add_entry(
        actor='orcha_core',
        category='startup',
        text='Configuring monitors',
        json={
            'monitor_config': monitor_config
        }
    )
    monitors.MONITOR_CONFIG = monitor_config
    if monitor_config:
        # remove any leading or trailing slashes from urls
        if monitor_config.orcha_ui_base_url:
            monitor_config.orcha_ui_base_url = monitor_config.orcha_ui_base_url.strip('/')
        if monitor_config.mqueue_config:
            if monitor_config.mqueue_config.broker:
                broker_ip = monitor_config.mqueue_config.broker.broker_bind_ip.strip('/')
                broker_port = monitor_config.mqueue_config.broker.broker_port
                Broker.setup(
                    mqueue_pg_host=orcha_server,
                    mqeue_pg_port=5432,
                    mqueue_pg_name=orcha_db,
                    mqueue_pg_user=orcha_user,
                    mqueue_pg_pass=orcha_pass,
                )
                Broker.run(
                    bind_ip=broker_ip,
                    bind_port=broker_port
                )
            if monitor_config.mqueue_config.consumer:
                consumer_ip = monitor_config.mqueue_config.consumer.consumer_bind_ip.strip('/')
                consumer_port = monitor_config.mqueue_config.consumer.consumer_port
                consumer_host = monitor_config.mqueue_config.consumer.consumer_host.strip('/')
                consumer_broker_host = monitor_config.mqueue_config.consumer.broker_host.strip('/')
                consumer_broker_port = monitor_config.mqueue_config.consumer.broker_port
            Consumer.setup(
                broker_host=consumer_broker_host,
                broker_port=consumer_broker_port,
                consumer_host=consumer_host,
                consumer_port=consumer_port,
                consumer_bind_ip=consumer_ip
            )
            if monitor_config.mqueue_config.producer:
                Producer.default_broker_host = monitor_config.mqueue_config.producer.broker_host
                Producer.default_broker_port = monitor_config.mqueue_config.producer.broker_port

        if not Consumer.broker_host:
            raise Exception('mqueue must be configured if using monitors and alerts')

    lm.add_entry('orcha_core', 'startup', 'Setting up tasks sqlalchemy', {})
    try:
        tasks._setup_sqlalchemy(
            orcha_user=orcha_user,
            orcha_pass=orcha_pass,
            orcha_server=orcha_server,
            orcha_db=orcha_db,
            orcha_schema=_ORCHA_SCHEMA,
            application_name=f'{application_name}_tasks'
        )
    except Exception as e:
        lm.add_entry('orcha_core', 'error', 'Error setting up tasks sqlalchemy', {
            'exception_type': type(e).__name__,
            'exception': str(e)
        })
        # still raise the exception as we want to fail-fast and not run properly
        # if something is wrong
        raise e

    lm.add_entry('orcha_core', 'startup', 'Setting up scheduler sqlalchemy', {})
    try:
        scheduler._setup_sqlalchemy(
            orcha_user=orcha_user,
            orcha_pass=orcha_pass,
            orcha_server=orcha_server,
            orcha_db=orcha_db,
            orcha_schema=_ORCHA_SCHEMA,
            application_name=f'{application_name}_scheduler'
        )
    except Exception as e:
        lm.add_entry('orcha_core', 'error', 'Error setting up scheduler sqlalchemy', {
            'exception_type': type(e).__name__,
            'exception': str(e)
        })
        # same as above logic
        raise e

    lm.add_entry('orcha_core', 'startup', 'Orcha initialisation complete', {})
    return LogManager('orcha_custom')


