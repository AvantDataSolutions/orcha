from __future__ import annotations

from orcha import migrations
from orcha.core import database, monitors, scheduler, tasks
from orcha.utils.log import LogManager
from orcha.utils.mqueue import Broker, Consumer, Producer
from orcha.utils import kvdb

_ORCHA_SCHEMA = 'orcha'

def initialise(
        orcha_user: str, orcha_pass: str,
        orcha_server: str, orcha_db: str,
        application_name: str,
        monitor_config: monitors.Config | None = None,
        kvdb_postgres_user: str | None = None,
        kvdb_postgres_pass: str | None = None,
        kvdb_postgres_server: str | None = None,
        kvdb_postgres_db: str | None = None,
        kvdb_postgres_schema: str | None = None,
        run_migrations: bool = True,
    ):
    """
    This function must be called before any other functions in the orcha package.
    This function does the following:
    - Applies the core database migrations (unless `run_migrations` is False)
    - Configures the shared sqlalchemy connection used by tasks, scheduler and logging
    - Sets up the kvdb store
    - (Optional) Sets up the monitor config if using monitors and alerts
    #### Returns
    - LogManager: The orcha log manager to be used for custom logging
    """

    # Apply the core migrations first so every table/schema the rest of
    # initialisation (and the running code) relies on already exists. The
    # migrations ship with orcha core, so they stay in sync with the table
    # definitions in the code. This is a no-op when the database is already at
    # head. Set run_migrations=False to manage migrations out-of-band instead.
    if run_migrations:
        migrations.upgrade_to_head(
            user=orcha_user,
            passwd=orcha_pass,
            server=orcha_server,
            db=orcha_db,
            application_name=f'{application_name}_migrations',
        )

    # Configure the shared engine + session_maker once. The ORM record classes
    # in tasks/scheduler/log are already mapped (at import time) onto the tables
    # in orcha.core.tables, so binding the shared connection here is all that is
    # needed to make them usable; logging works as soon as this returns.
    database.configure(
        user=orcha_user,
        passwd=orcha_pass,
        server=orcha_server,
        db=orcha_db,
        application_name=application_name,
    )

    lm = LogManager('orcha')

    kvdb.initialise(
        postgres_user=kvdb_postgres_user or orcha_user,
        postgres_pass=kvdb_postgres_pass or orcha_pass,
        postgres_server=kvdb_postgres_server or orcha_server,
        postgres_db=kvdb_postgres_db or orcha_db,
        postgres_schema=kvdb_postgres_schema or _ORCHA_SCHEMA,
    )

    # Do the monitor config first, so the mqueue is set up correctly
    # for the tasks and schedulers to use
    log_json = {}
    if monitor_config:
        log_json = {
            'send_as': monitor_config.email_send_as,
            'orcha_ui_base_url': monitor_config.orcha_ui_base_url,
        }
        if monitor_config.mqueue_config:
            if monitor_config.mqueue_config.broker:
                broker = monitor_config.mqueue_config.broker
                log_json['mqueue_broker_bind_ip'] = broker.broker_bind_ip
                log_json['mqueue_broker_port'] = broker.broker_port
            else:
                log_json['mqueue_broker'] = 'No broker config'

            if monitor_config.mqueue_config.consumer:
                consumer = monitor_config.mqueue_config.consumer
                log_json['mqueue_consumer_bind_ip'] = consumer.consumer_bind_ip
                log_json['mqueue_consumer_port'] = consumer.consumer_port
                log_json['mqueue_consumer_host'] = consumer.consumer_host
            else:
                log_json['mqueue_consumer'] = 'No consumer config'

            if monitor_config.mqueue_config.producer:
                producer = monitor_config.mqueue_config.producer
                log_json['mqueue_producer_broker_host'] = producer.broker_host
                log_json['mqueue_producer_broker_port'] = producer.broker_port
            else:
                log_json['mqueue_producer'] = 'No producer config'

    lm.add_entry(
        actor='orcha_core',
        category='startup',
        text='Configuring monitors' if monitor_config else 'No monitor config provided',
        json=log_json
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
            lm.add_entry(
                actor='orcha_core',
                category='error',
                text='Mqueue broker host required when using monitors and alerts',
                json={}
            )
            raise Exception('mqueue must be configured if using monitors and alerts')

    lm.add_entry('orcha_core', 'startup', 'Orcha initialisation complete', {})
    return LogManager('orcha_custom')