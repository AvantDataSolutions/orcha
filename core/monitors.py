from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable

from orcha.utils import email, graph_api

from orcha.utils import mqueue


class Config:
    """
    This class is used to store config and credentials for
    all monitor and alert classes. Graph API details are
    only required if using the MicrosoftEmailAlert class.
    #### Attributes
    - email_send_as: The email address that the alert will be sent as.
    - client_id: The client id for the Microsoft Graph API.
    - client_secret: The client secret for the Microsoft Graph API.
    - authority: The authority for the Microsoft Graph API.
    """
    def __init__(self,
        mqueue_config: MqueueConfig | None,
        email_send_as: str,
        client_id: str = '',
        client_secret: str = '',
        authority: str = '',
        orcha_ui_base_url: str | None = None,
        scope: list[str] = ['https://graph.microsoft.com/.default']
    ):
        self.mqueue_config = mqueue_config
        self.email_send_as = email_send_as
        self.client_id = client_id
        self.client_secret = client_secret
        self.authority = authority
        self.orcha_ui_base_url = orcha_ui_base_url
        self.scope = scope

@dataclass
class MqueueConfig:
    """
    This class is used to store config for the mqueue
    if using alerts and monitors.
    #### Attributes
    - broker_host: The host of the mqueue broker.
    - broker_bind_ip: The ip to bind the broker to.
    - broker_port: The port to bind the broker to.
    - consumer_host: The host of the mqueue consumer.
    - consumer_bind_ip: The ip to bind the consumer to.
    - consumer_port: The port to bind the consumer to.
    """
    broker_host: str
    broker_bind_ip: str
    broker_port: int
    consumer_host: str
    consumer_bind_ip: str
    consumer_port: int
    start_broker: bool


MONITOR_CONFIG: Config | None = None


@dataclass
class AlertBase(ABC):
    """
    The base class for all alert classes. Has one method
    to send alerts which must be implemented by all
    alert classes.
    """
    @abstractmethod
    def send_alert(self, message: str):
        """
        This method is used to send an alert.
        """
        pass


@dataclass
class PrintAlert(AlertBase):
    """
    This class is used to print an alert. Typically used
    for testing or on-device instances where logging to
    the console is sufficient.
    """
    def send_alert(self, message: str):
        """
        This method is used to print an alert.
        """
        print(message)


@dataclass
class MicrosoftEmailAlert(AlertBase):
    """
    This class is used to send an email alert using the
    Microsoft Graph API and sending on behalf of a user.
    NOTE: This class requires the relevant config info
    and the Mail.Send API permission for the Azure application.
    #### Attributes
    - to: The email address to send the alert to.
    - subject: The subject of the email.
    """
    to: list[str]
    subject: str

    def send_alert(self, message: str):
        """
        This method is used to send an email alert given a specific
        message for what the alert is about.
        #### Parameters
        - message: The message to send in the email. This should
        contain the details of the alert.
        """
        if not MONITOR_CONFIG:
            raise Exception('Monitor config not set.')

        token = graph_api.get_msal_token_app_only_login(
            client_id=MONITOR_CONFIG.client_id,
            client_secret=MONITOR_CONFIG.client_secret,
            authority=MONITOR_CONFIG.authority,
            scope=MONITOR_CONFIG.scope
        )
        subject = f'{self.subject}'
        email.send_email(
            token=token,
            send_as=MONITOR_CONFIG.email_send_as,
            to=self.to,
            subject=subject,
            header='Orcha Monitor Alert',
            body=message,
            importance='high'
        )

@dataclass
class MonitorBase(ABC):
    """
    The base class for all monitoring classes. Provides
    the alert (or callable) which can be called to raise
    an alert
    #### Attributes
    - alert: The alert to raise if the monitor fails.
    - monitor_name: The name of the monitor, this must be unique for each monitor.
    - message_channel: The message channel to use for the monitor.
    """

    def __init__(
            self,
            alert: AlertBase | Callable[[str], None],
            monitor_name: str,
            message_channel: mqueue.Channel | list[mqueue.Channel],
            check_function: Callable[[mqueue.Channel, mqueue.Message], None]
        ):
        self.message_channel = message_channel
        self.alert = alert

        mqueue.Consumer.register_consumer(
            consumer_name=monitor_name,
            channel=message_channel,
            callback=check_function
        )
        mqueue.Consumer.run()

    @abstractmethod
    def check(self, channel_name: str, message_string: str):
        """
        This method is used to check the status of the monitor and will be
        implemented by the subclass as required.
        """
        raise NotImplementedError('This method must be implemented by the subclass')
