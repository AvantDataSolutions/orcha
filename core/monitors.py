from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable

from orcha.utils import email, graph_api


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
        email_send_as: str,
        client_id: str = '',
        client_secret: str = '',
        authority: str = '',
        scope: list[str] = ['https://graph.microsoft.com/.default']
    ):
        self.email_send_as = email_send_as
        self.client_id = client_id
        self.client_secret = client_secret
        self.authority = authority
        self.scope = scope


MONITOR_CONFIG: Config | None = None


@dataclass
class AlertBase(ABC):
    """
    The base class for all alert classes. Has one method
    to send alerts which must be implemented by all
    alert classes.
    """
    @abstractmethod
    def send_alert(self, message: str, *args, **kwargs):
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
    def send_alert(self, message: str, *args, **kwargs):
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
    to: str
    subject: str

    def send_alert(self, message: str, *args, **kwargs):
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
    """
    alert: AlertBase | Callable[[str], None]

    @abstractmethod
    def check(self, *args, **kwargs):
        """
        This method is used to check the status of the monitor and will be
        implemented by the subclass as required.
        """
        raise NotImplementedError('This method must be implemented by the subclass')
