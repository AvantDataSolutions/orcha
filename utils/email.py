from dataclasses import dataclass
from datetime import datetime as dt
from email.message import EmailMessage
from smtplib import SMTP_SSL
from time import sleep
import uuid

from sqlalchemy import Column
from sqlalchemy.types import DateTime, String, Integer
from sqlalchemy.sql import text as sql

from utils.log import LogManager
from utils.sqlalchemy import sqlalchemy_scaffold, sqlalchemy_build

from orcha.core.credentials import *

(Base, engine, Session, CUR_SCHEMA) = sqlalchemy_scaffold('logs')

class EmailSendRecord(Base):
    __tablename__ = 'email_send_records'
    email_key = Column(String, primary_key=True)
    email_to = Column(String)
    email_sent = Column(DateTime)


sqlalchemy_build(Base, engine, CUR_SCHEMA)


class EmailSendResult():
    SUCCESS = 'SUCCESS'
    FAILED = 'FAILED'
    RATE_LIMITED = 'RATE_LIMITED'
    NO_CREDS = 'NO_CREDS'


@dataclass
class EmailTemplate():
    name: str
    template: str

    def populate(self, title: str, content: str, footer: str):
        full = self.template.replace('{{title}}', title)
        full = full.replace('{{content}}', content)
        full = full.replace('{{footer}}', footer)
        return full


class EmailRateLimiter():


    @dataclass
    class Limit():
        seconds: int
        count: int

    @dataclass
    class EmailSendItem():
        email_key: str
        email_to: str
        email_sent: dt

    LIMITS = [
        Limit(seconds=5, count=1),
        Limit(seconds=20, count=2),
        Limit(seconds=120, count=5)
    ]

    log = LogManager('email_utils')

    def __init__(self):
        self.cache: list[EmailRateLimiter.EmailSendItem] = []
        # load emails from the database in the last 1 minute
        with Session.begin() as tx:
            sql_data = tx.execute(sql(f'''
                SELECT email_key, email_to, email_sent
                FROM {CUR_SCHEMA}.email_send_records
                WHERE email_sent > CURRENT_TIMESTAMP - INTERVAL '10m'
            ''')).all()

    def add(self, to: str):
        self.cache.append(EmailRateLimiter.EmailSendItem(
            email_key=str(uuid.uuid4()),
            email_to=to,
            email_sent=dt.utcnow()
        ))
        with Session.begin() as tx:
            tx.add(EmailSendRecord(
                email_key=self.cache[-1].email_key,
                email_to=self.cache[-1].email_to,
                email_sent=self.cache[-1].email_sent
            ))

    def can_send(self, to: str):
        for limit in self.LIMITS:
            email_limit = [
                r for r in self.cache
                if r.email_to == to
                    and (dt.utcnow() - r.email_sent).total_seconds() < limit.seconds
            ]
            if len(email_limit) >= limit.count:
                return False
        return True

    def send(self, to: str, subject: str, html_body: str, cc = '', footer_content = ''):
        email_json = {'email': to, 'subject': subject, 'body': html_body}
        if not SES_SMTP_PASSWORD or not SES_SMTP_USERNAME:
            # If we dont have SES details, do nothing
            return EmailSendResult.NO_CREDS
        if not RATE_LIMITER.can_send(to):
            sleep(5)
            if not RATE_LIMITER.can_send(to):
                self.log.add_entry('send', 'rate_limited', email_json)
                return EmailSendResult.RATE_LIMITED

        full_html = _base_template.populate(
            title=subject,
            content=html_body,
            footer=footer_content
        )

        msg = EmailMessage()
        msg['From'] = 'noreply@avantdata.com'
        msg['To'] = to
        msg['Subject'] = subject
        msg.set_content(full_html, subtype='html')
        s = SMTP_SSL('email-smtp.ap-southeast-2.amazonaws.com',465)
        s.login(SES_SMTP_USERNAME, SES_SMTP_PASSWORD)
        s.send_message(msg)
        s.quit()

        self.log.add_entry('send', 'rate_limited', email_json)
        RATE_LIMITER.add(to)
        return EmailSendResult.SUCCESS


def send_email(to: str, subject: str, html_body: str, cc = '', footer_content = ''):
    """
        Simple wraper around RATE_LIMITER.send()
    """
    return RATE_LIMITER.send(to, subject, html_body, cc, footer_content)


def get_unsub_email_link(email_key: str):
    unsub_url = f'''{os.getenv('SERVER_ROOT_URL')}/api/notifications/
        unsubscribe?email_key={email_key}
    '''.replace('\n', '').replace(' ', '')
    return f'\n<a href="{unsub_url}">Unsubscribe</a>'


_base_template = EmailTemplate(
    name='base_email',
    template='''
        <!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <style>
                .container {
                    font-family: system-ui,-apple-system,"Segoe UI",Roboto,"Helvetica Neue",Arial,"Noto Sans","Liberation Sans",sans-serif,"Apple Color Emoji","Segoe UI Emoji","Segoe UI Symbol","Noto Color Emoji";
                    display: flex;
                    flex-direction: column;
                    margin-right: auto;
                    margin-left: auto;
                    margin-left: 20px;
                    margin-right: 20px;
                }
                @media only screen and (max-width: 600px) {
                    .container {
                        margin-left: 0;
                        margin-right: 0;
                    }
                }
                .row {
                    display: flex;
                    flex-wrap: wrap;
                    justify-content: center;
                    align-items: center;
                }
                .is-top-spacer {
                    height: 50px;
                    background-color: rgb(250, 250, 250);
                    border-bottom: 5px solid #007bff;
                    justify-content: end;
                    width: 100%;
                }
                .is-header {
                    justify-content: center;
                    background-color: rgb(230,230,250);
                    padding-top: 1rem;
                    padding-bottom: 1rem;
                    font-weight: 400;
                    font-size: 1.5rem;
                    padding: 0.5rem;
                    padding-bottom: 0;
                }
                .is-header .general {
                    opacity: 0.5;
                    font-weight: 400;
                    font-size: 1rem;
                    padding: 0.5rem;
                    padding-bottom: 0;
                }
                .is-header .title {
                    font-weight: 400;
                    font-size: 1.5rem;;
                    padding: 0.5rem;
                    padding-top: 0;
                }
                .is-content-outer {
                    background-color: rgb(230,230,250);
                }
                .is-content-inner {
                    background-color: white;
                    margin-left: 1rem;
                    margin-right: 1rem;
                    padding: 1rem;
                    width: 100%;
                }
                .is-content-footer {
                    display: flex;
                    justify-content: space-between;
                    background-color: rgb(230,230,250);
                    padding-top: 0.5rem;
                    padding-bottom: 0.5rem;
                    width: 100%;
                }
                .col {
                    display: flex;
                    padding-right: 15px;
                    padding-left: 15px;
                }
                .logo {
                    padding-right: 1rem;
                    height: 2rem;
                    opacity: 0.5;
                }
            </style>

        </head>
        <body>
            <div class="container">
                <div class="row is-top-spacer">
                    <img class="logo"
                        src="https://tianqi-production-input.avantdata.com/auth/static/avant2-xs.png">
                </div>
                <div class="row is-header">
                    <div class="general">
                        Tianqi Production Apps
                    </div>
                </div>
                <div class="row is-header">
                    <div class="title">
                        {{title}}
                    </div>
                </div>
                <div class="row is-content-outer">
                    <div class="is-content-inner">
                        {{content}}
                    </div>
                </div>
                <div class="row is-content-footer">
                    <div class="col">
                        {{footer}}
                    </div>
                    <div class="col">
                        Avant Data Solutions
                    </div>
                </div>
            </div>
        </body>
        </html>
        '''
)


RATE_LIMITER = EmailRateLimiter()