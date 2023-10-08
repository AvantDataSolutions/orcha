from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime as dt
from enum import IntEnum
from uuid import uuid4

from sqlalchemy import Column, DateTime
from sqlalchemy import Enum as SQL_Enum
from sqlalchemy import String, exc
from sqlalchemy.dialects.postgresql import JSON as PG_JSON
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.sql import text as sql

from utils import mqueue, pydantic_utils
from utils.sqlalchemy import sqlalchemy_build, sqlalchemy_scaffold

from ..credentials import *

print('Loading page:',__name__)


(Base, engine, Session, CUR_SCHEMA) = sqlalchemy_scaffold('automations')


class EmailFrequency(IntEnum):
    HOURLY = 100
    DAILY = 200
    WEEKLY = 300
    FORTNIGHTLY = 400
    MONTHLY = 500


class Status(IntEnum):
    ENABLED = 100
    DISABLED = 200


class ScheduledCategory():
    ADMIN = 'Admin'
    NOTIFICATION = 'Notification'
    TASK = 'Task'


@dataclass
class TriggerDef():
    channel_name: str
    consumer_name: str


class GroupRecord(Base):
    __tablename__ = 'notification_groups'
    group_key = Column(PG_UUID(as_uuid=True), primary_key=True)
    group_name = Column(String)
    group_owner_email = Column(String)
    group_visible_description = Column(String)
    group_status = Column(SQL_Enum(Status))


@dataclass
class GroupItem():
    group_key: str
    group_name: str
    group_owner_email: str
    group_visible_description: str
    group_status: Status

    @classmethod
    def from_sql(cls, rows: list):
        return [
            cls(**row)
            for row in rows
        ]


class GroupMembershipItem(pydantic_utils.BaseModelExtended):
    email_key: str
    email_status: str
    email_address: str
    group_key: str
    group_name: str


class EmailRecord(Base):
    __tablename__ = 'notification_emails'
    email_key = Column(PG_UUID(as_uuid=True), primary_key=True)
    email_group_key = Column(PG_UUID(as_uuid=True))
    email_address = Column(String)
    email_name = Column(String)
    email_frequency = Column(SQL_Enum(EmailFrequency))
    email_status = Column(SQL_Enum(Status))


class EmailItem(pydantic_utils.BaseModelExtended):
    group_owner_email: str
    group_visible_description: str
    email_address: str
    email_name: str
    email_key: str


class AutomationTemplateRecord(Base):
    __tablename__ = 'automation_templates'
    template_key = Column(String, primary_key=True)
    template_name = Column(String)
    template_description = Column(String)
    template_schedules = Column(PG_JSON)
    template_triggers = Column(PG_JSON)
    template_category = Column(String)
    template_status = Column(String)
    template_trigger_channels = Column(PG_JSON)


class AutomationTemplateItem(pydantic_utils.BaseModelExtended):
    template_key: str
    template_name: str
    template_description: str
    template_schedules: list[str]
    template_triggers: list[str]
    template_category: str
    template_status: str
    template_trigger_channels: list[str]

    @classmethod
    def get_all(cls):
        with Session.begin() as tx:
            tpls = tx.execute(sql(f'''
                SELECT
                    template_key, template_name, template_description,
                    template_schedules, template_triggers, template_category,
                    CASE WHEN ng.group_status = 'DISABLED' THEN 'Disabled' ELSE 'Active' END AS template_status,
                    template_trigger_channels
                FROM automations.automation_templates as nt
                LEFT JOIN automations.notification_groups AS ng ON ng.group_name = nt.template_name
                ORDER BY template_name DESC
            ''')).all()
            return cls.from_sql(tpls)

    def save(self):
        with Session.begin() as tx:
            tx.merge(AutomationTemplateRecord(**self.dict()))
            return True


class AutomationRunRecord(Base):
    __tablename__ = 'automation_runs'
    run_key = Column(PG_UUID(as_uuid=True), primary_key=True)
    run_name = Column(String)
    run_task_name = Column(String)
    run_start_utctime = Column(DateTime)
    run_end_utctime = Column(DateTime)
    run_result = Column(String)
    run_exception = Column(String)

    @staticmethod
    def start_run(name: str, task_name: str):
        with Session.begin() as session:
            run_key = uuid4()
            run = AutomationRunRecord(
                run_key=run_key,
                run_name=name,
                run_task_name=task_name,
                run_start_utctime=dt.utcnow(),
            )
            session.add(run)
            return run

    def end_run(self, result: str | None, exception: str | None):
        self.run_end_utctime = dt.utcnow()
        self.run_result = result
        self.run_exception = exception
        with Session.begin() as session:
            session.merge(self)
            session.commit()
        return True


# Create the schema and tables if needed
sqlalchemy_build(Base, engine, CUR_SCHEMA)


def trigger_automation(template_key: str):
    automs = AutomationTemplateItem.get_all()
    autom = None
    for a in automs:
        if a.template_key == template_key:
            autom = a

    if autom:
        send_results = []
        # Send the request to the queue
        for channel in autom.template_trigger_channels:
            print('sending to channel:', channel)
            send_results.append(mqueue.send_message(
                channel,
                'not_used_for_manual_triggers'
            ))
        return send_results


def get_groups():
    with Session.begin() as tx:
        data = tx.execute(sql(f'''
            SELECT
                group_key::text, group_name, group_owner_email,
                group_visible_description, group_status
            FROM automations.notification_groups
            ORDER BY group_key DESC
        ''')).all()
    return GroupItem.from_sql(data)


def get_group(name: str):
    groups = get_groups()
    for group in groups:
        if group.group_name == name:
            return group
    return None


def toggle_group_status(key: str):
    groups = get_groups()
    for group in groups:
        if group.group_key == key:
            # Toggle the status
            if group.group_status == Status.ENABLED.name:
                group.group_status = Status.DISABLED
            else:
                group.group_status = Status.ENABLED
            # Update the database
            update_group(
                group.group_key,
                group.group_name,
                group.group_owner_email,
                group.group_visible_description,
                group.group_status
            )
    return None


def upsert_group(name: str, email: str, desc: str):
    group = get_group(name)
    if group:
        update_group(group.group_key, name, email, desc, Status.ENABLED)
    else:
        add_group(name, email, desc)


def add_group(name: str, email: str, desc: str):
    update_group(uuid4().hex, name, email, desc, Status.ENABLED)


def update_group(group_key: str, name: str, email: str,
        desc: str, status: Status
    ):
    with Session.begin() as db:
        db.merge(GroupRecord(
            group_key = group_key,
            group_name = name,
            group_owner_email = email,
            group_visible_description = desc,
            group_status = status
        ))


def get_emails(group_key: str):
    with Session.begin() as tx:
        data = tx.execute(sql(f'''
            SELECT
                email_key::text, email_group_key::text, email_address,
                email_name, email_frequency, email_status
            FROM automations.notification_emails
            WHERE email_group_key = :tar_key
            ORDER BY email_key DESC
        ''').bindparams(tar_key=group_key)).all()
    return data


def get_emails_for_group(group_name: str):
    with Session.begin() as tx:
        data = tx.execute(sql(f'''
            SELECT
                gr.group_owner_email, gr.group_visible_description,
                em.email_address, em.email_name, em.email_key::text
            FROM automations.notification_groups gr
            LEFT JOIN automations.notification_emails em
            ON em.email_group_key = gr.group_key
            WHERE
                gr.group_name = :group_name
                AND em.email_status = 'ENABLED'
                AND gr.group_status = 'ENABLED'
        ''').bindparams(group_name=group_name)).all()
    return EmailItem.from_sql(data)


def add_email(group_key: str, name: str, address: str):
    update_email(uuid4().hex, group_key, name, address, Status.ENABLED)


def disable_email(email_key: str):
    try:
        with Session.begin() as db:
            db.merge(EmailRecord(
                email_key = email_key,
                email_status = Status.DISABLED
            ))
        return True, 'Unsubscribed'
    except exc.SQLAlchemyError as e:
        return False, e


def update_email(email_key: str, group_key: str, name: str,
        address: str, status: Status
    ):
    with Session.begin() as db:
        db.merge(EmailRecord(
            email_key = email_key,
            email_group_key = group_key,
            email_name = name,
            email_address = address,
            email_frequency = EmailFrequency.DAILY,
            email_status = status
        ))


def get_groups_for_user(user_email: str):
    # get all groups a user is a member of
    with Session.begin() as tx:
        data = tx.execute(sql(f'''
            SELECT
                em.email_key::text, em.email_status,
                em.email_address,
                gr.group_key::text, gr.group_name

            FROM automations.notification_groups gr
            LEFT JOIN automations.notification_emails em
            ON em.email_group_key = gr.group_key
            WHERE
                LOWER(em.email_address) = LOWER(:user_email)
        ''').bindparams(user_email=user_email)).all()

    return GroupMembershipItem.from_sql(data)