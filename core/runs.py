
#==============================================================================
# OUTDATED - CODE MOVED TO tasks.py
#==============================================================================

# from __future__ import annotations

# from dataclasses import dataclass
# from datetime import datetime as dt
# from typing import Any
# from uuid import uuid4

# from sqlalchemy import Column, DateTime, String
# from sqlalchemy.dialects.postgresql import JSON as PG_JSON

# from orcha.utils.sqlalchemy import (get_latest_versions, postgres_build,
#                                     postgres_scaffold)

# print('Loading dh:',__name__)

# CUR_SCHEMA = 'orcha'
# Base, engine, Session = postgres_scaffold(CUR_SCHEMA)


# class RunStatus():
#     QUEUED = 'queued'
#     RUNNING = 'running'
#     SUCCESS = 'success'
#     WARN = 'warn'
#     FAILED = 'failed'
#     CANCELLED = 'cancelled'

#     def __init__(self, status: str, text: str) -> None:
#         self.status = status
#         self.text = text


# class RunRecord(Base):
#     __tablename__ = 'runs'

#     run_idk = Column(String, primary_key=True)
#     version = Column(DateTime(timezone=False), primary_key=True)
#     task_idf = Column(String)
#     scheduled_time = Column(DateTime(timezone=False))
#     start_time = Column(DateTime(timezone=False))
#     end_time = Column(DateTime(timezone=False))
#     last_active = Column(DateTime(timezone=False))
#     status = Column(String)
#     output = Column(PG_JSON)


# @dataclass
# class RunItem():
#     run_idk: str
#     version: dt
#     task_idf: str
#     scheduled_time: dt
#     start_time: dt | None
#     end_time: dt | None
#     last_active: dt | None
#     status: str
#     output: dict | None = None
#     _task: TaskItem | None = None


#     @staticmethod
#     def create(task_id: str, scheduled_time: dt) -> RunItem:
#         run_idk = str(uuid4())
#         version = dt.utcnow()
#         status = RunStatus.QUEUED

#         item = RunItem(
#             run_idk = run_idk,
#             version = version,
#             task_idf = task_id,
#             scheduled_time = scheduled_time,
#             start_time = None,
#             end_time = None,
#             last_active = None,
#             status = status,
#             output = None
#         )

#         item._update_db()
#         return item

#     @staticmethod
#     def get_all(task_id: str, since: dt) -> list[RunItem]:
#         data = get_latest_versions(
#             session = Session,
#             table='orcha.runs',
#             key_columns=['run_idk'],
#             version_column='version',
#             select_columns='*',
#             match_pairs=[
#                 ('task_idf', '=', task_id),
#                 ('scheduled_time', '>=', since.isoformat())
#             ],
#         )
#         return [RunItem(**x) for x in data]

#     @staticmethod
#     def get_all_queued(task_id: str) -> list[RunItem]:
#         data = get_latest_versions(
#             session = Session,
#             table='orcha.runs',
#             key_columns=['run_idk'],
#             version_column='version',
#             select_columns='*',
#             match_pairs=[
#                 ('task_idf', '=', task_id),
#                 ('status', '=', RunStatus.QUEUED)
#             ],
#         )
#         return [RunItem(**x) for x in data]

#     @staticmethod
#     def get_latest(task_id: str) -> RunItem | None:
#         runs = RunItem.get_all(task_id, dt.min)
#         if len(runs) == 0:
#             return None
#         # order runs by scheduled_time
#         runs = sorted(runs, key=lambda x: x.scheduled_time, reverse=True)
#         return runs[0]

#     @staticmethod
#     def get_by_id(run_id: str) -> RunItem | None:
#         data = get_latest_versions(
#             session = Session,
#             table='orcha.runs',
#             key_columns=['run_idk'],
#             version_column='version',
#             select_columns='*',
#             match_pairs=[
#                 ('run_idk', '=', run_id)
#             ],
#         )
#         if len(data) == 0:
#             return None
#         return RunItem(**data[0])

#     def _update_db(self):
#         with Session.begin() as session:
#             session.merge(RunRecord(
#                 run_idk = self.run_idk,
#                 version = self.version,
#                 task_idf = self.task_idf,
#                 scheduled_time = self.scheduled_time,
#                 start_time = self.start_time,
#                 end_time = self.end_time,
#                 last_active = self.last_active,
#                 status = self.status,
#                 output = self.output
#             ))
#             session.commit()

#     def update_active(self):
#         self.last_active = dt.utcnow()
#         self._update_db()

#     def update(
#             self, status: str, start_time: dt | None ,
#             end_time: dt | None, output: dict | None = None
#         ):
#         self.status = status
#         self.start_time = start_time
#         self.end_time = end_time
#         self.output = output

#         db_data = RunItem.get_by_id(self.run_idk)

#         needs_update = False
#         if db_data is None:
#             needs_update = True
#         elif(
#             db_data.status != self.status or
#             db_data.start_time != self.start_time or
#             db_data.end_time != self.end_time or
#             db_data.output != self.output
#         ):
#             needs_update = True

#         if needs_update:
#             self.version = dt.utcnow()
#             self._update_db()

#     def set_running(self, output: dict | None = None):
#         db_item = RunItem.get_by_id(self.run_idk)
#         if db_item is not None:
#             if db_item.status == RunStatus.RUNNING:
#                 # if it's already set, we don't
#                 # want to update it again
#                 return
#         self.update(
#             status = RunStatus.RUNNING,
#             start_time = dt.utcnow(),
#             end_time = None,
#             output = output
#         )

#     def set_success(self, output: dict | None = None):
#         db_item = RunItem.get_by_id(self.run_idk)
#         if db_item is not None:
#             if db_item.status == RunStatus.SUCCESS:
#                 # if it's already set, we don't
#                 # want to update it again
#                 return
#         self.update(
#             status = RunStatus.SUCCESS,
#             start_time = self.start_time,
#             end_time = dt.utcnow(),
#             output = output
#         )

#     def set_failed(self, output: dict | None = None):
#         db_item = RunItem.get_by_id(self.run_idk)
#         if db_item is not None:
#             if db_item.status == RunStatus.FAILED:
#                 # if it's already set, we don't
#                 # want to update it again
#                 return
#         self.update(
#             status = RunStatus.FAILED,
#             start_time = self.start_time,
#             end_time = dt.utcnow(),
#             output = output
#         )


# postgres_build(Base, engine, CUR_SCHEMA)
