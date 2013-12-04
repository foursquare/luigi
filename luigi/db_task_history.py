# Copyright (c) 2013 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import task_history
import configuration
import datetime
import logging

from contextlib import contextmanager
from task import id_to_name_and_params
from task_status import PENDING, FAILED, DONE, RUNNING, UNKNOWN

from sqlalchemy.orm.collections import attribute_mapped_collection
from sqlalchemy import Table, Column, Integer, String, ForeignKey, TIMESTAMP, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

Base = declarative_base()

logger = logging.getLogger('luigi.server')


class DbTaskHistory(task_history.TaskHistory):
    """ Task History that writes to a database using sqlalchemy. Also has methods for useful db queries
    """
    @contextmanager
    def _session(self, session=None):
        if session:
            yield session
        else:
            session = self.session_factory()
            try:
                yield session
            except:
                session.rollback()
                raise
            else:
                session.commit()

    def __init__(self):
        config = configuration.get_config()
        connection_string = config.get('task_history', 'db_connection')
        self.engine = create_engine(
            connection_string,
            pool_size=config.getint('task_history', 'db_pool_size', 20),
            max_overflow=config.getint('task_history', 'db_pool_max_overflow', 30),
            pool_timeout=config.getint('task_history', 'db_pool_timeout', 60),
            pool_recycle=config.getint('task_history', 'db_pool_recycle', 3600)
        )
        self.session_factory = sessionmaker(bind=self.engine, expire_on_commit=False)
        Base.metadata.create_all(self.engine)

    def task_scheduled(self, task_id, deps):
        for (task_record, session) in self._get_or_create_task_records(task_id):
            # get only the very first scheduling timestamp
            if task_record.scheduling_ts is None:
                task_record.scheduling_ts = datetime.datetime.now()
            # update status and add event
            task_record.status = PENDING
            task_record.events.append(TaskEvent(event_name=PENDING, ts=datetime.datetime.now()))
            # record deps if needed
            if deps and not task_record.deps:
                for (dep_record, s) in self._get_or_create_task_records(deps, session):
                    task_record.deps.append(dep_record)

    def task_finished(self, task_id, successful):
        status = DONE if successful else FAILED
        for (task_record, session) in self._get_or_create_task_records(task_id):
            # if task is done, register completion time
            if status == DONE and task_record.status != DONE:
                task_record.completion_ts = datetime.datetime.now()
            # update status and add event
            task_record.status = status
            task_record.events.append(TaskEvent(event_name=status, ts=datetime.datetime.now()))

    def task_started(self, task_id, worker_host):
        for (task_record, session) in self._get_or_create_task_records(task_id):
            # mark task as running
            if task_record.status != RUNNING:
                task_record.execution_ts = datetime.datetime.now()
            # update status, worker host and add event
            task_record.status = RUNNING
            task_record.host = worker_host
            task_record.events.append(TaskEvent(event_name=RUNNING, ts=datetime.datetime.now()))

    def other_event(self, task_id, event_name):
        for (task_record, session) in self._get_or_create_task_records(task_id):
            # update status and add event
            task_record.status = event_name
            task_record.events.append(TaskEvent(event_name=event_name, ts=datetime.datetime.now()))

    def _get_or_create_task_records(self, task_ids, session=None):
        with self._session(session) as session:
            # work with a set of task_id
            if isinstance(task_ids, basestring):
                task_ids = set([task_ids])
            else:
                task_ids = set(task_ids)
            logger.debug("Finding or creating task(s) with id %s" % task_ids)
            # try to find existing task having given task id(s)
            tasks = session.query(TaskRecord).filter(TaskRecord.luigi_id.in_(task_ids)).all()
            # yield all the record we have found
            for task_record in tasks:
                task_ids.remove(task_record.luigi_id)
                yield (task_record, session)
            # create new record for ids not found and yield them
            for task_id in task_ids:
                task_record = self._make_new_task_record(task_id, session)
                yield (task_record, session)

    def _make_new_task_record(self, task_id, session):
        task_family, parameters = id_to_name_and_params(task_id)
        task_record = TaskRecord(luigi_id=task_id,
                                 name=task_family,
                                 status=UNKNOWN,
                                 host=None)
        for (k, v) in parameters.iteritems():
            task_record.parameters[k] = TaskParameter(name=k, value=v)
        session.add(task_record)
        return task_record

    def find_all_by_parameters(self, task_name, session=None, **task_params):
        ''' Find tasks with the given task_name and the same parameters as the kwargs
        '''
        with self._session(session) as session:
            tasks = session.query(TaskRecord).join(TaskEvent).filter(TaskRecord.name == task_name).order_by(TaskEvent.ts).all()
            for task in tasks:
                if all(k in task.parameters and v == str(task.parameters[k].value) for (k, v) in task_params.iteritems()):
                    yield task

    def find_all_by_name(self, task_name, session=None):
        ''' Find all tasks with the given task_name
        '''
        return self.find_all_by_parameters(task_name, session)

    def find_latest_runs(self, session=None):
        ''' Return tasks that have been updated in the past 24 hours.
        '''
        with self._session(session) as session:
            yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
            return session.query(TaskRecord).\
                join(TaskEvent).\
                filter(TaskEvent.ts >= yesterday).\
                group_by(TaskRecord.id, TaskEvent.event_name).\
                order_by(TaskEvent.ts.desc()).\
                all()

    def find_task_by_id(self, id, session=None):
        ''' Find task with the given record ID
        '''
        with self._session(session) as session:
            return session.query(TaskRecord).get(id)


class TaskParameter(Base):
    """ Table to track luigi.Parameter()s of a Task
    """
    __tablename__ = 'task_parameters'
    task_id = Column(Integer, ForeignKey('tasks.id'), primary_key=True)
    name = Column(String(128), primary_key=True)
    value = Column(String(256))

    def __repr__(self):
        return "TaskParameter(task_id=%d, name=%s, value=%s)" % (self.task_id, self.name, self.value)


class TaskEvent(Base):
    """ Table to track when a task is scheduled, starts, finishes, and fails
    """
    __tablename__ = 'task_events'
    id = Column(Integer, primary_key=True)
    task_id = Column(Integer, ForeignKey('tasks.id'))
    event_name = Column(String(20))
    ts = Column(TIMESTAMP, index=True)

    def __repr__(self):
        return "TaskEvent(task_id=%s, event_name=%s, ts=%s" % (self.task_id, self.event_name, self.ts)


deps_table = Table('task_dependencies', Base.metadata,
    Column('task_id', Integer, ForeignKey('tasks.id'), primary_key=True),
    Column('dep_id', Integer, ForeignKey('tasks.id'), primary_key=True)
)


class TaskRecord(Base):
    """ Base table to track information about a luigi.Task. References to other tables are available through
    task.events, task.parameters, etc.
    """
    __tablename__ = 'tasks'
    id = Column(Integer, primary_key=True)
    name = Column(String(128), index=True)
    host = Column(String(128))
    luigi_id = Column(String(600), index=True, unique=True)
    status = Column(String(10))
    parameters = relationship('TaskParameter', collection_class=attribute_mapped_collection('name'),
                              cascade="all, delete-orphan")
    deps = relationship('TaskRecord',
                        secondary=deps_table,
                        primaryjoin=id==deps_table.c.task_id,
                        secondaryjoin=id==deps_table.c.dep_id,
                        passive_deletes=True)
    events = relationship("TaskEvent", order_by=lambda: TaskEvent.ts.desc(), backref="task", lazy="dynamic")
    scheduling_ts = Column(TIMESTAMP)
    execution_ts = Column(TIMESTAMP)
    completion_ts = Column(TIMESTAMP)

    def __repr__(self):
        return "TaskRecord(name=%s, host=%s)" % (self.name, self.host)
