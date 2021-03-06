# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Provides a database backend to the central scheduler. This lets you see historical runs.
See :ref:`TaskHistory` for information about how to turn out the task history feature.
"""
#
# Description: Added codes for visualization of how long each task takes
# running-time until it reaches the next status (failed or done)
# At "{base_url}/tasklist", all completed(failed or done) tasks are shown.
# At "{base_url}/tasklist", a user can select one specific task to see
# how its running-time has changed over time.
# At "{base_url}/tasklist/{task_name}", it visualizes a multi-bar graph
# that represents the changes of the running-time for a selected task
# up to the next status (failed or done).
# This visualization let us know how the running-time of the specific task
# has changed over time.
#
# Copyright 2015 Naver Corp.
# Author Yeseul Park (yeseul.park@navercorp.com)
#

import datetime
import logging
from contextlib import contextmanager
from copy import copy

from luigi import six

from luigi import configuration
from luigi.task_status import PENDING, FAILED, DONE, RUNNING, UNKNOWN

import sqlalchemy
import sqlalchemy.ext.declarative
import sqlalchemy.orm
import sqlalchemy.orm.collections
from sqlalchemy.engine import reflection
Base = sqlalchemy.ext.declarative.declarative_base()

logger = logging.getLogger('luigi.server')


class DbTaskHistory(object):
    """
    Task History that writes to a database using sqlalchemy.
    Also has methods for useful db queries.
    """

    @contextmanager
    def _session(self, session=None):
        if session:
            yield session
        else:
            session = self.session_factory()
            try:
                yield session
            except BaseException:
                session.rollback()
                raise
            else:
                session.commit()

    def __init__(self):
        config = configuration.get_config()
        connection_string = config.get('task_history', 'db_connection')
        if 'mysql' in connection_string:
            self.engine = sqlalchemy.create_engine(
                connection_string,
                pool_size=config.getint('task_history', 'db_pool_size', 100),
                max_overflow=config.getint('task_history', 'db_pool_max_overflow', 200),
                pool_timeout=config.getint('task_history', 'db_pool_timeout', 60),
                pool_recycle=config.getint('task_history', 'db_pool_recycle', 3600)
            )
        else:
            self.engine = sqlalchemy.create_engine(connection_string)
        self.session_factory = sqlalchemy.orm.sessionmaker(bind=self.engine, expire_on_commit=False)
        Base.metadata.create_all(self.engine)

    def task_scheduled(self, task, ts=None):
        if ts is None:
            ts = datetime.datetime.now()
        for (task_record, session) in self._get_or_create_task_record(task):
            # set only the very first scheduling timestamp
            if task_record.scheduling_ts is None:
                task_record.scheduling_ts = ts
            # update status and add event
            task_record.status = PENDING
            task_record.events.append(TaskEvent(event_name=PENDING, ts=ts))
            # record deps if needed
            if task.deps and not task_record.deps:
                for (dep_record, s) in self._get_or_create_deps_records(task.deps, session):
                    task_record.deps.append(dep_record)

    def task_finished(self, task, successful, ts=None):
        if ts is None:
            ts = datetime.datetime.now()
        status = DONE if successful else FAILED
        for (task_record, session) in self._get_or_create_task_record(task):
            # if task is done, register completion time
            if status == DONE and task_record.status != DONE:
                task_record.completion_ts = ts
            # update status and add event
            task_record.status = status
            task_record.events.append(TaskEvent(event_name=status, ts=ts))

    def task_started(self, task, worker_host, ts=None):
        if ts is None:
            ts = datetime.datetime.now()
        for (task_record, session) in self._get_or_create_task_record(task):
            # mark task as running
            if task_record.status != RUNNING:
                task_record.execution_ts = ts
            # update status, worker host and add event
            task_record.status = RUNNING
            task_record.host = worker_host
            task_record.events.append(TaskEvent(event_name=RUNNING, ts=ts))

    def other_event(self, task, event_name, ts=None):
        if ts is None:
            ts = datetime.datetime.now()
        for (task_record, session) in self._get_or_create_task_record(task):
            # update status and add event
            task_record.status = event_name
            task_record.events.append(TaskEvent(event_name=event_name, ts=ts))

    def _get_or_create_task_record(self, task, session=None):
        with self._session(session) as session:
            logger.debug("Finding or creating task with id %s" % task.id)
            # try to find existing task having given task id
            task_record = session.query(TaskRecord).filter(TaskRecord.luigi_id == task.id).first()
            if not task_record:
                task_record = self._make_new_task_record(task.id, session)
            # make sure we store all info about the task
            task_record.name = task.family
            for (k, v) in task.params.iteritems():
                if k not in task_record.parameters:
                    task_record.parameters[k] = TaskParameter(name=k, value=v)
            # yield the record
            if hasattr(task, 'tracking_url') and task.tracking_url:
                task_record.tracking_url = task.tracking_url
            yield (task_record, session)

    def _get_or_create_deps_records(self, dep_ids, session=None):
        task_ids = copy(dep_ids)
        with self._session(session) as session:
            logger.debug("Finding or creating deps with id %s" % task_ids)
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
        task_record = TaskRecord(luigi_id=task_id, name=None, status=UNKNOWN, host=None)
        session.add(task_record)
        return task_record

    # following methods are used by web server

    def find_all_by_parameters(self, task_name, session=None, **task_params):
        """
        Find tasks with the given task_name and the same parameters as the kwargs.
        """
        with self._session(session) as session:
            query = session.query(TaskRecord).join(TaskEvent).filter(TaskRecord.name == task_name)
            for (k, v) in six.iteritems(task_params):
                alias = sqlalchemy.orm.aliased(TaskParameter)
                query = query.join(alias).filter(alias.name == k, alias.value == v)

            tasks = query.order_by(TaskEvent.ts)
            for task in tasks:
                # Sanity check
                assert all(k in task.parameters and v == str(task.parameters[k].value) for (k, v) in six.iteritems(task_params))

                yield task

    def find_all_by_name(self, task_name, session=None):
        """
        Find all tasks with the given task_name.
        """
        return self.find_all_by_parameters(task_name, session)

    def find_latest_runs(self, session=None):
        """
        Return tasks that have been updated in the past 24 hours.
        """
        with self._session(session) as session:
            yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
            return session.query(TaskRecord).\
                join(TaskEvent).\
                filter(TaskEvent.ts >= yesterday).\
                group_by(TaskRecord.id, TaskEvent.event_name, TaskEvent.ts).\
                order_by(TaskEvent.ts.desc()).\
                all()

    def find_task_by_id(self, id, session=None):
        """
        Find task with the given record ID.
        """
        with self._session(session) as session:
            return session.query(TaskRecord).get(id)


class TaskParameter(Base):
    """
    Table to track luigi.Parameter()s of a Task.
    """
    __tablename__ = 'task_parameters'
    task_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('tasks.id'), primary_key=True)
    name = sqlalchemy.Column(sqlalchemy.String(128), primary_key=True)
    value = sqlalchemy.Column(sqlalchemy.Text())

    def __repr__(self):
        return "TaskParameter(task_id=%d, name=%s, value=%s)" % (self.task_id, self.name, self.value)


class TaskEvent(Base):
    """
    Table to track when a task is scheduled, starts, finishes, and fails.
    """
    __tablename__ = 'task_events'
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    task_id = sqlalchemy.Column(sqlalchemy.Integer, sqlalchemy.ForeignKey('tasks.id'), index=True)
    event_name = sqlalchemy.Column(sqlalchemy.String(20))
    # todo(stefano): add host over here as well, it's super useful for debugging
    ts = sqlalchemy.Column(sqlalchemy.TIMESTAMP, index=True, nullable=False)

    def __repr__(self):
        return "TaskEvent(task_id=%s, event_name=%s, ts=%s" % (self.task_id, self.event_name, self.ts)


deps_table = sqlalchemy.Table('task_dependencies', Base.metadata,
    sqlalchemy.Column('task_id', sqlalchemy.Integer, sqlalchemy.ForeignKey('tasks.id'), primary_key=True),
    sqlalchemy.Column('dep_id', sqlalchemy.Integer, sqlalchemy.ForeignKey('tasks.id'), primary_key=True)
)


class TaskRecord(Base):
    """
    Base table to track information about a luigi.Task.

    References to other tables are available through task.events, task.parameters, etc.
    """
    __tablename__ = 'tasks'
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    # note(stefano): this is task_id on master, we should migrate our db to comply to that
    luigi_id = sqlalchemy.Column(sqlalchemy.String(600), index=True, unique=True)
    name = sqlalchemy.Column(sqlalchemy.String(128), index=True)
    host = sqlalchemy.Column(sqlalchemy.String(128))
    status = sqlalchemy.Column(sqlalchemy.String(10))
    parameters = sqlalchemy.orm.relationship(
        'TaskParameter',
        collection_class=sqlalchemy.orm.collections.attribute_mapped_collection('name'),
        cascade="all, delete-orphan")
    events = sqlalchemy.orm.relationship(
        'TaskEvent',
        order_by=(sqlalchemy.desc(TaskEvent.ts), sqlalchemy.desc(TaskEvent.id)),
        backref='task',
        lazy="dynamic")
    deps = sqlalchemy.orm.relationship(
        'TaskRecord',
        secondary=deps_table,
        primaryjoin=id==deps_table.c.task_id,
        secondaryjoin=id==deps_table.c.dep_id,
        passive_deletes=True)
    scheduling_ts = sqlalchemy.Column(sqlalchemy.TIMESTAMP)
    execution_ts = sqlalchemy.Column(sqlalchemy.TIMESTAMP)
    completion_ts = sqlalchemy.Column(sqlalchemy.TIMESTAMP)
    tracking_url = sqlalchemy.Column(sqlalchemy.String(255))

    def __repr__(self):
        return "TaskRecord(name=%s, host=%s)" % (self.name, self.host)


def _upgrade_schema(engine):
    """
    Ensure the database schema is up to date with the codebase.

    :param engine: SQLAlchemy engine of the underlying database.
    """
    inspector = reflection.Inspector.from_engine(engine)
    with engine.connect() as conn:

        # Upgrade 1.  Add task_id column and index to tasks
        if 'task_id' not in [x['name'] for x in inspector.get_columns('tasks')]:
            logger.warning('Upgrading DbTaskHistory schema: Adding tasks.task_id')
            conn.execute('ALTER TABLE tasks ADD COLUMN task_id VARCHAR(200)')
            conn.execute('CREATE INDEX ix_task_id ON tasks (task_id)')
            conn.execute('CREATE INDEX ix_tasks_name_scheduling_ts_status ON tasks(name, scheduling_ts, status)')
            conn.execute('CREATE INDEX ix_tasks_name_completion_ts_status ON tasks(name, completion_ts, status)')
            conn.execute('CREATE INDEX ix_tasks_name_execution_ts_status ON tasks(name, execution_ts, status)')
        # Upgrade 2. Alter value column to be TEXT, note that this is idempotent so no if-guard
        if 'mysql' in engine.dialect.name:
            conn.execute('ALTER TABLE task_parameters MODIFY COLUMN value TEXT')
        elif 'oracle' in engine.dialect.name:
            conn.execute('ALTER TABLE task_parameters MODIFY value TEXT')
        elif 'mssql' in engine.dialect.name:
            conn.execute('ALTER TABLE task_parameters ALTER COLUMN value TEXT')
        elif 'postgresql' in engine.dialect.name:
            conn.execute('ALTER TABLE task_parameters ALTER COLUMN value TYPE TEXT')
        elif 'sqlite' in engine.dialect.name:
            # SQLite does not support changing column types. A database file will need
            # to be used to pickup this migration change.
            for i in conn.execute('PRAGMA table_info(task_parameters);').fetchall():
                if i['name'] == 'value' and i['type'] != 'TEXT':
                    logger.warning(
                        'SQLite can not change column types. Please use a new database '
                        'to pickup column type changes.'
                    )
        else:
            logger.warning(
                'SQLAlcheny dialect {} could not be migrated to the TEXT type'.format(
                    engine.dialect
                )
            )

        # Upgrade 3.  Add tracking url column
        if 'tracking_url' not in [x['name'] for x in inspector.get_columns('tasks')]:
            logger.warning('Upgrading DbTaskHistory schema: Adding tasks.tracking_url')
            conn.execute('ALTER TABLE tasks ADD COLUMN tracking_url VARCHAR(255)')
