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
Simple REST server that takes commands in a JSON payload
Interface to the :py:class:`~luigi.scheduler.CentralPlannerScheduler` class.
See :doc:`/central_scheduler` for more info.
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

import atexit
import json
import logging
import mimetypes
import os
import posixpath
import signal
import sys
import datetime
import time

import pkg_resources
from threading import Thread
import tornado.gen
import tornado.httpclient
import tornado.httpserver
import tornado.ioloop
import tornado.netutil
import tornado.web
from tornado.concurrent import Future

from luigi.scheduler import CentralPlannerScheduler


logger = logging.getLogger("luigi.server")


def call_in_thread(fun, *args, **kwargs):
    future = Future()
    def wrapper():
        try:
            future.set_result(fun(*args, **kwargs))
        except Exception as e:
            future.set_exception(e)
    Thread(target=wrapper).start()
    return future


class RPCHandler(tornado.web.RequestHandler):
    """
    Handle remote scheduling calls using rpc.RemoteSchedulerResponder.
    """

    def initialize(self, scheduler):
        self._scheduler = scheduler

    def get(self, method):
        if method not in [
            'add_task',
            'add_worker',
            'dep_graph',
            'disable_worker',
            'fetch_error',
            'get_work',
            'graph',
            'inverse_dep_graph',
            'ping',
            'prune',
            're_enable_task',
            'resource_list',
            'task_list',
            'task_search',
            'update_resources',
            'worker_list',
            'set_task_status_message',
            'get_task_status_message',
        ]:
            self.send_error(404)
            return
        payload = self.get_argument('data', default="{}")
        arguments = json.loads(payload)

        # TODO: we should probably denote all methods on the scheduler that are "API-level"
        # versus internal methods. Right now you can do a REST method call to any method
        # defined on the scheduler, which is pretty bad from a security point of view.

        if hasattr(self._scheduler, method):
            result = getattr(self._scheduler, method)(**arguments)
            self.write({"response": result})  # wrap all json response in a dictionary
        else:
            self.send_error(404)

    post = get


class BaseTaskHistoryHandler(tornado.web.RequestHandler):
    def initialize(self, scheduler):
        self._scheduler = scheduler

    def get_template_path(self):
        return pkg_resources.resource_filename(__name__, 'templates')


def from_utc(utcTime, fmt=None):
    """convert UTC time string to time.struct_time: change datetime.datetime to time, return time.struct_time type"""
    if fmt is None:
        try_formats = ["%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"]
    else:
        try_formats = [fmt]

    for fmt in try_formats:
        try:
            time_struct = datetime.datetime.strptime(utcTime, fmt)
        except ValueError:
            pass
        else:
            date = int(time.mktime(time_struct.timetuple()))
            return date
    else:
        raise ValueError("No UTC format matches {}".format(utcTime))


class RecentRunHandler(BaseTaskHistoryHandler):
    @tornado.gen.coroutine
    def get(self):
        tasks = yield call_in_thread(self._scheduler.task_history.find_latest_runs)
        self.render("recent.html", tasks=tasks)


class ByNameHandler(BaseTaskHistoryHandler):
    @tornado.gen.coroutine
    def get(self, name):
        tasks = yield call_in_thread(self._scheduler.task_history.find_all_by_name, name)
        self.render("recent.html", tasks=tasks)


class ByIdHandler(BaseTaskHistoryHandler):
    @tornado.gen.coroutine
    def get(self, id):
        task = yield call_in_thread(self._scheduler.task_history.find_task_by_id, id)
        self.render("show.html", task=task)


class ByParamsHandler(BaseTaskHistoryHandler):
    @tornado.gen.coroutine
    def get(self, name):
        payload = self.get_argument('data', default="{}")
        arguments = json.loads(payload)
        tasks = yield call_in_thread(self._scheduler.task_history.find_all_by_parameters, name, session=None, **arguments)
        self.render("recent.html", tasks=tasks)


class StaticFileHandler(tornado.web.RequestHandler):
    def get(self, path):
        # Path checking taken from Flask's safe_join function:
        # https://github.com/mitsuhiko/flask/blob/1d55b8983/flask/helpers.py#L563-L587
        path = posixpath.normpath(path)
        if os.path.isabs(path) or path.startswith(".."):
            return self.send_error(404)

        extension = os.path.splitext(path)[1]
        if extension in mimetypes.types_map:
            self.set_header("Content-Type", mimetypes.types_map[extension])
        data = pkg_resources.resource_string(__name__, os.path.join("static", path))
        self.write(data)


class RootPathHandler(BaseTaskHistoryHandler):
    def get(self):
        self.redirect("/static/visualiser/index.html")


def app(scheduler):
    settings = {"static_path": os.path.join(os.path.dirname(__file__), "static"),
                "unescape": tornado.escape.xhtml_unescape}
    handlers = [
        (r'/api/(.*)', RPCHandler, {"scheduler": scheduler}),
        (r'/static/(.*)', StaticFileHandler),
        (r'/', RootPathHandler, {'scheduler': scheduler}),
        (r'/history', RecentRunHandler, {'scheduler': scheduler}),
        (r'/history/by_name/(.*?)', ByNameHandler, {'scheduler': scheduler}),
        (r'/history/by_id/(.*?)', ByIdHandler, {'scheduler': scheduler}),
        (r'/history/by_params/(.*?)', ByParamsHandler, {'scheduler': scheduler})
    ]
    api_app = tornado.web.Application(handlers, **settings)
    return api_app


def _init_api(scheduler, responder=None, api_port=None, address=None, unix_socket=None):
    if responder:
        raise Exception('The "responder" argument is no longer supported')
    api_app = app(scheduler)
    if unix_socket is not None:
        api_sockets = [tornado.netutil.bind_unix_socket(unix_socket)]
    else:
        api_sockets = tornado.netutil.bind_sockets(api_port, address=address)
    server = tornado.httpserver.HTTPServer(api_app)
    server.add_sockets(api_sockets)

    # Return the bound socket names.  Useful for connecting client in test scenarios.
    return [s.getsockname() for s in api_sockets]


def run(api_port=8082, address=None, unix_socket=None, scheduler=None, responder=None):
    """
    Runs one instance of the API server.
    """
    if scheduler is None:
        scheduler = CentralPlannerScheduler()

    # load scheduler state
    scheduler.load()

    _init_api(
        scheduler=scheduler,
        responder=responder,
        api_port=api_port,
        address=address,
        unix_socket=unix_socket,
    )

    # prune work DAG every 60 seconds
    pruner = tornado.ioloop.PeriodicCallback(scheduler.prune, 60000)
    pruner.start()

    def shutdown_handler(signum, frame):
        exit_handler()
        sys.exit(0)

    @atexit.register
    def exit_handler():
        logger.info("Scheduler instance shutting down")
        scheduler.dump()
        stop()

    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)
    if os.name == 'nt':
        signal.signal(signal.SIGBREAK, shutdown_handler)
    else:
        signal.signal(signal.SIGQUIT, shutdown_handler)

    logger.info("Scheduler starting up")

    tornado.ioloop.IOLoop.instance().start()


def stop():
    tornado.ioloop.IOLoop.instance().stop()


if __name__ == "__main__":
    run()
