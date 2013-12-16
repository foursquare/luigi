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

import json
import tornado.gen
import tornado.web
import task_history
import logging
logger = logging.getLogger("luigi.server")

from threading import Thread
from tornado.concurrent import Future

def call_in_thread(fun, *args, **kwargs):
    future = Future()
    def wrapper():
        try:
            future.set_result(fun(*args, **kwargs))
        except Exception as e:
            future.set_exception(e)
    Thread(target=wrapper).start()
    return future

class BaseTaskHistoryHandler(tornado.web.RequestHandler):
    def initialize(self, api):
        self._api = api

    def get_template_path(self):
        return 'luigi/templates'


class RecentRunHandler(BaseTaskHistoryHandler):
    @tornado.gen.coroutine
    def get(self):
        tasks = yield call_in_thread(self._api.task_history.find_latest_runs)
        self.render("recent.html", tasks=tasks)


class ByNameHandler(BaseTaskHistoryHandler):
    @tornado.gen.coroutine
    def get(self, name):
        tasks = yield call_in_thread(self._api.task_history.find_all_by_name, name)
        self.render("recent.html", tasks=tasks)


class ByIdHandler(BaseTaskHistoryHandler):
    @tornado.gen.coroutine
    def get(self, id):
        task = yield call_in_thread(self._api.task_history.find_task_by_id, id)
        self.render("show.html", task=task)


class ByParamsHandler(BaseTaskHistoryHandler):
    @tornado.gen.coroutine
    def get(self, name):
        payload = self.get_argument('data', default="{}")
        arguments = json.loads(payload)
        tasks = yield call_in_thread(self._api.task_history.find_all_by_parameters, name, session=None, **arguments)
        self.render("recent.html", tasks=tasks)


def get_handlers(api):
    handlers = [
        (r'/history', RecentRunHandler, {'api': api}),
        (r'/history/by_name/(.*?)', ByNameHandler, {'api': api}),
        (r'/history/by_id/(.*?)', ByIdHandler, {'api': api}),
        (r'/history/by_params/(.*?)', ByParamsHandler, {'api': api})
    ]
    return handlers