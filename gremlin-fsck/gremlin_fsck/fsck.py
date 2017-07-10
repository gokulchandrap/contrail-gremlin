from __future__ import unicode_literals
import os
import sys
import inspect
from time import time
import gevent
import socket

from tornado.httpclient import HTTPError

from contrail_api_cli.command import Command, Option
from contrail_api_cli.exceptions import CommandError

from gremlin_python.structure.graph import Graph
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection

from . import utils
from .checks import *


avail_checks = [(name, obj) for name, obj in inspect.getmembers(sys.modules[__name__])
                if inspect.isfunction(obj) and name.startswith('check_')]
avail_cleans = [(name, obj) for name, obj in inspect.getmembers(sys.modules[__name__])
                if inspect.isfunction(obj) and name.startswith('clean_')]


class Fsck(Command):
    description = 'Checks and optionally clean API inconsistencies'
    gremlin_server = Option(default=os.environ.get('GREMLIN_FSCK_SERVER', 'localhost:8182'),
                            help='host:port of gremlin serveri (default: %(default)s)')
    checks = Option(help='Name of checks to run',
                    nargs='*', choices=[n[6:] for n, o in avail_checks],
                    default=[n[6:] for n, o in avail_checks],
                    metavar='check')
    clean = Option(help='Run cleans (default: %(default)s)',
                   action='store_true',
                   default=bool(int(os.environ.get('GREMLIN_FSCK_CLEAN', 0))))
    loop = Option(help='Run in loop (default: %(default)s)',
                  action='store_true',
                  default=bool(int(os.environ.get('GREMLIN_FSCK_LOOP', 0))))
    loop_interval = Option(help='Interval between loops in seconds (default: %(default)s)',
                           default=os.environ.get('GREMLIN_FSCK_LOOP_INTERVAL', 60 * 5),
                           type=float)
    json = Option(help='Output logs in json',
                  action='store_true',
                  default=bool(int(os.environ.get('GREMLIN_FSCK_JSON', 0))))

    def _check_by_name(self, name):
        c = None
        for n, check in avail_checks:
            if not name == n[6:]:
                continue
            else:
                c = check
        if c is None:
            raise CommandError("Can't find %s check method" % name)
        return c

    def _clean_by_name(self, name):
        c = None
        for n, clean in avail_cleans:
            if not name == n[6:]:
                continue
            else:
                c = clean
        if c is None:
            raise CommandError("Can't find %s clean method" % name)
        return c

    def __call__(self, gremlin_server=None, checks=None, clean=False, loop=False, loop_interval=None, json=False):
        utils.JSON_OUTPUT = json
        graph = Graph()
        try:
            self.g = graph.traversal().withRemote(DriverRemoteConnection('ws://%s/gremlin' % gremlin_server, 'g'))
        except (HTTPError, socket.error) as e:
            raise CommandError('Failed to connect to Gremlin server: %s' % e)
        if loop is True:
            self.run_loop(checks, clean, loop_interval)
        else:
            self.run(checks, clean)

    def run_loop(self, checks, clean, loop_interval):
        while True:
            self.run(checks, clean)
            gevent.sleep(loop_interval)

    def run(self, checks, clean):
        utils.log('Running checks...')
        start = time()
        for check_name in checks:
            check = self._check_by_name(check_name)
            r = check(self.g)
            if len(r) > 0:
                if clean is False:
                    continue
                try:
                    clean = self._clean_by_name(check_name)
                    utils.log('Cleaning...')
                    for r_ in r:
                        clean(r_)
                    utils.log('Clean done.')
                except CommandError as e:
                    utils.log(e)
                    pass
        end = time() - start
        utils.log('Checks done in %ss' % end)
