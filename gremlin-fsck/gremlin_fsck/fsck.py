import os
import sys
import inspect
from six import text_type
import gevent

from contrail_api_cli.command import Command, Option
from contrail_api_cli.exceptions import CommandError
from contrail_api_cli.resource import Resource
from contrail_api_cli.utils import parallel_map, printo

from gremlin_python.structure.graph import Graph
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.graph_traversal import id, label, union

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
                   default=bool(os.environ.get('GREMLIN_FSCK_CLEAN', False)))
    loop = Option(help='Run in loop (default: %(default)s)',
                  action='store_true',
                  default=bool(os.environ.get('GREMLIN_FSCK_LOOP', False)))
    loop_interval = Option(help='Interval between loops in seconds (default: %(default)s)',
                           default=os.environ.get('GREMLIN_FSCK_LOOP_INTERVAL', 60 * 5),
                           type=float)

    def _check_by_name(self, name):
        c = None
        for n, check in avail_checks:
            if not name == n[6:]:
                continue
            else:
                c = check
        if c is None:
            raise CommandError("Can't find %s check" % name)
        return c

    def _clean_by_name(self, name):
        c = None
        for n, clean in avail_cleans:
            if not name == n[6:]:
                continue
            else:
                c = clean
        if c is None:
            raise CommandError("Can't find %s clean" % name)
        return c

    def __call__(self, gremlin_server=None, checks=None, clean=False, loop=False, loop_interval=None):
        graph = Graph()
        self.g = graph.traversal().withRemote(DriverRemoteConnection('ws://%s/gremlin' % gremlin_server, 'g'))
        if loop is True:
            self.run_loop(checks, clean, loop_interval)
        else:
            self.run(checks, clean)

    def run_loop(self, checks, clean, loop_interval):
        while True:
            self.run(checks, clean)
            gevent.sleep(loop_interval)

    def run(self, checks, clean):
        printo('Running checks...')
        for check_name in checks:
            check = self._check_by_name(check_name)
            t = check(self.g)
            r = t.map(union(label(), id()).fold()).toList()
            # convert gremlin result in [Resource]
            r = [Resource(res_type.replace('_', '-'), uuid=uuid["@value"])
                 for res_type, uuid in r]
            if len(r) > 0:
                printo('%s check found %d bad resources:' % (check_name, len(r)))
                printo('%s' % "\n".join([text_type(r_) for r_ in r]))
                if clean is False:
                    continue
                try:
                    clean = self._clean_by_name(check_name)
                    printo('Cleaning...')
                    parallel_map(clean, r, workers=20)
                    printo('Clean done.')
                except CommandError:
                    printo('Clean not found for this check. Skip.')
                    pass
        printo('Checks done.')
