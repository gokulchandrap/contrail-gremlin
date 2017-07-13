from __future__ import unicode_literals
import functools
import json
from cStringIO import StringIO
import sys
from six import text_type
import time
import logging

from gremlin_python.process.graph_traversal import id, label, union, values
from gremlin_python.process.traversal import lt

from contrail_api_cli.resource import Resource
from contrail_api_cli.exceptions import CommandError
from contrail_api_cli.utils import printo
from contrail_api_cli.manager import CommandManager


cmd_mgr = CommandManager(load_default=False)
cmd_mgr.load_namespace('contrail_api_cli.clean')
JSON_OUTPUT = False
ZK_SERVER = 'localhost:2181'


def log(string):
    if JSON_OUTPUT:
        return
    printo(string)


def to_resources(fun):
    @functools.wraps(fun)
    def wrapper(*args):
        now = int(time.time())
        t = fun(*args)
        # take only resources updated at least 5min ago
        t = t.has('updated', lt(now - 5 * 60))
        # we should be able to fold() fq_name: https://issues.apache.org/jira/browse/TINKERPOP-1711
        r = t.map(union(label(), id(), values('fq_name')).fold()).toList()
        # convert gremlin result in [Resource]
        resources = []
        for r_ in r:
            res_type = r_[0].replace('_', '-')
            uuid = r_[1]["@value"]
            fq_name = r_[2:]
            resources.append(Resource(res_type, uuid=uuid, fq_name=fq_name))
        return resources
    return wrapper


def log_resources(fun):
    @functools.wraps(fun)
    def wrapper(*args):
        r = fun(*args)
        if len(r) > 1:
            printo('Found %d %s:' % (len(r), fun.__doc__.strip()))
            for r_ in r:
                printo('  - %s/%s - %s' % (r_.type, r_.uuid, r_.fq_name))
        return r
    return wrapper


def log_json(fun):
    def json_log(fun, total, output, duration):
        return json.dumps({
            "application": 'gremlin-fsck',
            "type": fun.__name__.split('_')[0],
            "name": fun.__name__,
            "total": total,
            "output": output,
            "success": total >= 0,
            "duration": "%0.2f ms" % duration
        })

    @functools.wraps(fun)
    def wrapper(*args):
        if JSON_OUTPUT:
            old_stdout = sys.stdout
            sys.stdout = my_stdout = StringIO()
        start = time.time()
        try:
            r = fun(*args)
        except CommandError as e:
            r = -1
            printo(text_type(e))
        end = time.time()
        if JSON_OUTPUT:
            sys.stdout = old_stdout
            if r == -1:
                total = -1
            elif isinstance(r, list):
                total = len(r)
            else:
                total = 1
            printo(json_log(fun, total, my_stdout.getvalue(), (end - start) * 1000.0))
            my_stdout.close()
        return r
    return wrapper


def count_lines(fun):
    @functools.wraps(fun)
    def wrapper(*args):
        old_stdout = sys.stdout
        sys.stdout = my_stdout = StringIO()
        root = logging.getLogger()
        ch = logging.StreamHandler(my_stdout)
        ch.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        root.addHandler(ch)

        def cleanup():
            sys.stdout = old_stdout
            root.removeHandler(ch)
            output = my_stdout.getvalue()
            my_stdout.close()
            return output

        try:
            fun(*args)
            output = cleanup()
            # return a list for log_json count
            return range(1, output.count('\n'))
        except CommandError as e:
            raise CommandError("%s:\n%s" % (text_type(e), cleanup()))

    return wrapper


def v_to_r(v):
    if v.label:
        return Resource(v.label.replace('_', '-'), uuid=v.id["@value"])
    raise CommandError('Vertex has no label, cannot transform it to Resource')


def cmd(name):
    return cmd_mgr.get(name)
