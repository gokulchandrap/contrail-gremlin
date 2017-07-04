from __future__ import unicode_literals
import functools
import json
from cStringIO import StringIO
import sys
from six import text_type
import time

from gremlin_python.process.graph_traversal import id, label, union, values

from contrail_api_cli.resource import Resource
from contrail_api_cli.exceptions import CommandError
from contrail_api_cli.utils import printo
from contrail_api_cli.manager import CommandManager


CommandManager(load_default=False).load_namespace('contrail_api_cli.clean')
JSON_OUTPUT = False


def log(string):
    if JSON_OUTPUT:
        return
    printo(string)


def to_resources(fun):
    @functools.wraps(fun)
    def wrapper(*args):
        t = fun(*args)
        r = t.map(union(label(), id(), values('fq_name')).fold()).toList()
        # convert gremlin result in [Resource]
        r = [Resource(res_type.replace('_', '-'), uuid=uuid["@value"], fq_name=fq_name)
             for res_type, uuid, fq_name in r]
        return r
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
            total = 1
            if isinstance(r, list):
                total = len(r)
            printo(json_log(fun, total, my_stdout.getvalue(), (end - start) * 1000.0))
            my_stdout.close()
        return r
    return wrapper


def v_to_r(v):
    if v.label:
        return Resource(v.label.replace('_', '-'), uuid=v.id["@value"])
    raise CommandError('Vertex has no label, cannot transform it to Resource')


def cmd(name):
    return CommandManager(load_default=False).get(name)
