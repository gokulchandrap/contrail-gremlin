from __future__ import unicode_literals
import functools

from gremlin_python.process.graph_traversal import id, label, union, values

from contrail_api_cli.resource import Resource
from contrail_api_cli.exceptions import CommandError
from contrail_api_cli.utils import printo


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


def v_to_r(v):
    if v.label:
        return Resource(v.label.replace('_', '-'), uuid=v.id["@value"])
    raise CommandError('Vertex has no label, cannot transform it to Resource')
