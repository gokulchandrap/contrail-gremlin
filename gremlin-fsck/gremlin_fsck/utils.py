import functools

from gremlin_python.process.graph_traversal import id, label, union

from contrail_api_cli.resource import Resource


def to_resource(fun):
    @functools.wraps(fun)
    def wrapper(*args):
        t = fun(*args)
        r = t.map(union(label(), id()).fold()).toList()
        # convert gremlin result in [Resource]
        r = [Resource(res_type.replace('_', '-'), uuid=uuid["@value"])
             for res_type, uuid in r]
        return r
    return wrapper
