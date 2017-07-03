from gremlin_python.process.graph_traversal import __, union, select
from gremlin_python.process.traversal import within, eq
from gremlin_python import statics

from contrail_api_cli.utils import printo
from contrail_api_cli.manager import CommandManager

from .utils import to_resources, log_resources, log_json, v_to_r


CommandManager().load_namespace('contrail_api_cli.clean')
rm = CommandManager().get('rm')
clean_stale_si = CommandManager().get('clean-stale-si')
fix_sg = CommandManager().get('fix-sg')

statics.default_lambda_language = 'gremlin-groovy'
statics.load_statics(globals())


@log_json
@log_resources
@to_resources
def check_vn_with_iip_without_vmi(g):
    """instance-ip without any virtual-machine-interface
    """
    return g.V().hasLabel("virtual_network").not_(
        __.in_().hasLabel('virtual_machine_interface')
    ).in_().hasLabel("instance_ip")


def clean_vn_with_iip_without_vmi(iip):
    iip.delete()
    printo('Deleted %s' % iip)


@log_json
@log_resources
@to_resources
def check_unused_rt(g):
    """unused route-target
    """
    return g.V().hasLabel("route_target").not_(
        __.in_().hasLabel(within("routing_instance", "logical_router"))
    )


def clean_unused_rt(rt):
    rt.delete()
    printo('Deleted %s' % rt)


@log_json
@log_resources
@to_resources
def check_iip_without_instance_ip_address(g):
    """instance-ip without any instance_ip_address property
    """
    return g.V().hasLabel("instance_ip").not_(
        __.has("instance_ip_address")
    )


@log_json
@log_resources
@to_resources
def check_snat_without_lr(g):
    """Snat SI without any logical-router
    """
    return g.V().hasLabel("service_template").has("name", "netns-snat-template") \
        .in_().hasLabel("service_instance").not_(__.in_().hasLabel("logical_router"))


@log_json
def clean_snat_without_lr(si):
    clean_stale_si([si.path])


@log_json
@log_resources
@to_resources
def check_lbaas_without_lbpool(g):
    """LBaaS SI without any loadbalancer-pool
    """
    return g.V().hasLabel("service_template") \
        .has("name", "haproxy-loadbalancer-template") \
        .in_().hasLabel("service_instance") \
        .not_(__.in_().hasLabel("loadbalancer_pool"))


@log_json
def clean_lbaas_without_lbpool(si):
    clean_stale_si([si.path])


@log_json
@log_resources
@to_resources
def check_lbaas_without_vip(g):
    """LBaaS SI without any virtual-ip
    """
    return g.V().hasLabel("service_instance") \
        .where(__.in_().hasLabel("loadbalancer_pool").not_(__.in_().hasLabel("virtual_ip")))


@log_json
def clean_lbaas_without_vip(si):
    clean_stale_si([si.path])


@log_json
@log_resources
@to_resources
def check_ri_without_rt(g):
    """routing-instance that doesn't have any route-target (that crashes schema)
    """
    return g.V().hasLabel("routing_instance") \
        .not_(__.has('fq_name', within("__default__", "__link_local__"))) \
        .not_(__.out().hasLabel("route_target"))


@log_json
@log_resources
@to_resources
def check_acl_without_sg(g):
    """access-control-list without security-group
    """
    return g.V().hasLabel('access_control_list').where(
        __.inE().count().is_(eq(0))
    )


@log_json
def clean_acl_without_sg(acl):
    acl.delete()
    printo('Deleted %s' % acl)


@log_json
def check_duplicate_ip_addresses(g):
    """networks with duplicate ip addresses
    """
    r = g.V().hasLabel("virtual_network").as_('vn').flatMap(
        union(
            select('vn'),
            __.in_().hasLabel("instance_ip").has("instance_ip_address")
            .group().by("instance_ip_address").unfold()
            .filter(lambda: "it.get().value.size() > 1")
        ).fold().filter(lambda: "it.get().size() > 1")
    ).toList()
    if len(r) > 0:
        printo('Found %d %s:' % (len(r), check_duplicate_ip_addresses.__doc__.strip()))
    for dup in r:
        # First item is the vn
        printo("  - %s" % v_to_r(dup[0]))
        for ips in dup[1:]:
            for ip, iips in ips.items():
                printo("      %s:" % ip)
                for iip in iips:
                    printo("        - %s" % v_to_r(iip))
    return r


@log_json
def check_duplicate_default_sg(g):
    """duplicate default security groups
    """
    r = g.V().hasLabel('project').flatMap(
        __.out().hasLabel('security_group').has('display_name', 'default').group().by(
            __.in_().hasLabel('project').id()
        ).unfold()
        .filter(lambda: "it.get().value.size() > 1")
    ).toList()
    if len(r) > 0:
        printo('Found %d %s:' % (len(r), check_duplicate_default_sg.__doc__.strip()))
    projects = []
    for dup in r:
        for p, sgs in dup.items():
            projects.append(v_to_r(p))
            printo("  %s:" % projects[-1])
            for sg in sgs:
                printo("    - %s" % sg)
    return projects


@log_json
def clean_duplicate_default_sg(p):
    fix_sg(paths=[p.path], yes=True)


@log_json
def check_duplicate_public_ips(g):
    """duplicate public ips
    """
    r = g.V().hasLabel(within('floating_ip', 'instance_ip')) \
        .property('ip_address', __.values('floating_ip_address', 'instance_ip_address')).group().by('ip_address').unfold() \
        .filter(lambda: "it.get().value.size() > 1 && it.get().value.findAll{it.label.value == 'floating_ip'} != []").toList()
    if len(r) > 0:
        printo('Found %d %s:' % (len(r), check_duplicate_public_ips.__doc__.strip()))
    return r
